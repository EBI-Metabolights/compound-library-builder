#!/usr/bin/env python3
import argparse, json, os, re, sys
import base64
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import requests
from rdkit import Chem
from rdkit.Chem import AllChem

# ---------- JSON Schemas (trimmed but strict enough) ----------
COMPOUND_SCHEMA = {
  "type":"object",
  "required":["id","name","formula","inchiKey"],
  "properties":{
    "id":{"type":"string"},
    "name":{"type":"string"},
    "formula":{"type":"string"},
    "inchiKey":{"type":"string"},
    "exactmass":{"type":["number","string"]},
    "averagemass":{"type":["number","string"]},
    "charge":{"type":["integer","string"]},
    "pathways":{"type":"object"},
    "reactions":{"type":"array"},
    "citations":{"type":"array"},
    "structure":{"type":"string"},
    "spectra":{"type":"object"}
  }
}
SPECTRUM_SCHEMA = {
  "type":"object",
  "required":["spectrumId","peaks"],
  "properties":{
    "spectrumId":{"type":"string"},
    "modality":{"enum":["MS","NMR"]},
    "peaks":{"type":"array","items":{
      "type":"object","required":["mz","intensity"],
      "properties":{"mz":{"type":"number"},"intensity":{"type":"number"}}
    }},
    "mzStart":{"type":"number"},
    "mzStop":{"type":"number"},
    "ppmStart":{"type":"number"},
    "ppmStop":{"type":"number"}
  }
}

try:
    import jsonschema
    def validate_json(schema, data):
        jsonschema.validate(instance=data, schema=schema)
except ImportError:
    def validate_json(schema, data):
        return  # no-op if jsonschema not installed

# ---------- tiny utils ----------
def as_float(x):
    if x is None: return None
    if isinstance(x,(int,float)): return float(x)
    s=str(x).strip().replace(",","")
    m=re.search(r'[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?', s)
    return float(m.group(0)) if m else None

def as_int(x):
    if x is None: return None
    if isinstance(x,int): return x
    m=re.search(r'\d+', str(x))
    return int(m.group(0)) if m else None

def norm_mode(v):
    return str(v).strip().lower() if v is not None else None

def jsonl(lines: List[dict]) -> str:
    return "\n".join(json.dumps(x, ensure_ascii=False, separators=(",",":")) for x in lines) + "\n"

# ---------- attribute mapping ----------
ATTR_MAP = {
  "instrument": "instrument",
  "instrument type": "instrument_type",
  "ionization": "ionization",
  "ionization mode": "ionization_mode",
  "polarity": "polarity",
  "ms level": "ms_level",
  "fragmentation mode": "fragmentation_mode",
  "collision energy": "collision_energy",
  "resolution": "resolution",
  "retention time": "retention_time",
  "precursor m/z": "precursor_mz",
  "precursor type": "precursor_type",
  "column": "column",
  "flow gradient": "flow_gradient",
  "flow rate": "flow_rate",
  "date": "date",
  "accession": "accession"
}

# ---------- ES client (used only when not --dry-run) ----------
class ES:
    def __init__(self, base, auth=None, api_key=None, timeout=60):
        self.base = base.rstrip("/")
        self.auth = auth
        self.timeout = timeout
        self.headers = {"Content-Type": "application/x-ndjson"}
        if api_key:
            self.headers["Authorization"] = f"ApiKey {api_key}"
    def bulk(self, actions: List[dict]):
        if not actions: return (0, [])
        r = requests.post(
            f"{self.base}/_bulk",
            data=("\n".join(json.dumps(a, separators=(",", ":")) for a in actions) + "\n"),
            headers=self.headers,
            auth=self.auth,
            timeout=self.timeout,
        )
        r.raise_for_status()
        resp=r.json()
        errs=[it for it in resp.get("items",[]) if any(v.get("error") for v in it.values())]
        return len(resp.get("items",[])), errs


def is_compound_json(path: Path) -> bool:
    try:
        if path.suffix.lower() != ".json": return False
        j = json.loads(path.read_text(encoding="utf-8"))
        # Heuristic: must look like a compound
        return isinstance(j, dict) and ("inchiKey" in j or "inchikey" in j) and "formula" in j and "name" in j
    except Exception:
        return False

def find_compound_dirs(root: Path) -> List[Path]:
    comp_dirs = []
    for d in root.rglob("*"):
        if not d.is_dir(): continue
        # must contain at least one compound-looking JSON at this level
        if any(is_compound_json(p) for p in d.iterdir() if p.is_file()):
            comp_dirs.append(d)
    return comp_dirs

def bulk_flush(es, actions, max_bytes=90*1024*1024):
    if not actions: return actions
    payload = ("\n".join(json.dumps(a, separators=(",",":")) for a in actions) + "\n").encode()
    if len(payload) >= max_bytes:
        es.bulk(actions); return []
    return actions

# ---------- parsing & validation ----------
def pick_compound_json(comp_dir: Path) -> Optional[Path]:
    files=[p for p in comp_dir.iterdir() if p.is_file() and p.suffix.lower()==".json"]
    data=[p for p in files if p.name.lower().endswith("_data.json")]
    if data: return data[0]
    if len(files)==1: return files[0]
    # choose the one that looks like a compound
    for p in files:
        if is_compound_json(p): return p
    return None

def parse_compound(j: dict) -> Tuple[dict, Dict[str,dict], List[str]]:
    """returns (normalized_compound, spectrum_meta_map, warnings)"""
    warnings=[]
    j=dict(j)
    if "inchiKey" not in j and "inchikey" in j: j["inchiKey"]=j["inchikey"]
    if "inchiKey" in j and j["inchiKey"]: j["inchiKey"]=j["inchiKey"].upper()
    else: warnings.append("missing inchiKey")
    # coerce numerics (tolerant)
    j["exactmass"]=as_float(j.get("exactmass"))
    j["averagemass"]=as_float(j.get("averagemass"))
    j["charge"]=as_int(j.get("charge"))

    # spectra pointers → metadata map
    spectrum_ids=[]
    spectrum_meta={}
    spect = (j.get("spectra") or {}).get("MS") or []
    seen=set()
    for s in spect:
        sid = s.get("name")
        if not sid:
            warnings.append("MS entry missing 'name' (spectrum id)")
            continue
        if sid in seen:
            warnings.append(f"duplicate spectrum id in compound.spectra: {sid}")
        seen.add(sid)
        md={"inchikey": j.get("inchiKey"), "compound_id": j.get("id")}
        if s.get("splash") and isinstance(s["splash"], dict):
            md["splash"]=s["splash"].get("splash")
        if s.get("url"): md.setdefault("source", {})["url"]=s["url"]
        if s.get("submitter"): md.setdefault("source", {})["submitter"]=s["submitter"]
        # flatten attributes
        for a in (s.get("attributes") or []):
            k=(a.get("attributeName") or "").strip().lower()
            v=a.get("attributeValue")
            if k not in ATTR_MAP:
                # soft-warn on unknown attributes
                continue
            dest=ATTR_MAP[k]
            if dest == "ms_level": v = as_int(v)
            elif dest in ("resolution","retention_time","precursor_mz"): v = as_float(v)
            elif dest in ("ionization_mode","polarity"): v = norm_mode(v)
            if dest=="accession": md.setdefault("source", {})["accession"]=str(v)
            else: md[dest]=v
        spectrum_meta[str(sid)]=md
        spectrum_ids.append(str(sid))

    j["spectrum_ids"]=sorted(set(spectrum_ids))
    j["spectra_count"]=len(j["spectrum_ids"])
    return j, spectrum_meta, warnings

def parse_spectrum_file(p: Path) -> Tuple[Optional[dict], Optional[str]]:
    try:
        s=json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        return None, f"parse_error: {e}"
    # schema/shape checks happen outside so we can switch them off
    out={
      "spectrumId": s.get("spectrumId"),
      "modality": s.get("modality") or "MS",
      "mzStart": as_float(s.get("mzStart")),
      "mzStop": as_float(s.get("mzStop")),
      "ppmStart": as_float(s.get("ppmStart")),
      "ppmStop": as_float(s.get("ppmStop")),
    }
    peaks = s.get("peaks")
    if not isinstance(peaks, list): return None, "invalid: peaks not a list"
    mz_arr=[]; it_arr=[]
    for pp in peaks:
        if not isinstance(pp, dict) or "mz" not in pp or "intensity" not in pp:
            return None, "invalid: peak missing mz/intensity"
        mz_arr.append(as_float(pp.get("mz")))
        it_arr.append(as_float(pp.get("intensity")))
    out["peaks_mz"]=mz_arr
    out["peaks_intensity"]=it_arr
    return out, None

def normalize_compound(d):
    """
    Normalize a raw compound dict into an ES-friendly document.
    - Flattens species -> species_hits[]
    - Flattens pathways into compact lists
      * ReactomePathways may be a dict of lists keyed by species; we dedupe across species.
    """
    # 1) species → species_hits[]
    hits = []
    for sp_name, items in (d.get("species") or {}).items():
        study_ids = {i.get("SpeciesAccession") for i in items if i.get("SpeciesAccession")}
        assay_sum = sum(int(i.get("Assay", 0) or 0) for i in items)
        hits.append({"species": sp_name, "study_ids": sorted(study_ids), "assay_sum": assay_sum})

    # 2) pathways → compact lists
    # KEGG: keep simple list (id, name, ko)
    kegg = []
    for p in (d.get("pathways", {}).get("KEGGPathways") or []):
        if not isinstance(p, dict):
            continue
        kegg.append({
            "id": p.get("id"),
            "name": p.get("name"),
            "ko": p.get("KO_PATHWAYS")
        })

    # Reactome: may be dict-of-lists keyed by species; each item has name, pathwayId, url, reactomeId
    reactome = []
    seen_rx = set()
    rx_src = (d.get("pathways", {}) or {}).get("ReactomePathways") or []
    if isinstance(rx_src, dict):
        # dict: { "Homo sapiens": [ {...}, ... ], "Mus musculus": [ {...}, ... ] }
        for _species, lst in rx_src.items():
            for p in (lst or []):
                if not isinstance(p, dict):
                    continue
                rid = p.get("reactomeId") or p.get("pathwayId") or p.get("id")
                name = p.get("name")
                key = (rid or "", name or "")
                if key in seen_rx:
                    continue
                seen_rx.add(key)
                reactome.append({"id": rid, "name": name})
    else:
        # already a flat list
        for p in (rx_src or []):
            if not isinstance(p, dict):
                continue
            rid = p.get("reactomeId") or p.get("pathwayId") or p.get("id")
            name = p.get("name")
            key = (rid or "", name or "")
            if key in seen_rx:
                continue
            seen_rx.add(key)
            reactome.append({"id": rid, "name": name})

    # WikiPathways: keep simple list (id, name)
    wikipw = []
    for p in (d.get("pathways", {}).get("WikiPathways") or []):
        if not isinstance(p, dict):
            continue
        wikipw.append({"id": p.get("id"), "name": p.get("name")})

    # counts / presence
    synonyms = d.get("synonyms") or []
    iupacs = d.get("iupacNames") or []
    citations = d.get("citations") or []
    reactions = d.get("reactions") or []
    spectrum_ids = d.get("spectrum_ids") or []
    spectra_count = int(d.get("spectra_count") or 0)

    # ---- keep citations/reactions mapping-safe; full originals stay in d/raw ----
    allowed_cit_keys = {"source", "type", "value", "title", "doi", "author", "year"}
    clean_citations = []
    for c in citations:
        if not isinstance(c, dict):
            continue
        clean_citations.append({k: v for k, v in c.items() if k in allowed_cit_keys})
    citations = clean_citations

    allowed_rxn_keys = {"id", "name"}
    clean_reactions = []
    for r in reactions:
        if not isinstance(r, dict):
            continue
        clean_reactions.append({k: v for k, v in r.items() if k in allowed_rxn_keys})
    reactions = clean_reactions

    doc = {
        "id": d.get("id"),
        "name": d.get("name"),
        "definition": d.get("definition"),
        "iupacNames": iupacs,
        "synonyms": synonyms,
        "smiles": d.get("smiles"),
        "inchi": d.get("inchi"),
        "inchiKey": d.get("inchiKey"),
        "formula": d.get("formula"),
        "charge": int(d.get("charge") or 0),
        "averagemass": float(d.get("averagemass") or 0) if d.get("averagemass") else None,
        "exactmass":  float(d.get("exactmass")  or 0) if d.get("exactmass")  else None,

        "flags": {
            "hasLiterature": d.get("flags", {}).get("hasLiterature") == "true",
            "hasReactions":  d.get("flags", {}).get("hasReactions")  == "true",
            "hasSpecies":    d.get("flags", {}).get("hasSpecies")    == "true",
            "hasPathways":   d.get("flags", {}).get("hasPathways")   == "true",
            "hasNMR":        d.get("flags", {}).get("hasNMR")        == "true",
            "hasMS":         d.get("flags", {}).get("hasMS")         == "true",

            "hasMolfile":        bool(d.get("structure")),
            "hasSmiles":         bool(d.get("smiles")),
            "hasInchi":          bool(d.get("inchi")),
            "hasSynonyms":       len(synonyms) > 0,
            "hasIupac":          len(iupacs) > 0,
            "hasCitations":      len(citations) > 0,
            "hasReactionsList":  len(reactions) > 0,
            "hasSpeciesHits":    len(hits) > 0,
            "hasKegg":           len(kegg) > 0,
            "hasReactome":       len(reactome) > 0,
            "hasWikiPathways":   len(wikipw) > 0,
            "hasSpectraListed":  spectra_count > 0,
            "hasExactMass":      d.get("exactmass") is not None,
            "hasAverageMass":    d.get("averagemass") is not None,
            "hasCharge":         str(d.get("charge") or "0") not in ("", "0"),
        },

        "counts": {
            "synonyms": len(synonyms),
            "iupac": len(iupacs),
            "citations": len(citations),
            "reactions": len(reactions),
            "species_hits": len(hits),
            "species_total_assays": sum(h.get("assay_sum", 0) for h in hits),
            "kegg": len(kegg),
            "reactome": len(reactome),
            "wikipathways": len(wikipw),
            "spectra": spectra_count,
        },

        "pathways": {
            "kegg": kegg,
            "reactome": reactome,
            "wikipathways": wikipw
        },

        "citations": citations,
        "reactions": reactions,
        "species_hits": hits,

        "spectrum_ids": spectrum_ids,
        "spectra_count": spectra_count,

        # keep retrieval-only bits
        "structure_molfile": base64.b64encode((d.get("structure") or "").encode("utf-8")).decode("ascii"),
        "raw": d
    }
    return doc
def normalize_spectrum(s: dict) -> dict:
    """
    Normalize a spectrum doc to a compact, mapping-friendly shape and add a few
    derived metrics. Keeps only a safe whitelist to avoid field explosion.
    """
    out = {}

    # --- identity & modality ---
    out["spectrumId"] = str(s.get("spectrumId")) if s.get("spectrumId") is not None else None
    mod = s.get("modality") or "MS"
    out["modality"] = str(mod).upper()

    # --- peaks (clean, typed, derived) ---
    mz = [as_float(x) for x in (s.get("peaks_mz") or [])]
    it = [as_float(x) for x in (s.get("peaks_intensity") or [])]
    pairs = [(m, i) for m, i in zip(mz, it) if m is not None and i is not None]
    if pairs:
        mz, it = map(list, zip(*pairs))
    else:
        mz, it = [], []
    out["peaks_mz"] = mz
    out["peaks_intensity"] = it
    out["n_peaks"] = len(mz)

    if mz:
        out["min_mz"] = min(mz)
        out["max_mz"] = max(mz)

    # mz/ppm windows (prefer explicit; fall back to computed when possible)
    mzStart = as_float(s.get("mzStart"))
    mzStop  = as_float(s.get("mzStop"))
    out["mzStart"] = mzStart if mzStart is not None else (out.get("min_mz"))
    out["mzStop"]  = mzStop  if mzStop  is not None else (out.get("max_mz"))

    if "ppmStart" in s: out["ppmStart"] = as_float(s.get("ppmStart"))
    if "ppmStop"  in s: out["ppmStop"]  = as_float(s.get("ppmStop"))

    # TIC and base peak
    if it:
        out["tic"] = sum(v for v in it if v is not None)
        idx = max(range(len(it)), key=lambda k: it[k])
        out["bpi"] = it[idx]
        out["base_peak_mz"] = mz[idx]

    # --- carry whitelisted meta (typed) ---
    META_KEYS = (
        "inchikey","compound_id","instrument","instrument_type","ionization",
        "ionization_mode","polarity","ms_level","fragmentation_mode",
        "collision_energy","resolution","retention_time","precursor_mz",
        "precursor_type","column","flow_gradient","flow_rate","date"
    )
    for k in META_KEYS:
        v = s.get(k)
        if v is None:
            continue
        if k in ("ms_level",):
            v = as_int(v)
        elif k in ("collision_energy","resolution","retention_time","precursor_mz"):
            v = as_float(v)
        elif k in ("ionization_mode","polarity"):
            v = norm_mode(v)
        out[k] = v

    # source sub-object (keep it tight)
    if isinstance(s.get("source"), dict):
        src = {}
        for kk in ("url","submitter","accession"):
            if s["source"].get(kk) is not None:
                src[kk] = str(s["source"][kk])
        if src:
            out["source"] = src

    return out

def compute_structure_features(d: dict):
    """Returns (fp_dense, fp_bits, elements, canon_smiles) or Nones if RDKit missing/parse fails."""
    if not Chem: return (None, None, None, None)
    mol = None
    molfile = d.get("structure") or ""
    smi = d.get("smiles")
    if molfile:
        mol = Chem.MolFromMolBlock(molfile, sanitize=True)
    if not mol and smi:
        mol = Chem.MolFromSmiles(smi)
    if not mol: return (None, None, None, None)

    # canonical smiles
    canon_smiles = Chem.MolToSmiles(mol, canonical=True)

    # elements
    elems = sorted({a.GetSymbol() for a in mol.GetAtoms()})

    # ECFP4 (radius=2), 2048 bits
    bv = AllChem.GetMorganFingerprintAsBitVect(mol, radius=2, nBits=2048)
    onbits = list(bv.GetOnBits())
    fp_dense = [1.0 if i in onbits else 0.0 for i in range(2048)]
    fp_bits = [str(i) for i in onbits]

    return (fp_dense, fp_bits, elems, canon_smiles)

def dump_bulk_errs(errs, label, n=5):
    print(f"[{label} BULK ERR] showing {min(n, len(errs))}/{len(errs)}", file=sys.stderr)
    for it in errs[:n]:
        # each item is like {"index": {...}} or {"create": {...}}
        op, info = next(iter(it.items()))
        err = info.get("error") or {}
        status = info.get("status")
        doc_id = info.get("_id")
        idx = info.get("_index")
        print(
            f"  - op={op} status={status} index={idx} id={doc_id}\n"
            f"    error_type={err.get('type')}\n"
            f"    reason={err.get('reason')}\n"
            f"    caused_by={err.get('caused_by')}\n",
            file=sys.stderr
        )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root-dir", required=True, help="Root directory containing per-compound folders")
    ap.add_argument("--es", default="https://wwwdev.ebi.ac.uk/metabolights/es/")
    ap.add_argument("--user", default=None)
    ap.add_argument("--password", default=None)
    ap.add_argument("--compounds-index", default="compounds_v1")
    ap.add_argument("--spectra-index", default="spectra_v1")
    ap.add_argument("--pipeline", default="spectra_pipeline_v1")
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--dry-run", action="store_true", help="Do not index; only validate and report")
    ap.add_argument("--validate", action="store_true", help="Validate against JSON Schemas (requires jsonschema)")
    ap.add_argument("--report", default=None, help="Directory to write JSONL reports")
    ap.add_argument("--api-key", default=None, help="Elasticsearch API key")

    args = ap.parse_args()
    if args.api_key:
        es = None if args.dry_run else ES(args.es, api_key=args.api_key)
    else:
        auth = (args.user, args.password) if args.user else None
        es = None if args.dry_run else ES(args.es, auth=auth)
    #auth = (args.user,args.password) if args.user else None
    #es = None if args.dry_run else ES(args.es, **auth)

    # reports
    comp_rows = []
    spec_rows = []

    comp_ok = comp_invalid = comp_parse = 0
    spec_ok = spec_invalid = spec_parse = spec_unlinked = 0

    comp_actions = []
    spec_actions = []

    compounds_with_multiple_spectrum = []
    total_spectra_files = []

    root = Path(args.root_dir)
    comp_dirs = find_compound_dirs(root)
    for comp_dir in sorted(comp_dirs):
        comp_path=pick_compound_json(comp_dir)
        if not comp_path:
            comp_parse += 1
            comp_rows.append(
                {"type":"compound","path":str(comp_dir),"status":"parse_error","reason":"no compound json found"}
            )
            continue

        try:
            raw=json.loads(comp_path.read_text(encoding="utf-8"))
        except Exception as e:
            comp_parse += 1
            comp_rows.append({"type":"compound","path":str(comp_path),"status":"parse_error","reason":str(e)})
            continue

        # schema validation
        reasons=[]
        if args.validate:
            try:
                validate_json(COMPOUND_SCHEMA, raw)
            except Exception as e:
                reasons.append(f"schema: {e}")

        comp_doc, meta_map, warns = parse_compound(raw)
        # some compounds have identical inchikeys, so we make a source ID by hand to prevent overwriting
        src_uid = hashlib.sha1(str(comp_path.resolve()).encode("utf-8")).hexdigest()[:16]

        reasons.extend(warns)

        if ("inchiKey" not in comp_doc) or not comp_doc.get("inchiKey"):
            reasons.append("missing inchiKey (cannot route/enrich spectra)")

        if reasons:
            comp_invalid += 1
            comp_rows.append({"type":"compound","path":str(comp_path),"id":raw.get("id"),"inchiKey":raw.get("inchiKey") or raw.get("inchikey"),
                              "status":"invalid","reasons":reasons})
        else:
            comp_ok += 1
            comp_rows.append({"type":"compound","path":str(comp_path),"id":comp_doc.get("id"),
                              "inchiKey":comp_doc.get("inchiKey"),"status":"ok","spectra_listed":comp_doc.get("spectra_count")})

        # Index compound (only if not dry-run)

        comp_doc = normalize_compound(comp_doc)

        if not args.dry_run:
            ik = comp_doc.get("inchiKey")

            comp_doc["source"] = {
                "path": str(comp_path),
                "dir": str(comp_dir),
                "filename": comp_path.name,
                "uid": src_uid,
            }
            comp_doc["inchiKey_std"] = ik

            index_meta = {
                "_index": args.compounds_index,
                "_id": f"compound:{ik}:{src_uid}" if ik else f"compound:NA:{src_uid}",
            }
            if ik:
                index_meta["routing"] = ik

            comp_actions.append({"index": index_meta})
            comp_actions.append(comp_doc)
            if len(comp_actions) >= args.batch * 2:
                _, errs = es.bulk(comp_actions)
                comp_actions = []
                if errs:

                    print(f"[COMPOUND BULK ERR] {len(errs)}", file=sys.stderr)
                    dump_bulk_errs(errs, "COMPOUND")

        # spectra under this compound dir
        spectra_files = [p for p in comp_dir.rglob("*") if
                         p.is_file() and p.suffix.lower() == ".json" and p != comp_path]
        if len(spectra_files) > 1:
            compounds_with_multiple_spectrum.append(comp_dir)
        total_spectra_files.extend(spectra_files)
        for sf in spectra_files:
            spec_doc, err = parse_spectrum_file(sf)
            if err:
                spec_parse += 1
                spec_rows.append({"type":"spectrum","path":str(sf),"status":"parse_error","reason":err})
                continue

            # schema validation
            reasons=[]
            if args.validate:
                try:
                    validate_json(SPECTRUM_SCHEMA, {"spectrumId":spec_doc.get("spectrumId"),"peaks":[{"mz":1,"intensity":1}]})
                    # We validated structure shape lightly; detailed peaks already parsed
                    if not spec_doc.get("spectrumId"):
                        raise Exception("missing spectrumId")
                except Exception as e:
                    reasons.append(f"schema: {e}")

            sid=str(spec_doc.get("spectrumId"))
            meta = meta_map.get(sid)
            if not meta:
                spec_unlinked += 1
                reasons.append("unlinked: spectrumId not listed under compound.spectra.MS")
            else:
                # merge meta into doc (only for indexing path)
                pass

            # sanity on peaks
            if not spec_doc["peaks_mz"] or not spec_doc["peaks_intensity"]:
                reasons.append("empty peaks")
            elif len(spec_doc["peaks_mz"]) != len(spec_doc["peaks_intensity"]):
                reasons.append("peaks length mismatch")

            status = "ok" if not reasons else "invalid"
            if status=="ok": spec_ok += 1
            else: spec_invalid += 1

            spec_rows.append({
                "type":"spectrum","path":str(sf),"spectrumId":sid,
                "status":status,"reasons":reasons or None
            })

            # Index spectrum (only if not dry-run)
            if not args.dry_run:
                spec_uid = hashlib.sha1(str(sf.resolve()).encode("utf-8")).hexdigest()[:16]

                if meta:
                    spec_doc.update({k: v for k, v in meta.items() if k != "source"})
                    if "source" in meta:
                        src = spec_doc.get("source", {})
                        src.update(meta["source"])
                        spec_doc["source"] = src
                    spec_doc["inchiKey_std"] = meta.get("inchikey")

                # provenance linkage back to the compound source entry
                links = spec_doc.get("links", {}) or {}
                links["compound_source_uid"] = src_uid
                spec_doc["links"] = links

                action = {
                    "_index": args.spectra_index,
                    "_id": f"spectrum:{sid}:{spec_uid}",
                    "pipeline": args.pipeline,
                }
                if meta and meta.get("inchikey"):
                    action["routing"] = meta["inchikey"]

                spec_actions.append({"index": action})
                spec_actions.append(spec_doc)
                if len(spec_actions) >= args.batch * 2:
                    _, errs = es.bulk(spec_actions)
                    spec_actions = []
                    if errs:
                        print(f"[SPECTRA BULK ERR] {len(errs)}", file=sys.stderr)
                        dump_bulk_errs(errs, 'SPECTRA')

    # final flush (if indexing)
    if not args.dry_run:
        if comp_actions: es.bulk(comp_actions)
        if spec_actions: es.bulk(spec_actions)

    # write reports
    if args.report:
        outdir=Path(args.report); outdir.mkdir(parents=True, exist_ok=True)
        (outdir/"compounds.jsonl").write_text(jsonl(comp_rows), encoding="utf-8")
        (outdir/"spectra.jsonl").write_text(jsonl(spec_rows), encoding="utf-8")

    # summary
    print("\n=== DRY RUN SUMMARY ===" if args.dry_run else "\n=== LOAD SUMMARY ===")
    print(f"Compounds: ok={comp_ok} invalid={comp_invalid} parse_error={comp_parse}")
    print(f"Spectra:   ok={spec_ok} invalid={spec_invalid} parse_error={spec_parse} unlinked={spec_unlinked}")
    print(f"Compounds  with more than one spectra: {len(compounds_with_multiple_spectrum)}")
    print(f"Total spectra: {len(total_spectra_files)}")

if __name__=="__main__":
    main()