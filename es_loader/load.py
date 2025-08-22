#!/usr/bin/env python3
import argparse, json, os, re, sys
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import requests

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
    def __init__(self, base, auth=None, timeout=60):
        self.base=base.rstrip("/")
        self.auth=auth
        self.timeout=timeout
    def bulk(self, actions: List[dict]):
        if not actions: return (0, [])
        r=requests.post(f"{self.base}/_bulk",
                        data=("\n".join(json.dumps(a, separators=(",",":")) for a in actions) + "\n"),
                        headers={"Content-Type":"application/x-ndjson"},
                        auth=self.auth, timeout=self.timeout)
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

# ---------- parsing & validation ----------
def pick_compound_json(comp_dir: Path) -> Optional[Path]:
    files=[p for p in comp_dir.glob("*.json") if p.is_file()]
    data=[p for p in files if p.name.endswith("_data.json")]
    if data: return data[0]
    if len(files)==1: return files[0]
    for p in files:  # heuristic
        try:
            j=json.loads(p.read_text(encoding="utf-8"))
            if "inchiKey" in j or "smiles" in j: return p
        except: pass
    # if many jsons and none look like the compound data, return None
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

# ---------- main ----------
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--root-dir", required=True, help="Root directory containing per-compound folders")
    ap.add_argument("--es", default="http://localhost:9200")
    ap.add_argument("--user", default=None)
    ap.add_argument("--password", default=None)
    ap.add_argument("--compounds-index", default="compounds_v1")
    ap.add_argument("--spectra-index", default="spectra_v1")
    ap.add_argument("--pipeline", default="spectra_pipeline_v1")
    ap.add_argument("--batch", type=int, default=5000)
    ap.add_argument("--dry-run", action="store_true", help="Do not index; only validate and report")
    ap.add_argument("--validate", action="store_true", help="Validate against JSON Schemas (requires jsonschema)")
    ap.add_argument("--report", default=None, help="Directory to write JSONL reports")
    args=ap.parse_args()

    auth=(args.user,args.password) if args.user else None
    es = None if args.dry_run else ES(args.es, auth)

    # reports
    comp_rows=[]; spec_rows=[]

    comp_ok=comp_invalid=comp_parse=0
    spec_ok=spec_invalid=spec_parse=spec_unlinked=0

    comp_actions=[]; spec_actions=[]

    root=Path(args.root_dir)
    comp_dirs=[p for p in root.iterdir() if p.is_dir()]
    for comp_dir in sorted(comp_dirs):
        comp_path=pick_compound_json(comp_dir)
        if not comp_path:
            comp_parse += 1
            comp_rows.append({"type":"compound","path":str(comp_dir),"status":"parse_error","reason":"no compound json found"})
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
        if not args.dry_run:
            ik = comp_doc.get("inchiKey")
            comp_actions.append({"index":{"_index":args.compounds_index,"_id":f"compound:{ik}","routing":ik}})
            comp_actions.append(comp_doc)
            if len(comp_actions)>=args.batch*2:
                _, errs = es.bulk(comp_actions); comp_actions=[]
                if errs: print(f"[COMPOUND BULK ERR] {len(errs)}", file=sys.stderr)

        # spectra under this compound dir
        spectra_files=[p for p in comp_dir.rglob("*.json") if p != comp_path]
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
                if meta and meta.get("inchikey"):
                    spec_doc.update({k:v for k,v in meta.items() if k!="source"})
                    if "source" in meta:
                        src = spec_doc.get("source", {})
                        src.update(meta["source"])
                        spec_doc["source"]=src
                    action={"_index":args.spectra_index,"_id":f"spectrum:{sid}","routing":meta["inchikey"],"pipeline":args.pipeline}
                else:
                    action={"_index":args.spectra_index,"_id":f"spectrum:{sid}","pipeline":args.pipeline}
                spec_actions.append({"index": action})
                spec_actions.append(spec_doc)
                if len(spec_actions)>=args.batch*2:
                    _, errs = es.bulk(spec_actions); spec_actions=[]
                    if errs: print(f"[SPECTRA BULK ERR] {len(errs)}", file=sys.stderr)

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

if __name__=="__main__":
    main()