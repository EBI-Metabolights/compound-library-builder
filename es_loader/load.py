#!/usr/bin/env python3
import argparse, json, os, re, sys
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import requests

# ------------------ small helpers ------------------
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

def jsonl(lines: List[dict]) -> bytes:
    return ("\n".join(json.dumps(x, separators=(",",":")) for x in lines) + "\n").encode()

class ES:
    def __init__(self, base, auth=None, timeout=60):
        self.base=base.rstrip("/")
        self.auth=auth
        self.timeout=timeout
    def bulk(self, actions: List[dict]):
        if not actions: return (0, [])
        r=requests.post(f"{self.base}/_bulk", data=jsonl(actions),
                        headers={"Content-Type":"application/x-ndjson"},
                        auth=self.auth, timeout=self.timeout)
        r.raise_for_status()
        resp=r.json()
        errs=[it for it in resp.get("items",[]) if any(v.get("error") for v in it.values())]
        return len(resp.get("items",[])), errs

# ------------------ parsing ------------------
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

def pick_compound_json(comp_dir: Path) -> Optional[Path]:
    files=[p for p in comp_dir.glob("*.json") if p.is_file()]
    # Prefer *_data.json; otherwise if only one JSON, use it.
    data=[p for p in files if p.name.endswith("_data.json")]
    if data: return data[0]
    if len(files)==1: return files[0]
    # Heuristic: smallest file with key fields
    for p in files:
        try:
            j=json.loads(p.read_text(encoding="utf-8"))
            if "inchiKey" in j or "smiles" in j:
                return p
        except: pass
    return None

def parse_compound(j: dict) -> Tuple[dict, Dict[str,dict]]:
    # normalize
    j=dict(j)
    j["inchiKey"]=(j.get("inchiKey") or j.get("inchikey") or "").upper()
    j["exactmass"]=as_float(j.get("exactmass"))
    j["averagemass"]=as_float(j.get("averagemass"))
    j["charge"]=as_int(j.get("charge"))
    # spectra pointers
    spectrum_ids=[]
    spectrum_meta={}
    spect = (j.get("spectra") or {}).get("MS") or []
    for s in spect:
      sid = s.get("name") or None
      if sid:
        spectrum_ids.append(str(sid))
        md={"inchikey": j["inchiKey"], "compound_id": j.get("id")}
        # source/package-level fields
        if s.get("splash") and isinstance(s["splash"], dict):
            md["splash"]=s["splash"].get("splash")
        if s.get("url"): md.setdefault("source", {})["url"]=s["url"]
        if s.get("submitter"): md.setdefault("source", {})["submitter"]=s["submitter"]
        # flatten attributes
        for a in (s.get("attributes") or []):
            k=(a.get("attributeName") or "").strip().lower()
            v=a.get("attributeValue")
            dest=ATTR_MAP.get(k)
            if not dest: continue
            if dest in ("ms_level","resolution"): v = as_int(v) if dest=="ms_level" else as_float(v)
            if dest in ("ionization_mode","polarity"): v = norm_mode(v)
            if dest=="precursor_mz": v = as_float(v)
            if dest=="retention_time": v = as_float(v)
            if dest=="accession": md.setdefault("source", {})["accession"]=str(v)
            else: md[dest]=v
        spectrum_meta[str(sid)]=md
    j["spectrum_ids"]=sorted(set(spectrum_ids))
    j["spectra_count"]=len(j["spectrum_ids"])
    return j, spectrum_meta

def parse_spectrum_file(p: Path) -> Optional[dict]:
    try:
        s=json.loads(p.read_text(encoding="utf-8"))
        if "spectrumId" not in s or "peaks" not in s: return None
        out={
          "spectrumId": s["spectrumId"],
          "modality": s.get("modality") or "MS",
          "mzStart": as_float(s.get("mzStart")),
          "mzStop": as_float(s.get("mzStop")),
          "peaks_mz": [as_float(pp.get("mz")) for pp in (s.get("peaks") or []) if pp.get("mz") is not None],
          "peaks_intensity": [as_float(pp.get("intensity")) for pp in (s.get("peaks") or []) if pp.get("intensity") is not None]
        }
        return out
    except Exception as e:
        print(f"[WARN] Unable to read {p}: {e}", file=sys.stderr)
        return None

# ------------------ main walk + bulk ------------------
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--root-dir", required=True)
    ap.add_argument("--es", default="http://localhost:9200")
    ap.add_argument("--user", default=None)
    ap.add_argument("--password", default=None)
    ap.add_argument("--compounds-index", default="compounds_v1")
    ap.add_argument("--spectra-index", default="spectra_v1")
    ap.add_argument("--pipeline", default="spectra_pipeline_v1")
    ap.add_argument("--batch", type=int, default=5000)
    args=ap.parse_args()

    auth=(args.user,args.password) if args.user else None
    es=ES(args.es, auth)

    comp_actions=[]
    spec_actions=[]
    total_specs=0
    matched=0

    root=Path(args.root_dir)
    for comp_dir in sorted([p for p in root.iterdir() if p.is_dir()]):
        comp_json_path=pick_compound_json(comp_dir)
        if not comp_json_path:
            continue
        comp_raw=json.loads(comp_json_path.read_text(encoding="utf-8"))
        comp_doc, meta_map = parse_compound(comp_raw)
        ik = comp_doc["inchiKey"]
        comp_actions.append({ "index": { "_index": args.compounds_index, "_id": f"compound:{ik}", "routing": ik } })
        comp_actions.append(comp_doc)

        # find spectra JSONs anywhere under this compound dir (excluding the compound json file)
        for p in comp_dir.rglob("*.json"):
            if p == comp_json_path: continue
            spec_doc=parse_spectrum_file(p)
            if not spec_doc: continue
            total_specs += 1
            sid=str(spec_doc["spectrumId"])
            meta=meta_map.get(sid)
            if meta:
                matched += 1
                # merge metadata and source
                spec_doc.update({k:v for k,v in meta.items() if k not in ("source",)})
                if "source" in meta:
                    src = spec_doc.get("source", {})
                    src.update(meta["source"])
                    spec_doc["source"]=src
                if "inchikey" in meta:
                    spec_actions.append({ "index": {
                        "_index": args.spectra_index,
                        "_id": f"spectrum:{sid}",
                        "routing": meta["inchikey"],
                        "pipeline": args.pipeline
                    }})
                else:
                    spec_actions.append({ "index": {
                        "_index": args.spectra_index,
                        "_id": f"spectrum:{sid}",
                        "pipeline": args.pipeline
                    }})
            else:
                # still index; pipeline can’t enrich without inchikey
                spec_actions.append({ "index": {
                    "_index": args.spectra_index,
                    "_id": f"spectrum:{sid}",
                    "pipeline": args.pipeline
                }})
            spec_actions.append(spec_doc)

        # flush in batches
        if len(comp_actions) >= args.batch*2:
            es.bulk(comp_actions); comp_actions=[]
        if len(spec_actions) >= args.batch*2:
            es.bulk(spec_actions); spec_actions=[]

    if comp_actions: es.bulk(comp_actions)
    if spec_actions: es.bulk(spec_actions)

    print(f"[DONE] Spectra indexed: {total_specs} (matched to compound metadata: {matched})")

if __name__=="__main__":
    main()
