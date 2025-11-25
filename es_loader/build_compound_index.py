"""
Bootstrap compounds + spectra indices and pipelines (no loader).

Steps:
- PUT /compounds_v1
- PUT /spectra_v1
- PUT /_enrich/policy/compound_by_inchikey
- POST /_enrich/policy/compound_by_inchikey/_execute
- PUT /_ingest/pipeline/spectra_pipeline_v1
- POST /_aliases (optional)

Auth:
- Prefer API key via Authorization: ApiKey <key>
- Otherwise basic auth via --user/--password
"""

import argparse
import json
from typing import Any, Dict, Optional

import requests
from requests.auth import HTTPBasicAuth


COMPOUNDS_INDEX_BODY: Dict[str, Any] = {
    "settings": {"index.mapping.total_fields.limit": 2000},
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "id":           {"type": "keyword"},
            "name":         {"type": "text", "fields": {"kw": {"type": "keyword"}}},
            "definition":   {"type": "text"},
            "iupacNames":   {"type": "keyword"},
            "synonyms":     {"type": "keyword"},
            "smiles":       {"type": "keyword"},
            "inchi":        {"type": "keyword"},
            "inchiKey":     {"type": "keyword"},
            "formula":      {"type": "keyword"},
            "charge":       {"type": "byte"},
            "averagemass":  {"type": "float"},
            "exactmass":    {"type": "float"},

            "flags": {
                "type": "object",
                "dynamic": "strict",
                "properties": {
                    "hasLiterature": {"type": "boolean"},
                    "hasReactions":  {"type": "boolean"},
                    "hasSpecies":    {"type": "boolean"},
                    "hasPathways":   {"type": "boolean"},
                    "hasNMR":        {"type": "boolean"},
                    "hasMS":         {"type": "boolean"},
                },
            },

            "pathways": {
                "properties": {
                    "kegg": {
                        "type": "nested",
                        "properties": {
                            "id":   {"type": "keyword"},
                            "name": {"type": "text", "fields": {"kw": {"type": "keyword"}}},
                            "ko":   {"type": "keyword"},
                        },
                    },
                    "wikipathways": {"type": "keyword"},
                    "reactome":     {"type": "keyword"},
                }
            },

            "citations": {
                "type": "nested",
                "properties": {
                    "source": {"type": "keyword"},
                    "type":   {"type": "keyword"},
                    "value":  {"type": "keyword"},
                    "title":  {"type": "text"},
                    "doi":    {"type": "keyword"},
                    "author": {"type": "text"},
                    "year":   {"type": "short"},
                },
            },

            "reactions": {
                "type": "nested",
                "properties": {
                    "id":   {"type": "keyword"},
                    "name": {"type": "text", "fields": {"kw": {"type": "keyword"}}},
                },
            },

            "species_hits": {
                "type": "nested",
                "properties": {
                    "species":   {"type": "keyword"},
                    "study_ids": {"type": "keyword"},
                    "assay_sum": {"type": "short"},
                },
            },

            "structure_molfile": {"type": "binary"},
            "raw":               {"type": "object", "enabled": False},
        }
    }
}

SPECTRA_INDEX_BODY: Dict[str, Any] = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 1},
    "mappings": {
        "properties": {
            "spectrumId": {"type": "keyword"},
            "modality":   {"type": "keyword"},

            "inchikey":    {"type": "keyword"},
            "compound_id": {"type": "keyword"},
            "compound_summary": {
                "properties": {
                    "name":      {"type": "keyword"},
                    "formula":   {"type": "keyword"},
                    "exactmass": {"type": "double"},
                }
            },

            "instrument":      {"type": "keyword"},
            "technique":       {"type": "keyword"},
            "ionization_mode": {"type": "keyword"},
            "polarity":        {"type": "keyword"},
            "ms_level":        {"type": "byte"},
            "precursor_mz":    {"type": "double"},
            "precursor_mz_bin_10ppm": {"type": "integer"},
            "retention_time":  {"type": "double"},

            "mzStart": {"type": "double"},
            "mzStop":  {"type": "double"},

            "peaks_mz":        {"type": "double"},
            "peaks_intensity": {"type": "double"},
        }
    }
}

ENRICH_POLICY_BODY: Dict[str, Any] = {
    "match": {
        "indices": "compounds_enrich_v1",
        "match_field": "inchiKey",
        "enrich_fields": ["compound_id", "name", "formula", "exactmass"],
    }
}

SPECTRA_PIPELINE_BODY: Dict[str, Any] = {
    "processors": [
        {"enrich": {
            "policy_name": "compound_by_inchikey",
            "field": "inchikey",
            "target_field": "compound_summary",
            "max_matches": 1,
        }},
        {"script": {
            "if": "ctx.precursor_mz != null",
            "source": "ctx.precursor_mz_bin_10ppm = (int)Math.floor(ctx.precursor_mz * 1e4);",
        }},
    ]
}

ALIASES_BODY: Dict[str, Any] = {
    "actions": [
        {"add": {"index": "compounds_v1", "alias": "compounds"}},
        {"add": {"index": "compounds_enrich_v1", "alias": "compounds_enrich"}},
        {"add": {"index": "spectra_v1", "alias": "spectra"}},
    ]
}


def build_session(api_key: Optional[str], user: Optional[str], password: Optional[str]) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json"})
    if api_key:
        s.headers.update({"Authorization": f"ApiKey {api_key}"})
    elif user and password:
        s.auth = HTTPBasicAuth(user, password)
    return s


def req(
    s: requests.Session,
    method: str,
    url: str,
    body: Optional[Dict[str, Any]] = None,
    ok_status=(200, 201),
) -> Dict[str, Any]:
    data = json.dumps(body) if body is not None else None
    r = s.request(method, url, data=data)
    if r.status_code not in ok_status:
        raise RuntimeError(f"{method} {url} failed: {r.status_code}\n{r.text}")
    if r.text.strip():
        try:
            return r.json()
        except Exception:
            return {"raw": r.text}
    return {}


def main() -> int:
    p = argparse.ArgumentParser(description="Bootstrap compounds + spectra indices and pipelines (no loader).")
    p.add_argument("--es-root", required=True, help="Elasticsearch root URL, e.g. https://host:9200")
    p.add_argument("--apikey", default=None, help="Elasticsearch API key. If set, overrides basic auth.")
    p.add_argument("--user", default=None, help="Basic auth user (if not using apikey).")
    p.add_argument("--password", default=None, help="Basic auth password (if not using apikey).")
    p.add_argument("--add-aliases", action="store_true", help="Add aliases as in the markdown.")
    args = p.parse_args()

    es = args.es_root.rstrip("/")
    s = build_session(args.apikey, args.user, args.password)

    print(f"==> Creating compounds_v1")
    req(s, "PUT", f"{es}/compounds_v1", COMPOUNDS_INDEX_BODY)

    print(f"==> Creating spectra_v1")
    req(s, "PUT", f"{es}/spectra_v1", SPECTRA_INDEX_BODY)

    print(f"==> Creating enrich policy compound_by_inchikey")
    req(s, "PUT", f"{es}/_enrich/policy/compound_by_inchikey", ENRICH_POLICY_BODY)

    print(f"==> Executing enrich policy compound_by_inchikey")
    req(s, "POST", f"{es}/_enrich/policy/compound_by_inchikey/_execute", body=None, ok_status=(200, 201))

    print(f"==> Creating ingest pipeline spectra_pipeline_v1")
    req(s, "PUT", f"{es}/_ingest/pipeline/spectra_pipeline_v1", SPECTRA_PIPELINE_BODY)

    if args.add_aliases:
        print(f"==> Adding aliases")
        req(s, "POST", f"{es}/_aliases", ALIASES_BODY)

    print("==> Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())