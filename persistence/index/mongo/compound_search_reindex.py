import argparse
import json
from typing import Any, Dict, Iterable, Optional, List

from pymongo import MongoClient
import requests
from requests.auth import HTTPBasicAuth

from compound_common.argparse_classes.parsers import ArgParsers
from persistence.db.mongo.mongo_client import MongoWrapper

COMPOUNDS_SEARCH_INDEX_BODY: Dict[str, Any] = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1,
        "index.mapping.total_fields.limit": 256,
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "id": {"type": "keyword"},
            "name": {
                "type": "text",
                "fields": {"kw": {"type": "keyword"}},
            },
            "definition": {"type": "text"},
            "iupacNames": {
                "type": "text",
                "fields": {"kw": {"type": "keyword"}},
            },
            "synonyms": {
                "type": "text",
                "fields": {"kw": {"type": "keyword"}},
            },
            "smiles": {"type": "keyword"},
            "inchi": {"type": "keyword"},
            "inchiKey": {"type": "keyword"},
            "formula": {"type": "keyword"},
            "charge": {"type": "byte"},
            "averagemass": {"type": "float"},
            "exactmass": {"type": "float"},

            "flags": {
                "type": "object",
                "dynamic": "strict",
                "properties": {
                    "hasLiterature":    {"type": "boolean"},
                    "hasReactions":     {"type": "boolean"},
                    "hasSpecies":       {"type": "boolean"},
                    "hasPathways":      {"type": "boolean"},
                    "hasNMR":           {"type": "boolean"},
                    "hasMS":            {"type": "boolean"},
                    "hasMolfile":       {"type": "boolean"},
                    "hasSmiles":        {"type": "boolean"},
                    "hasInchi":         {"type": "boolean"},
                    "hasSynonyms":      {"type": "boolean"},
                    "hasIupac":         {"type": "boolean"},
                    "hasCitations":     {"type": "boolean"},
                    "hasReactionsList": {"type": "boolean"},
                    "hasSpeciesHits":   {"type": "boolean"},
                    "hasKegg":          {"type": "boolean"},
                    "hasReactome":      {"type": "boolean"},
                    "hasWikiPathways":  {"type": "boolean"},
                    "hasSpectraListed": {"type": "boolean"},
                    "hasExactMass":     {"type": "boolean"},
                    "hasAverageMass":   {"type": "boolean"},
                    "hasCharge":        {"type": "boolean"},
                },
            },

            "counts": {
                "type": "object",
                "dynamic": "strict",
                "properties": {
                    "synonyms":             {"type": "integer"},
                    "iupac":                {"type": "integer"},
                    "citations":            {"type": "integer"},
                    "reactions":            {"type": "integer"},
                    "species_hits":         {"type": "integer"},
                    "species_total_assays": {"type": "integer"},
                    "kegg":                 {"type": "integer"},
                    "reactome":             {"type": "integer"},
                    "wikipathways":         {"type": "integer"},
                    "spectra":              {"type": "integer"},
                },
            },

            "species_hits": {
                "type": "nested",
                "properties": {
                    "species":   {"type": "keyword"},
                    "study_ids": {"type": "keyword"},
                    "assay_sum": {"type": "integer"},
                },
            },

            "spectra_count": {"type": "integer"},
        },
    },
}

def build_es_session(
    api_key: Optional[str],
    user: Optional[str],
    password: Optional[str],
) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json"})
    if api_key:
        s.headers.update({"Authorization": f"ApiKey {api_key}"})
    elif user and password:
        s.auth = HTTPBasicAuth(user, password)
    return s


def es_request(
    session: requests.Session,
    method: str,
    url: str,
    body: Optional[Dict[str, Any]] = None,
    ok_status=(200, 201),
) -> Dict[str, Any]:
    data = json.dumps(body) if body is not None else None
    r = session.request(method, url, data=data)
    if r.status_code not in ok_status:
        raise RuntimeError(f"{method} {url} failed: {r.status_code}\n{r.text}")
    if r.text.strip():
        try:
            return r.json()
        except Exception:
            return {"raw": r.text}
    return {}


def create_compounds_search_index(
    es_root: str,
    index_name: str,
    session: requests.Session,
    recreate: bool = False,
) -> None:
    """
    Create a compounds search index, only delete and recreate if relevant flag is set.
    :param es_root: root of Elasticsearch instance
    :param index_name: name of compound search index
    :param session: requets.Session object
    :param recreate: Whether to delete and recreate the index.
    :return: None
    """
    es = es_root.rstrip("/")
    if recreate:
        # Best-effort delete
        r = session.delete(f"{es}/{index_name}")
        if r.status_code not in (200, 404):
            raise RuntimeError(f"DELETE {index_name} failed: {r.status_code}\n{r.text}")
        print(f"Deleted index {index_name} (if it existed).")

    print(f"Creating index {index_name}")
    es_request(session, "PUT", f"{es}/{index_name}", COMPOUNDS_SEARCH_INDEX_BODY)
    print("Index created.")


def project_flags(flags: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract only the allowed flag fields (those defined in the ES mapping).
    Drops any unexpected fields such as 'MS'.
    """
    if not isinstance(flags, dict):
        return {}

    allowed_flag_keys = [
        "hasLiterature",
        "hasReactions",
        "hasSpecies",
        "hasPathways",
        "hasNMR",
        "hasMS",
        "hasMolfile",
        "hasSmiles",
        "hasInchi",
        "hasSynonyms",
        "hasIupac",
        "hasCitations",
        "hasReactionsList",
        "hasSpeciesHits",
        "hasKegg",
        "hasReactome",
        "hasWikiPathways",
        "hasSpectraListed",
        "hasExactMass",
        "hasAverageMass",
        "hasCharge",
    ]

    return {k: flags[k] for k in allowed_flag_keys if k in flags}


def project_counts(counts: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract only the count fields that exist in the ES mapping.
    """
    if not isinstance(counts, dict):
        return {}

    allowed_count_keys = [
        "synonyms",
        "iupac",
        "citations",
        "reactions",
        "species_hits",
        "species_total_assays",
        "kegg",
        "reactome",
        "wikipathways",
        "spectra",
    ]

    return {k: counts[k] for k in allowed_count_keys if k in counts}

def project_compound_for_es(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Project a MongoDB compound document into the flattened structure
    expected by the Elasticsearch compound search index.
    Strictly includes only mapped fields.
    """
    d: Dict[str, Any] = {}

    # Required
    d["id"] = doc["id"]

    # Basic string fields
    for key in ("name", "definition", "smiles", "inchi", "inchiKey", "formula"):
        if key in doc:
            d[key] = doc[key]

    # Arrays
    d["iupacNames"] = doc.get("iupacNames") or []
    d["synonyms"] = doc.get("synonyms") or []

    # Numerics
    for key in ("charge", "averagemass", "exactmass"):
        if key in doc:
            d[key] = doc[key]

    # Flags (cleaned)
    d["flags"] = project_flags(doc.get("flags", {}))

    # Counts (cleaned)
    d["counts"] = project_counts(doc.get("counts", {}))

    # species_hits (nested)
    species_hits = doc.get("species_hits") or []
    if isinstance(species_hits, list):
        d["species_hits"] = [
            {
                "species": sh.get("species"),
                "study_ids": sh.get("study_ids") or [],
                "assay_sum": sh.get("assay_sum"),
            }
            for sh in species_hits
            if isinstance(sh, dict)
        ]

    # spectra_count
    if "spectra_count" in doc:
        d["spectra_count"] = doc["spectra_count"]

    return d

def iter_compounds(
    mongo_uri: str,
    db_name: str = "compound_library",
    collection: str = "compounds",
    batch_size: int = 1000,
) -> Iterable[List[Dict[str, Any]]]:
    client = MongoWrapper()
    coll = client.collection(collection)

    batch: List[Dict[str, Any]] = []
    for doc in coll.find({}, no_cursor_timeout=True):
        batch.append(doc)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def bulk_index_batch(
    session: requests.Session,
    es_root: str,
    index_name: str,
    docs: List[Dict[str, Any]],
) -> Dict[str, Any]:
    es = es_root.rstrip("/")
    lines: List[str] = []

    for doc in docs:
        es_doc = project_compound_for_es(doc)
        compound_id = es_doc["id"]
        action = {"index": {"_index": index_name, "_id": compound_id}}
        lines.append(json.dumps(action))
        lines.append(json.dumps(es_doc, default=str))

    body = "\n".join(lines) + "\n"
    r = session.post(f"{es}/{index_name}/_bulk", data=body,
                     headers={"Content-Type": "application/x-ndjson"})
    if r.status_code != 200:
        raise RuntimeError(f"BULK index failed: {r.status_code}\n{r.text}")
    result = r.json()
    return result


def reindex_compounds(
    mongo_uri: str,
    es_root: str,
    index_name: str,
    session: requests.Session,
    batch_size: int = 1000,
    db_name: str = "compound_library",
    collection: str = "compounds",
) -> None:
    total = 0
    total_errors = 0

    for batch in iter_compounds(
        mongo_uri=mongo_uri,
        db_name=db_name,
        collection=collection,
        batch_size=batch_size,
    ):
        result = bulk_index_batch(session, es_root, index_name, batch)

        items = result.get("items", [])
        errors = [item for item in items if any(op.get("error") for op in item.values())]

        batch_count = len(items)
        total += batch_count
        total_errors += len(errors)

        print(f"Indexed batch of {batch_count}, total so far: {total}")

        if errors:
            print(f"  Errors in batch: {len(errors)} (showing up to 3)")
            for e in errors[:3]:
                op_name, op_data = next(iter(e.items()))
                err = op_data.get("error")
                doc_id = op_data.get("_id")
                print(
                    f"    op={op_name} id={doc_id} "
                    f"error_type={err.get('type')} reason={err.get('reason')}"
                )

    print(f"Reindex complete. Total docs: {total}, total errors: {total_errors}")


def main():

    parser = ArgParsers.mongo_to_elastic_parser()
    args = parser.parse_args()

    # 1) Build ES session
    session = build_es_session(
        api_key=args.api_key,
        user=args.user,
        password=args.password,
    )

    # 2) Create / recreate the index
    create_compounds_search_index(
        es_root=args.es,
        index_name=args.index,
        session=session,
        recreate=args.force,
    )

    # 3) Reindex from Mongo → ES
    reindex_compounds(
        mongo_uri=args.mongo_uri,
        es_root=args.es,
        index_name=args.index,
        session=session,
        batch_size=args.batch_size,
        db_name=args.mongo_db,
        collection=args.mongo_coll,
    )


if __name__ == "__main__":
    raise SystemExit(main())