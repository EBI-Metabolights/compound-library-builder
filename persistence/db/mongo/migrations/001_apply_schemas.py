import argparse
import json
from pathlib import Path

from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "compound_library"

BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMAS_DIR = BASE_DIR / "schemas"


def load_schema(name: str) -> dict:
    path = SCHEMAS_DIR / f"{name}_schema.json"
    return json.loads(path.read_text())


def apply_schema(db, coll_name: str, schema: dict) -> None:

    validator = schema

    if coll_name in db.list_collection_names():
        # Update existing collection
        db.command(
            "collMod",
            coll_name,
            validator=validator,
            validationLevel="moderate",
        )
    else:
        # Create new collection
        db.create_collection(
            coll_name,
            validator=validator,
            validationLevel="moderate",
        )

def with_mongo_auth(uri: str, user: str, pwd: str) -> str:
    return f'{uri.replace(f"mongodb://", f"mongodb://{user}:{pwd}@")}?authSource=admin'

def run():
    p = argparse.ArgumentParser(description="Create mongodb indexes for the compound library")
    p.add_argument("--mongo-uri", required=False, help="The root url of the mongo instance", default="mongodb://localhost:27017/")
    p.add_argument("--mongo-user", required=False, help="User to create db with", default="devuser")
    p.add_argument("--mongo-pass", required=False, help="Password for user", default="devpass")
    args = p.parse_args()

    full_uri = with_mongo_auth(args.mongo_uri, args.mongo_user, args.mongo_pass)
    full_uri = args.mongo_uri
    client = MongoClient(full_uri)
    db = client[DB_NAME]

    compounds_schema = load_schema("compound")
    spectra_schema = load_schema("spectra")

    apply_schema(db, "compounds", compounds_schema)
    apply_schema(db, "spectra", spectra_schema)


if __name__ == "__main__":
    run()