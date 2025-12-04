import argparse

from pymongo import MongoClient, ASCENDING

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "compound_library"

def with_mongo_auth(uri: str, user: str, pwd: str) -> str:
    return uri.replace(f"mongodb://", f"mongodb://{user}:{pwd}@")

def run():
    p = argparse.ArgumentParser(description="Create mongodb indexes for the compound library")
    p.add_argument("--mongo-uri", required=False, help="The root url of the mongo instance", default="mongodb://localhost:27017")
    p.add_argument("--mongo-user", required=False, help="User to create db with", default="devuser")
    p.add_argument("--mongo-pass", required=False, help="Password for user", default="devpass")

    args = p.parse_args()
    full_uri = with_mongo_auth(args.mongo_uri, args.mongo_user, args.mongo_pass)
    full_uri = args.mongo_uri
    client = MongoClient(full_uri)
    db = client[DB_NAME]

    # Compounds indexes
    db.compounds.create_index([("id", ASCENDING)], unique=True, name="id_unique")
    db.compounds.create_index([("inchiKey", ASCENDING)], name="inchiKey_idx")
    db.compounds.create_index([("formula", ASCENDING)], name="formula_idx")
    db.compounds.create_index(
        [("flags.hasSpectraListed", ASCENDING)],
        name="hasSpectraListed_idx",
    )
    db.compounds.create_index(
        [("name", "text"), ("definition", "text"), ("synonyms", "text")],
        name="compounds_text_search",
    )

    # Spectra indexes
    db.spectra.create_index([("spectrumId", ASCENDING)], unique=True, name="spectrumId_unique")
    # If/when you add compound_id:
    # db.spectra.create_index([("compound_id", ASCENDING)], name="compound_id_idx")


if __name__ == "__main__":
    run()