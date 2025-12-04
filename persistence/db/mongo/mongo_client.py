from pymongo import MongoClient
from typing import Any, Dict, Optional


class MongoWrapper:
    def __init__(
        self,
        uri: str = "mongodb://localhost:27017",
        db_name: str = "compound_library",
    ):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def collection(self, name: str):
        """Return a collection handle."""
        return self.db[name]

    def insert(self, coll: str, doc: Dict[str, Any]):
        """Insert a single document."""
        return self.collection(coll).insert_one(doc)

    def upsert(self, coll: str, query: Dict[str, Any], doc: Dict[str, Any]):
        """Upsert a document based on a query."""
        return self.collection(coll).update_one(query, {"$set": doc}, upsert=True)

    def find_one(self, coll: str, query: Dict[str, Any]):
        """Find a single document."""
        return self.collection(coll).find_one(query)

    def count(self, coll: str, query: Optional[Dict[str, Any]] = None) -> int:
        """Count documents in a collection."""
        return self.collection(coll).count_documents(query or {})

    def drop_collection(self, coll: str):
        """Drop a collection."""
        return self.collection(coll).drop()

    def list_collections(self):
        """List collection names in the DB."""
        return self.db.list_collection_names()