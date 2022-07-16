"""MongoDB tap class."""
from typing import Iterable, List, Optional, Tuple

from pymongo.database import Database
from pymongo.mongo_client import MongoClient
from singer_sdk import Stream, Tap
from singer_sdk import typing as th


def load_db_from_config(mut_config: dict) -> Database:
    database = mut_config.pop("database")
    return MongoClient(**mut_config).get_database(database)


def replace_encrypted_bytes(record: Tuple[dict, List]):
    if isinstance(record, dict):
        for k in record:
            if isinstance(record[k], bytes):
                record[k] = "****"
            else:
                replace_encrypted_bytes(record[k])
    elif isinstance(record, List):
        for v in record:
            replace_encrypted_bytes(v)
    return record


class CollectionStream(Stream):
    # Sent in schema message but not needed
    primary_keys = ["_id"]

    # Schema is ignored in our target since the entirety of the contents should be
    # understood to be an object
    schema = {"properties": {"*": {}}}

    def __init__(
        self, database: Database, collection_name: str, *args, **kwargs
    ) -> None:
        self.name = collection_name
        super().__init__(*args, **kwargs)
        self.database = database

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        for record in self.database[self.name].find({}):
            self.schema["properties"] = {k: {} for k in record.keys()}
            replace_encrypted_bytes(record)
            yield record


class TapMongoDB(Tap):
    """MongoDB tap class."""

    name = "tap-mongodb"
    config_jsonschema = th.PropertiesList().to_dict()

    def discover_streams(self) -> List[Stream]:
        mut_conf = {k: v for k, v in self.config.items()}
        db = load_db_from_config(mut_conf)
        return [
            CollectionStream(tap=self, database=db, collection_name=collection)
            for collection in db.list_collection_names()
        ]
