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

    # TODO: Yet to find a way to dynamically set this in a schemaless
    # set-up where the metadata key will determine replication_key
    # on a per stream basis where it could be time based or non time based
    is_timestamp_replication_key = False

    # Schema is dynamically derived on each message
    # the target should wrap and load the data into an unstructured target
    schema = {"properties": {"*": {}}}

    def __init__(
        self, database: Database, collection_name: str, prefix: str, *args, **kwargs
    ) -> None:
        self.collection_name = collection_name
        self.name = f"{prefix}_{collection_name}" if prefix else collection_name
        super().__init__(*args, **kwargs)
        self.database = database

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        bookmark = self.get_starting_replication_key_value(context)
        for record in self.database[self.collection_name].find(
            {self.replication_key: {"$gt": bookmark}} if bookmark else {}
        ):
            self.schema["properties"] = {k: {} for k in record.keys()}
            replace_encrypted_bytes(record)
            yield record


class TapMongoDB(Tap):
    """MongoDB tap class."""

    name = "tap-mongodb"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "database",
            th.StringType,
            required=True,
        ),
        th.Property(
            "prefix_override",
            th.StringType
        ),
        # Note: All other props are directly passed through to MongoClient
        # additional_properties = {} <- TODO: MongoClient args jsonschema
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        mut_conf = {k: v for k, v in self.config.items()}
        prefix = mut_conf.pop("prefix_override", "")
        db = load_db_from_config(mut_conf)
        return [
            CollectionStream(
                tap=self,
                database=db,
                collection_name=collection,
                prefix=(prefix or db.name).replace("-", "_"),
            )
            for collection in db.list_collection_names()
        ]
