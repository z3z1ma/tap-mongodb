"""MongoDB tap class."""
import decimal
from typing import Iterable, List, Optional

import orjson
import singer.messages
from pymongo.database import Database
from pymongo.mongo_client import MongoClient
from singer_sdk import Stream, Tap
from singer_sdk import typing as th


def default(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    elif isinstance(obj, bytes):
        return "****"
    raise TypeError


singer.messages.format_message = lambda message: orjson.dumps(
    message.asdict(), default=default, option=orjson.OPT_OMIT_MICROSECONDS
).decode("utf-8")


class CollectionStream(Stream):
    primary_keys = ["_id"]

    # TODO: Yet to find a way to dynamically set this in a schemaless
    # set-up where the metadata key will determine replication_key
    # on a per stream basis where it could be time based or non time based
    is_timestamp_replication_key = False

    # Schema is dynamically derived on each message
    # the target should wrap and load the data into an unstructured target
    schema = {"properties": {"_id": {"type": "string"}}}

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
        th.Property("prefix_override", th.StringType),
        # Note: All other props are directly passed through to MongoClient
        # additional_properties = {} <- TODO: MongoClient args jsonschema
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        mut_conf = {k: v for k, v in self.config.items()}
        database = mut_conf.pop("database")
        prefix = mut_conf.pop("prefix_override", "")
        db = MongoClient(**mut_conf).get_database(database)
        return [
            CollectionStream(
                tap=self,
                database=db,
                collection_name=collection,
                prefix=(prefix or db.name).replace("-", "_"),
            )
            for collection in db.list_collection_names()
        ]
