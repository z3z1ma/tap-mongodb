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
    schema = {"properties": {"_id": {"type": "string"}}}

    @property
    def is_timestamp_replication_key(self) -> bool:
        return self.name in self.config["ts_based_replication"]

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
            "prefix",
            th.StringType,
            description="Optionally add a prefix for all streams, useful if ingesting \
                from multiple shards/clusters via independent tap-mongodb configs.",
            default="",
        ),
        th.Property(
            "ts_based_replication",
            th.ArrayType(th.StringType),
            description="Each item should correspond to a stream name. A stream mentioned here \
                indicates it uses timestamp-based replication. The default for a stream is \
                non-timestamp based since Mongo most often uses epochs. This is required since \
                the determination of timestamp based replication requires the key exist in the \
                jsonschema with a date-like type which is impossible when there is no explicit \
                schema prior to runtime. NOTE: Streams still require `metadata` mapping with an \
                explicit callout of `replication-key` and `replication-method`.",
            default=[],
        ),
        th.Property(
            "mongo",
            th.ObjectType(),
            description="These props are passed directly to pymongo MongoClient allowing the \
                tap user full flexibility not provided in any other Mongo tap.",
            required=True,
        ),
        th.Property("stream_maps", th.ObjectType()),
        th.Property("stream_map_settings", th.ObjectType()),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        streams = []
        client = MongoClient(**self.config["mongo"])
        for db_name in client.list_database_names():
            try:
                collections = client[db_name].list_collection_names()
            except:
                self.logger.debug(
                    "Skipping database %s, authenticated user does not have permission to access",
                    db_name,
                )
                continue
            for collection_name in collections:
                prefix = self.config.get("prefix", "") + db_name.replace("-", "_")
                streams.append(
                    CollectionStream(
                        tap=self,
                        database=client[db_name],
                        collection_name=collection_name,
                        prefix=prefix,
                    )
                )
        if not streams:
            self.logger.error(
                "No accessible collections found for supplied Mongo credentials"
            )
        return streams
