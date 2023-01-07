"""MongoDB tap class."""
import datetime
import decimal
from typing import Any, Dict, Generator, Iterable, List, Optional

import orjson
import singer_sdk._singerlib as singer
import singer_sdk._singerlib.messages
import singer_sdk.helpers._typing
from bson.objectid import ObjectId
from pymongo.database import Database
from pymongo.mongo_client import MongoClient
from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk.helpers._state import increment_state
from singer_sdk.helpers._util import utc_now
from singer_sdk.streams.core import REPLICATION_INCREMENTAL, REPLICATION_LOG_BASED


def default(obj):
    if isinstance(obj, (decimal.Decimal, ObjectId)):
        return str(obj)
    elif isinstance(obj, bytes):
        return "****"
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError


singer_sdk._singerlib.messages.format_message = lambda message: orjson.dumps(
    message.to_dict(), default=default, option=orjson.OPT_OMIT_MICROSECONDS
).decode("utf-8")


def noop(*args, **kwargs) -> None:
    pass


singer_sdk.helpers._typing._warn_unmapped_properties = noop


class CollectionStream(Stream):
    primary_keys = ["_id"]
    schema = {"properties": {"_id": {"type": "string"}}, "additionalProperties": True}

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
            yield record

    def _generate_record_messages(
        self,
        record: dict,
    ) -> Generator[singer.RecordMessage, None, None]:
        for stream_map in self.stream_maps:
            mapped_record = stream_map.transform(record)
            if mapped_record is not None:
                record_message = singer.RecordMessage(
                    stream=stream_map.stream_alias,
                    record=mapped_record,
                    version=None,
                    time_extracted=utc_now(),
                )
                yield record_message

    def _increment_stream_state(
        self, latest_record: Dict[str, Any], *, context: Optional[dict] = None
    ) -> None:
        state_dict = self.get_context_state(context)
        if latest_record:
            if self.replication_method in [
                REPLICATION_INCREMENTAL,
                REPLICATION_LOG_BASED,
            ]:
                if not self.replication_key:
                    raise ValueError(
                        f"Could not detect replication key for '{self.name}' stream"
                        f"(replication method={self.replication_method})"
                    )
                treat_as_sorted = self.is_sorted
                if not treat_as_sorted and self.state_partitioning_keys is not None:
                    # Streams with custom state partitioning are not resumable.
                    treat_as_sorted = False
                try:
                    increment_state(
                        state_dict,
                        replication_key=self.replication_key,
                        latest_record=latest_record,
                        is_sorted=treat_as_sorted,
                        check_sorted=self.check_sorted,
                    )
                except Exception as exc:
                    if self.config.get("resilient_replication_key", False):
                        self.logger.warn("Failed to increment state")
                    else:
                        raise RuntimeError("Failed to increment state") from exc


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
                tap user full flexibility not provided in any other Mongo tap since every kwarg can be tuned.",
            required=True,
        ),
        th.Property(
            "resilient_replication_key",
            th.BooleanType,
            description="This setting allows the tap to continue processing if a document is \
                missing the replication key. Useful if a very small percentage of documents \
                are missing the prop. Subsequent executions with a bookmark will ensure they \
                only ingested once.",
            default=False,
        ),
        th.Property("stream_maps", th.ObjectType()),
        th.Property("stream_map_settings", th.ObjectType()),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        streams: List[Stream] = []
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
                _name = f"{prefix}_{collection_name}" if prefix else collection_name
                # Skip collections that are not in the catalog if the catalog is provided
                # SDK plumbing does not handle a purposefully sparse catalog when the
                # discover_streams() method returns additional streams
                if self.input_catalog is not None and self.input_catalog.get_stream(_name) is None:
                    continue
                streams.append(
                    CollectionStream(
                        tap=self,
                        database=client[db_name],
                        collection_name=collection_name,
                        prefix=prefix,
                    )
                )
        if not streams:
            raise RuntimeError(
                "No accessible collections found for supplied Mongo credentials. "
                "Please check your credentials and try again. If you are using "
                "a catalog, please ensure that the catalog contains at least one "
                "collection that the authenticated user has access to."
            )
        return streams
