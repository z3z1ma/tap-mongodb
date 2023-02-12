"""MongoDB tap class."""
from __future__ import annotations

import collections
import os
from typing import Any, Generator, Iterable, MutableMapping

import orjson
import singer_sdk._singerlib as singer
import singer_sdk.helpers._flattening
from bson.objectid import ObjectId
from bson.timestamp import Timestamp
from pymongo.collection import Collection
from singer_sdk import Stream
from singer_sdk.helpers._state import increment_state
from singer_sdk.helpers._util import utc_now
from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams.core import (
    REPLICATION_INCREMENTAL,
    REPLICATION_LOG_BASED,
    TypeConformanceLevel,
)


def _flatten_record(
    record_node: MutableMapping[Any, Any],
    flattened_schema: dict | None = None,
    parent_key: list[str] | None = None,
    separator: str = "__",
    level: int = 0,
    max_level: int = 0,
) -> dict:
    if parent_key is None:
        parent_key = []
    items: list[tuple[str, Any]] = []
    for k, v in record_node.items():
        new_key = singer_sdk.helpers._flattening.flatten_key(k, parent_key, separator)
        if isinstance(v, collections.abc.MutableMapping) and level < max_level:
            items.extend(
                _flatten_record(
                    v,
                    flattened_schema,
                    parent_key + [k],
                    separator=separator,
                    level=level + 1,
                    max_level=max_level,
                ).items()
            )
        else:
            items.append(
                (
                    new_key,
                    # Override the default json encoder to use orjson
                    # and a string encoder for ObjectIds, etc.
                    orjson.dumps(
                        v, default=lambda o: str(o), option=orjson.OPT_OMIT_MICROSECONDS
                    ).decode("utf-8")
                    if singer_sdk.helpers._flattening._should_jsondump_value(k, v, flattened_schema)
                    else v,
                )
            )
    return dict(items)


# Monkey patch the singer lib to use orjson + bson json_util default
singer_sdk.helpers._flattening._flatten_record = _flatten_record


class CollectionStream(Stream):
    """Collection stream class.

    This stream is used to represent a collection in a database. It is a generic
    stream that can be used to represent any collection in a database."""

    # The output stream will always have _id as the primary key
    primary_keys = ["_id"]

    # Disable timestamp replication keys. One caveat is this relies on an
    # alphanumerically sortable replication key. Python __gt__ and __lt__ are
    # used to compare the replication key values. This works for most cases.
    is_timestamp_replication_key = False

    # No conformance level is set by default since this is a generic stream
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.NONE

    def __init__(
        self,
        tap: TapBaseClass,
        schema: str | os.PathLike | dict[str, Any] | singer.Schema | None = None,
        name: str | None = None,
        *,
        collection: Collection,
    ) -> None:
        """Initialize the stream."""
        super().__init__(tap=tap, schema=schema, name=name)
        self._collection = collection
        self._strategy = self.config.get("strategy", "raw")

    def _make_resume_token(oplog_doc: dict):
        """Make a resume token the hard way for Mongo <=3.6

        The idea here is to use change streams but there are nuances that don't fit a batch use
        case such as the fact it is a capped collection."""
        rt = b"\x82"
        rt += oplog_doc["ts"].time.to_bytes(4, byteorder="big") + oplog_doc["ts"].inc.to_bytes(
            4, byteorder="big"
        )
        rt += b"\x46\x64\x5f\x69\x64\x00\x64"
        rt += bytes.fromhex(str(oplog_doc["o"]["_id"]))
        rt += b"\x00\x5a\x10\x04"
        rt += oplog_doc["ui"].bytes
        rt += b"\x04"

        return {"_data": rt}

    def _make_start_op_time(self):
        """Make a Timestamp used to resume a change stream for Mongo >3.6

        The idea here is to use change streams but there are nuances that don't fit a batch use
        case such as the fact it is a capped collection."""
        first_record: ObjectId = list(self._collection.find(projection=[]).sort("_id", 1).limit(1))[
            0
        ]["_id"]
        return Timestamp(first_record.generation_time, first_record._inc)

    def get_records(self, context: dict | None) -> Iterable[dict]:
        bookmark = self.get_starting_replication_key_value(context)
        for record in self._collection.find(
            {self.replication_key: {"$gt": bookmark}} if bookmark else {}
        ):
            if self._strategy == "envelope":
                # Return the record wrapped in a document key
                yield {"_id": record["_id"], "document": record}
            else:
                # Return the record as is
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
        self, latest_record: dict[str, Any], *, context: dict | None = None
    ) -> None:
        """This override adds error handling for replication key incrementing.

        This is useful since a single bad document could otherwise break the stream."""
        state_dict = self.get_context_state(context)
        if latest_record:
            if self.replication_method in [REPLICATION_INCREMENTAL, REPLICATION_LOG_BASED]:
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
                except Exception as e:
                    # Handle the case where the replication key is not in the latest record
                    # since this is a valid case for Mongo
                    if self.config.get("optional_replication_key", False):
                        self.logger.warn("Failed to increment state. Ignoring...")
                        return
                    raise RuntimeError(
                        "Failed to increment state. Got record %s", latest_record
                    ) from e


class MockCollection:
    """Mock collection class.

    This class is used to mock a collection in the unit tests."""

    def __init__(self, name: str, schema: dict[str, Any]) -> None:
        self.name = name
        self.schema = schema

    def find(self, query: dict[str, Any]) -> list[dict[str, Any]]:
        """Mock find method."""
        return [{"_id": "1", "name": "test"}]

    def aggregate(self, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Mock aggregate method."""
        return [{"_id": "1", "name": "test"}]

    def distinct(self, key: str) -> list[str]:
        """Mock distinct method."""
        return ["test"]

    def count_documents(self, query: dict[str, Any]) -> int:
        """Mock count_documents method."""
        return 1

    def drop(self) -> None:
        """Mock drop method."""
        pass
