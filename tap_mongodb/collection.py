"""MongoDB tap class."""
from __future__ import annotations

import json
import os
from typing import Any, Generator, Iterable

import genson
import singer_sdk._singerlib as singer
from pymongo.collection import Collection
from bson.json_util import dumps
from bson.objectid import ObjectId
from bson.timestamp import Timestamp
from singer_sdk import Stream
from singer_sdk.helpers._state import increment_state
from singer_sdk.helpers._util import utc_now
from singer_sdk.plugin_base import PluginBase as TapBaseClass
from singer_sdk.streams.core import REPLICATION_INCREMENTAL, REPLICATION_LOG_BASED


class CollectionStream(Stream):
    """Collection stream class.

    This stream is used to represent a collection in a database. It is a generic
    stream that can be used to represent any collection in a database."""

    primary_keys = ["_id"]
    schema = {"properties": {"_id": {"type": "string"}}, "additionalProperties": True}
    # One caveat is this relies on an alphanumerically sortable replication key
    # so this accounts for a majority of use cases but there are some edge cases.
    # Integer based replication keys work fine, ISO formatted dates work fine, but
    # any other format may not work as expected.
    is_timestamp_replication_key = False

    def __init__(
        self,
        tap: TapBaseClass,
        schema: str | os.PathLike | dict[str, Any] | singer.Schema | None = None,
        name: str | None = None,
        *,
        collection: Collection,
    ) -> None:
        """Initialize the stream."""
        if tap.config.get("infer_schema", False) and not schema:
            # Infer the schema from the first 2,000 records (or the max_schema_inference)
            tap.logger.info("Inferring schema for collection '%s'", collection.name)
            builder = genson.SchemaBuilder(schema_uri=None)
            for record in collection.aggregate(
                [{"$sample": {"size": tap.config.get("infer_schema_max_docs", 2_000)}}]
            ):
                builder.add_object(json.loads(dumps(record)))
            schema = builder.to_schema()
            schema.pop("required", None)
            tap.logger.info("Inferred schema: %s", schema)
            # End of schema inference
        super().__init__(tap, schema=schema, name=name)
        self._collection = collection

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
        yield from self._collection.find(
            {self.replication_key: {"$gt": bookmark}} if bookmark else {}
        )

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
