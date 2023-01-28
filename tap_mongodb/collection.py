"""MongoDB tap class."""
from __future__ import annotations

import os
from typing import Any, Generator, Iterable

import singer_sdk._singerlib as singer
from pymongo.collection import Collection
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

    def __init__(
        self,
        tap: TapBaseClass,
        schema: str | os.PathLike | dict[str, Any] | singer.Schema | None = None,
        name: str | None = None,
        *,
        collection: Collection,
    ) -> None:
        """Initialize the stream."""
        _ = schema  # TODO: Infer schema from collection
        super().__init__(tap, name=name)
        self._collection = collection

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
                        self.logger.warn("Failed to increment state")
                    else:
                        raise RuntimeError("Failed to increment state") from e


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
