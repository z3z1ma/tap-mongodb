"""MongoDB tap class."""
from __future__ import annotations

import os

import orjson
import singer_sdk._singerlib.messages
import singer_sdk.helpers._typing
from bson.json_util import default
from pymongo.mongo_client import MongoClient
from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_mongodb.collection import CollectionStream, MockCollection

BLANK = ""
"""A sentinel value to represent a blank value in the config."""

# Monkey patch the singer lib to use orjson
singer_sdk._singerlib.messages.format_message = lambda message: orjson.dumps(
    message.to_dict(), default=default, option=orjson.OPT_OMIT_MICROSECONDS
).decode("utf-8")


def noop(*args, **kwargs) -> None:
    """No-op function to silence the warning about unmapped properties."""
    pass


# Monkey patch the singer lib to silence the warning about unmapped properties
singer_sdk.helpers._typing._warn_unmapped_properties = noop


class TapMongoDB(Tap):
    """MongoDB tap class."""

    name = "tap-mongodb"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "mongo",
            th.ObjectType(),
            description=(
                "These props are passed directly to pymongo MongoClient allowing the "
                "tap user full flexibility not provided in any other Mongo tap since every kwarg "
                "can be tuned."
            ),
            required=True,
        ),
        th.Property(
            "stream_prefix",
            th.StringType,
            description=(
                "Optionally add a prefix for all streams, useful if ingesting from "
                "multiple shards/clusters via independent tap-mongodb configs."
            ),
            default=BLANK,
        ),
        th.Property(
            "optional_replication_key",
            th.BooleanType,
            description=(
                "This setting allows the tap to continue processing if a document is "
                "missing the replication key. Useful if a very small percentage of documents "
                "are missing the property."
            ),
            default=False,
        ),
        th.Property(
            "database_includes",
            th.ArrayType(th.StringType),
            description=(
                "A list of databases to include. If this list is empty, all databases "
                "will be included."
            ),
        ),
        th.Property(
            "database_excludes",
            th.ArrayType(th.StringType),
            description=(
                "A list of databases to exclude. If this list is empty, no databases "
                "will be excluded."
            ),
        ),
        th.Property(
            "infer_schema",
            th.BooleanType,
            description=(
                "If true, the tap will infer the schema from documents sampled from the collection."
            ),
            default=False,
        ),
        th.Property(
            "infer_schema_max_docs",
            th.IntegerType,
            description=(
                "The maximum number of documents to sample when inferring the schema. "
                "This is only used when infer_schema is true."
            ),
            default=2_000,
        ),
        th.Property("stream_maps", th.ObjectType()),
        th.Property("stream_map_settings", th.ObjectType()),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        if "TAP_MONGO_TEST_NO_DB" in os.environ:
            # This is a hack to allow the tap to be tested without a MongoDB instance
            return [
                CollectionStream(
                    self, name="test", collection=MockCollection(name="test", schema={})
                )
            ]
        streams: list[Stream] = []
        client = MongoClient(**self.config["mongo"])
        try:
            client.server_info()
        except Exception as exc:
            raise RuntimeError("Could not connect to MongoDB") from exc
        db_includes = self.config.get("database_includes", [])
        db_excludes = self.config.get("database_excludes", [])
        for db_name in client.list_database_names():
            if db_includes and db_name not in db_includes:
                continue
            if db_excludes and db_name in db_excludes:
                continue
            try:
                collections = client[db_name].list_collection_names()
            except Exception:
                # Skip databases that are not accessible by the authenticated user
                # This is a common case when using a shared cluster
                # https://docs.mongodb.com/manual/core/security-users/#database-user-privileges
                # TODO: vet the list of exceptions that can be raised here
                self.logger.debug(
                    "Skipping database %s, authenticated user does not have permission to access",
                    db_name,
                )
                continue
            for collection in collections:
                try:
                    client[db_name][collection].find_one()
                except Exception:
                    # Skip collections that are not accessible by the authenticated user
                    # This is a common case when using a shared cluster
                    # https://docs.mongodb.com/manual/core/security-users/#database-user-privileges
                    # TODO: vet the list of exceptions that can be raised here
                    self.logger.debug(
                        (
                            "Skipping collections %s, authenticated user does not have permission"
                            " to access"
                        ),
                        db_name,
                    )
                    continue
                stream_prefix = self.config.get("stream_prefix", BLANK)
                stream_prefix += db_name.replace("-", "_").replace(".", "_")
                streams.append(
                    CollectionStream(
                        tap=self,
                        name=f"{stream_prefix}_{collection}",
                        collection=client[db_name][collection],
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
