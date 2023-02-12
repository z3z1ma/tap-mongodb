"""MongoDB tap class."""
from __future__ import annotations

import os

import orjson
import genson
import singer_sdk._singerlib.messages
import singer_sdk.helpers._typing
from bson.json_util import default  # noqa
from pymongo.mongo_client import MongoClient
from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk._singerlib.catalog import Catalog, CatalogEntry

from tap_mongodb.collection import CollectionStream, MockCollection

BLANK = ""
"""A sentinel value to represent a blank value in the config."""

# Monkey patch the singer lib to use orjson
singer_sdk._singerlib.messages.format_message = lambda message: orjson.dumps(
    message.to_dict(), default=lambda o: str(o), option=orjson.OPT_OMIT_MICROSECONDS
).decode("utf-8")


def noop(*args, **kwargs) -> None:
    """No-op function to silence the warning about unmapped properties."""
    pass


# Monkey patch the singer lib to silence the warning about unmapped properties
singer_sdk.helpers._typing._warn_unmapped_properties = noop


def recursively_drop_required(schema):
    """Recursively drop the required property from a schema."""
    schema.pop("required", None)
    if "properties" in schema:
        for prop in schema["properties"]:
            if schema["properties"][prop].get("type") == "object":
                recursively_drop_required(schema["properties"][prop])


class TapMongoDB(Tap):
    """MongoDB tap class."""

    name = "tap-mongodb"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "mongo",
            th.ObjectType(),
            description=(
                "These props are passed directly to pymongo MongoClient allowing the "
                "tap user full flexibility not provided in other Mongo taps since every kwarg "
                "can be tuned."
            ),
            required=True,
        ),
        th.Property(
            "stream_prefix",
            th.StringType,
            description=(
                "Optionally add a prefix for all streams, useful if ingesting from multiple"
                " shards/clusters via independent tap-mongodb configs. This is applied during"
                " catalog generation. Regenerate the catalog to apply a new stream prefix."
            ),
            default=BLANK,
        ),
        th.Property(
            "optional_replication_key",
            th.BooleanType,
            description=(
                "This setting allows the tap to continue processing if a document is"
                " missing the replication key. Useful if a very small percentage of documents"
                " are missing the property."
            ),
            default=False,
        ),
        th.Property(
            "database_includes",
            th.ArrayType(th.StringType),
            description=(
                "A list of databases to include. If this list is empty, all databases"
                " will be included."
            ),
        ),
        th.Property(
            "database_excludes",
            th.ArrayType(th.StringType),
            description=(
                "A list of databases to exclude. If this list is empty, no databases"
                " will be excluded."
            ),
        ),
        th.Property(
            "strategy",
            th.StringType,
            description=(
                "The strategy to use for schema resolution. Defaults to 'raw'. The 'raw' strategy"
                " uses a relaxed schema using additionalProperties: true to accept the document"
                " as-is leaving the target to respect it. Useful for blob or jsonl. The 'envelope'"
                " strategy will envelope the document under a key named `document`. The target"
                " should use a variant type for this key. The 'infer' strategy will infer the"
                " schema from the data based on a configurable number of documents."
            ),
            default="raw",
            allowed_values=["raw", "envelope", "infer"],
        ),
        th.Property(
            "infer_schema_max_docs",
            th.IntegerType,
            description=(
                "The maximum number of documents to sample when inferring the schema."
                " This is only used when infer_schema is true."
            ),
            default=2_000,
        ),
        th.Property("stream_maps", th.ObjectType()),
        th.Property("stream_map_config", th.ObjectType()),
        th.Property("batch_config", th.ObjectType()),
    ).to_dict()

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        # Use cached catalog if available
        if hasattr(self, "_catalog_dict") and self._catalog_dict:
            return self._catalog_dict
        # Defer to passed in catalog if available
        if self.input_catalog:
            return self.input_catalog.to_dict()
        # If no catalog is provided, discover streams
        catalog = Catalog()
        client = MongoClient(**self.config["mongo"])
        try:
            client.server_info()
        except Exception as exc:
            raise RuntimeError("Could not connect to MongoDB to generate catalog") from exc
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
                # TODO: vet the list of exceptions that can be raised here to be more explicit
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
                    # TODO: vet the list of exceptions that can be raised here to be more explicit
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
                stream_name = f"{stream_prefix}_{collection}"
                entry = CatalogEntry.from_dict({"tap_stream_id": stream_name})
                entry.stream = stream_name
                strategy: str | None = self.config.get("strategy")
                if strategy == "infer":
                    builder = genson.SchemaBuilder(schema_uri=None)
                    for record in client[db_name][collection].aggregate(
                        [{"$sample": {"size": self.config.get("infer_schema_max_docs", 2_000)}}]
                    ):
                        builder.add_object(
                            orjson.loads(
                                orjson.dumps(
                                    record,
                                    default=lambda o: str(o),
                                    option=orjson.OPT_OMIT_MICROSECONDS,
                                ).decode("utf-8")
                            )
                        )
                    schema = builder.to_schema()
                    recursively_drop_required(schema)
                    if not schema:
                        # If the schema is empty, skip the stream
                        # this errs on the side of strictness
                        continue
                    self.logger.info("Inferred schema: %s", schema)
                elif strategy == "envelope":
                    schema = {
                        "type": "object",
                        "properties": {
                            "_id": {
                                "type": ["string", "null"],
                                "description": "The document's _id",
                            },
                            "document": {
                                "type": "object",
                                "additionalProperties": True,
                                "description": "The document from the collection",
                            },
                        },
                    }
                elif strategy == "raw":
                    schema = {
                        "type": "object",
                        "additionalProperties": True,
                        "description": "The document from the collection",
                        "properties": {
                            "_id": {
                                "type": ["string", "null"],
                                "description": "The document's _id",
                            },
                        },
                    }
                else:
                    raise RuntimeError(f"Unknown strategy {strategy}")
                entry.schema = entry.schema.from_dict(schema)
                entry.metadata.get_standard_metadata(schema=schema, key_properties=["_id"])
                entry.database = db_name
                entry.table = collection
                catalog.add_stream(entry)
        self._catalog_dict = catalog.to_dict()
        return self._catalog_dict

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        if "TAP_MONGO_TEST_NO_DB" in os.environ:
            # This is a hack to allow the tap to be tested without a MongoDB instance
            return [
                CollectionStream(
                    self, name="test", collection=MockCollection(name="test", schema={})
                )
            ]
        client = MongoClient(**self.config["mongo"])
        try:
            client.server_info()
        except Exception as e:
            raise RuntimeError("Could not connect to MongoDB") from e
        db_includes = self.config.get("database_includes", [])
        db_excludes = self.config.get("database_excludes", [])
        for entry in self.catalog_dict["streams"]:
            if entry["database_name"] in db_excludes:
                continue
            if db_includes and entry["database_name"] not in db_includes:
                continue
            stream = CollectionStream(
                tap=self,
                name=entry.get("stream", entry["tap_stream_id"]),
                schema=entry["schema"],
                collection=client[entry["database_name"]][entry["table_name"]],
            )
            stream.apply_catalog(Catalog.from_dict(entry))
            yield stream
