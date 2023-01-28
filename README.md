<h1 align="center">Tap-Mongodb</h1>

<p align="center">
<a href="https://github.com/z3z1ma/tap-mongodb/actions/"><img alt="Actions Status" src="https://github.com/z3z1ma/tap-mongodb/actions/workflows/ci_workflow.yml/badge.svg"></a>
<a href="https://github.com/z3z1ma/tap-mongodb/blob/main/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/License-MIT-yellow.svg"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
</p>


`tap-mongodb` is a Singer tap for MongoDB.

This tap differentiates itself from existing taps in a few ways. First, rather than expose a very specific set of configuration options for the underlying pymongo driver, we expose all possible arguments by accepting an object underneath the `mongo` key which pass all kwargs straight through to the driver. There are over 40 configurable kwargs available as seen [here](https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html#module-pymongo.mongo_client). This gives it more flexibility in contrast to a constrained interface. Secondly, this tap has two replication modes. 

- Mode 1 merely outputs data with an `additonalProperties: true` schema. This is ideal for loading to unstructured sources such as blob storage or VARIANT/JSON columns. At worst, it can be loaded into a string column.

- Mode 2 infer the schema from a configurable sample size of records. This allows the tap to work with strongly typed destinations. Under the hood we leverage genson. This is an attractive option. Particularly when we don't expect the documents to vary dramatically.

Lastly, I hope that this tap exemplifies how we can use as little code as possible with the existing plumbing in the SDK.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

The package on pypi is named `z3-tap-mongodb` but the executable it ships with is simply `tap-mongodb`. This allows me to release work without concerns of naming conflicts on the package index.

```bash
# Use pipx or pip
pipx install z3-tap-mongodb
# Verify it is installed
tap-mongodb --version
```

## Incremental Syncs

We support incremental syncs on a collection by collection basis. All this requires is the developer adding a `replication_key` to the catalog for a stream. After dumping the `tap-mongodb --config ... --discover > catalog.json`, you can modify the catalog and version control it for ongoing use. If using meltano, you can use the `metadata` key to update the catalog dynamically achieving the same affect. One caveat of our incrementality is that this relies on an alphanumerically sortable replication key. This accounts for a majority of use cases but there are some edge cases. Integer based replication keys (such as epochs) work fine obviously, ISO formatted dates work fine, but any other format may not work as expected.

## Settings

| Setting                 | Required | Default | Description |
|:------------------------|:--------:|:-------:|:------------|
| mongo                   | True     | None    | These props are passed directly to pymongo MongoClient allowing the tap user full flexibility not provided in any other Mongo tap since every kwarg can be tuned. |
| stream_prefix           | False    |         | Optionally add a prefix for all streams, useful if ingesting from multiple shards/clusters via independent tap-mongodb configs. |
| optional_replication_key| False    |       0 | This setting allows the tap to continue processing if a document is missing the replication key. Useful if a very small percentage of documents are missing the property. |
| database_includes       | False    | None    | A list of databases to include. If this list is empty, all databases will be included. |
| database_excludes       | False    | None    | A list of databases to exclude. If this list is empty, no databases will be excluded. |
| infer_schema            | False    |       0 | If true, the tap will infer the schema from documents sampled from the collection. |
| infer_schema_max_docs   | False    |    2000 | The maximum number of documents to sample when inferring the schema. This is only used when infer_schema is true. |
| stream_maps             | False    | None    |             |
| stream_map_settings     | False    | None    |             |
| stream_map_config       | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled      | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth    | False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities is available by running: `tap-mongodb --about`

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`

## Usage

You can easily run `tap-mongodb` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-mongodb --version
tap-mongodb --help
tap-mongodb --config CONFIG --discover > ./catalog.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_mongodb/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-mongodb` CLI interface directly using `poetry run`:

```bash
poetry run tap-mongodb --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-mongodb
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-mongodb --version
# OR run a test `elt` pipeline:
meltano elt tap-mongodb target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
