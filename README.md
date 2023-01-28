<h1 align="center">Tap-Mongodb</h1>

<p align="center">
<a href="https://github.com/z3z1ma/tap-mongodb/actions/"><img alt="Actions Status" src="https://github.com/z3z1ma/tap-mongodb/actions/workflows/ci_workflow.yml/badge.svg"></a>
<a href="https://github.com/z3z1ma/tap-mongodb/blob/main/LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/License-MIT-yellow.svg"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
</p>


`tap-mongodb` is a Singer tap for MongoDB.

This tap differentiates itself from existing taps in a few ways. First, rather than expose a very specific set of configuration options for the underlying pymongo driver, we expose all possible arguments by accepting an object underneath the `mongo` key which pass all kwargs straight through to the driver. There are over 40 configurable kwargs available as seen [here](https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html#module-pymongo.mongo_client). This gives it more flexibility in contrast to a constrained interface. Secondly, this tap aspires to have two replication modes. Mode 1 (implemented) merely outputs data with an `additonalProperties: true` schema. Mode 2 (pending) will buffer records and infer the schema before emitting. 

Mode 1 is ideal for unstructured targets such as JSONL, blob storage, or VARIANT/JSON type columns. At worst, it can be loaded into a string column. 

Mode 2 will permit the tap to work with strongly typed sources. Ideally these sources are able to handle schema evolution. Given that, I expect this to be an attractive option as well. Particularly when we don't expect the documents to vary dramatically.

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

## Configuration

### Accepted Config Options

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| prefix              | False    |    ""   | Optionally add a prefix for all streams, useful if ingesting from multiple shards/clusters via independent tap-mongodb configs. |
| ts_based_replication| False    |    []   | A list of stream names. A stream mentioned here indicates it uses timestamp-based replication. The default for a stream is ❗️ non-timestamp based since Mongo most often uses epochs. This overriding approach is required since the determination of timestamp based replication requires the key exist in the jsonschema with a date-like type which is impossible when there is no explicit schema prior to runtime. NOTE: Streams still require `metadata` mapping with an explicit callout of `replication-key` and `replication-method`. |
| mongo               | True     | None    | User-defined props. These props are passed directly to pymongo MongoClient allowing the tap user full flexibility not provided in any other Mongo tap. |
| resilient_replication_key | False    | False    | This setting allows the tap to continue processing if a document is missing the replication key. Useful if a very small percentage of documents are missing the prop. Subsequent executions with a bookmark will ensure they only ingested once. |
| stream_maps         | False    | None    |             |
| stream_map_settings | False    | None    |             |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-mongodb --about
```

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
