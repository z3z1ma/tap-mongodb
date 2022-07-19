# tap-mongodb

`tap-mongodb` is a Singer tap for MongoDB.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

```bash
pipx install git+https://github.com/z3z1ma/tap-mongodb.git
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
