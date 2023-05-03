# Ingest-Timestream

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

This repository is intended to be used to convert netCDF files produced by pipelines in other `ingest-*` repositories
to a CSV format compatible with timestream.

The repository structure is similar to the other `ingest-*` repositories, but the pipelines here do not use `tsdat`.

## Development environment

Run the following commands to create and activate your conda environment:

```shell
conda env create
conda activate ingest-timestream
```

This project requires python 3.8+.

## Adding a new pipeline

Use a cookiecutter template to generate a new pipeline folder. From your top level
repository folder run:

```bash
make cookies
```

Follow the prompts that appear to generate a new ingestion pipeline. After completing all the
prompts cookiecutter will run and your new pipeline code will appear inside the
`pipelines/<module_name>` folder.

See the `README.md` file inside that folder for more information on how to configure, run,
test, and debug your pipeline.
