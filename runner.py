import logging
from pathlib import Path
from typing import List

import typer

from utils.registry import PipelineRegistry

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False)


@app.command()
def run_pipeline(
    filepaths: List[Path] = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path(s) to the file(s) to process",
    ),
    clump: bool = typer.Option(
        False,
        help="A flag indicating if the dispatcher should use a single pipeline to "
        "process the input keys. This typically results in one output data file being "
        "produced. Omit this option to run files independently and generally produce one"
        " output data file for each input file.",
    ),
    multidispatch: bool = typer.Option(
        False,
        help="A flag indicating if the dispatcher is allowed to use multiple pipelines "
        "to process each input key. If True, any pipeline whose regex pattern matches an "
        "input key will be used to process the input key.",
    ),
    verbose: bool = typer.Option(False, help="Turn logging level up to DEBUG."),
):
    """Main entry point to the ingest controller. This script takes a path to an input
    file, automatically determines which ingest(s) to use, and runs those ingests on the
    provided input data."""

    # If in verbose mode, then turn up logging to DEBUG
    logging_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=logging_level)

    # Downstream code expects a list of strings
    files = [str(file) for file in filepaths]
    logger.debug(f"Found input files: {files}")

    # Run the pipeline on the input files
    dispatcher = PipelineRegistry()
    dispatcher.dispatch(files, clump=clump, multidispatch=multidispatch)


if __name__ == "__main__":
    app()
