from pathlib import Path
import tempfile
from typing import Any, Dict, List, Optional, Pattern, Protocol, Tuple, Union

import yaml


def read_yaml(filepath: Path) -> Dict[Any, Any]:
    """Returns a dictionary representation of a yaml file."""
    return list(yaml.safe_load_all(filepath.read_text(encoding="UTF-8")))[0]


def import_string(dotted_path: str) -> Any:
    """
    Stolen from pydantic. Import a dotted module path and return the attribute/class
    designated by the last name in the path. Raise ImportError if the import fails.
    """
    from importlib import import_module

    try:
        module_path, class_name = dotted_path.strip(" ").rsplit(".", 1)
    except ValueError as e:
        raise ImportError(f'"{dotted_path}" doesn\'t look like a module path') from e

    module = import_module(module_path)
    try:
        return getattr(module, class_name)
    except AttributeError as e:
        raise ImportError(
            f'Module "{module_path}" does not define a "{class_name}" attribute'
        ) from e


class Converter(Protocol):
    def __call__(
        self,
        filepath: Union[Path, str],
        variables: List[str],
        directory: Optional[Union[Path, str]] = None,
        **kwargs: Optional[Any],
    ) -> Union[Tuple[Path, ...], Path]:
        ...


# TODO: Implement all of the following methods.
# This class should retain all of the parameters defined in the pipeline config
# and make them easily accessible for future use. We also must implement the run
# method such that
class TimestreamPipeline:
    def __init__(
        self,
        triggers: List[Pattern[str]],
        converter: Converter,
        variables: List[str],
        bucket_name: str,
        storage_root: str,
    ) -> None:
        self.triggers = triggers
        self.converter = converter
        self.variables = variables
        self.bucket_name = bucket_name
        self.storage_root = storage_root

    def __repr_name__(self) -> str:
        return type(self).__name__

    @classmethod
    def from_config(cls, config_file: Path):
        # TODO: Instantiate the class from the config file
        # return cls(...)
        config = read_yaml(config_file)

        triggers = config.get("triggers", [])
        inputs = config.get("inputs", {})
        outputs = config.get("outputs", {})
        converter = inputs.get("converter", "")
        variables = inputs.get("variables", [])
        bucket_name = outputs.get("bucket_name", "")
        storage_root = outputs.get("storage_root", "")

        converter = import_string(converter)

        return cls(triggers, converter, variables, bucket_name, storage_root)

    def run(self, inputs: List[str]) -> None:
        # TODO: Call converter method and upload output(s) to correct location in S3

        with tempfile.TemporaryDirectory() as tmp_dir:
            # TODO: Use template to calculate standard_fpath
            # TODO: Calculate substitutions
            # TODO: date and time should be current date and time.
            for input in inputs:
                tmp_filepath = Path(tmp_dir) / standard_fpath
                tmp_filepath.parent.mkdir(parents=True, exist_ok=True)
                self.converter(input, self.variables, directory=self.storage_root)

            for filepath in Path(tmp_dir).glob("**/*"):
                if filepath.is_dir():
                    continue
                s3_filepath = filepath.relative_to(tmp_dir).as_posix()
                # TODO: add self._bucket (boto3)
                self._bucket.upload_file(Filename=filepath.as_posix(), Key=s3_filepath)
                # TODO:
                # logger.info(
                #     "Saved %s data file to s3://%s/%s",
                #     datastream,
                #     self.parameters.bucket,
                #     s3_filepath,
                # )

        ...
