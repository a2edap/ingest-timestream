from pathlib import Path
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
        variable: List[str],
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
        reader: Converter,
        variables: List[str],
        bucket_name: str,
        storage_root: str,
    ) -> None:
        self.triggers = triggers
        self.reader = reader
        # and so on...
        ...

    def __repr_name__(self) -> str:
        return type(self).__name__

    @classmethod
    def from_config(cls, config_file: Path):
        # TODO: Instantiate the class from the config file
        # return cls(...)
        ...

    def run(self, inputs: List[str]) -> None:
        # TODO: Call converter method and upload output(s) to correct location in S3
        ...
