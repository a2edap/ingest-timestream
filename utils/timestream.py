from __future__ import annotations

import datetime
from functools import lru_cache

import os
import re
import tempfile
from pathlib import Path
import time
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Pattern,
    Protocol,
    Tuple,
    Union,
    Callable,
    Mapping,
    Match,
)

import yaml
import boto3


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


_SQUARE_BRACKET_REGEX = r"\[(.*?)\]"
_CURLY_BRACKET_REGEX = r"\{(.*?)\}"


class Template:
    """Python f-string implementation with lazy and optional variable substitutions.

    The template string is expected to be formatted in the same way as python f-strings,
    with variables that should be substituted wrapped in curly braces `{}`.
    Additionally, square brackets may be used around curly brackets and other text to
    mark that substitution as optional -- i.e. if the variable cannot be found then the
    text wrapped in the square brackets will be removed.


    Examples:

        ```python
        mapping = dict(a="x", b="y", c="z")

        Template("{a}.{b}{c}w").substitute(mapping) # -> "x.yzw"
        Template("{a}.{b}[.{c}]").substitute(mapping) # -> "x.y.z"
        Template("{a}.{b}.{d}").substitute(mapping)  # raises ValueError
        Template("{a}.{b}[.{d}]").substitute(mapping) # -> "x.y"
        Template("{a}.{b}.{d}").substitute(mapping, True) # -> "x.y.{d}"
        ```

    Args:
        template (str): The template string. Variables to substitute should be wrapped
            by curly braces `{}`.
    """

    def __init__(self, template: str) -> None:
        if not self._is_balanced(template):
            raise ValueError(f"Unbalanced brackets in template string: '{template}'")
        self.template = template

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.template!r})"

    def __str__(self) -> str:
        return self.template

    @classmethod
    def _is_balanced(cls, template: str):
        queue: list[str] = []
        for char in template:
            if char in "{[":
                queue.append("}" if char == "{" else "]")
            elif char in "}]":
                if not queue or char != queue.pop():
                    return False
        return len(queue) == 0

    def substitute(
        self,
        mapping: Mapping[str, str | Callable[[], str] | None] | None = None,
        allow_missing: bool = False,
        **kwds: str | Callable[[], str] | None,
    ) -> str:
        """Substitutes variables in a template string.

        The template string is expected to be formatted in the same way as python
        f-strings, with variables that should be substituted wrapped in curly braces
        `{}`. Additionally, square brackets may be used around curly brackets and other
        text to mark that substitution as optional -- i.e. if the variable cannot be
        found then the text wrapped in the square brackets will be removed.

        Examples:

            ```python
            mapping = dict(a="x", b="y", c="z")

            substitute("{a}.{b}{c}w", mapping) == "x.yzw"  # True
            substitute("{a}.{b}[.{c}]", mapping) == "x.y.z"  # True
            substitute("{a}.{b}[.{d}]", mapping) == "x.y"  # True
            substitute("{a}.{b}.{d}", mapping, True) == "x.y.{d}"  # True
            substitute("{a}.{b}.{d}", mapping, False)  # raises ValueError
            ```

        Args:
            mapping (Mapping[str, str | Callable[[], str] | None] | None): A key-value
                pair of variable name to the value to replace it with. If the value is a
                string it is dropped-in directly. If it is a no-argument callable the
                return value of the callable is used. If it is None, then it is treated
                as missing and the action taken depends on the `allow_missing`
                parameter.
            allow_missing (bool, optional): Allow variables outside of square brackets
                to be missing, in which case they are left as-is, including the curly
                brackets. This is intended to allow users to perform some variable
                substitutions before all variables in the mapping are known. Defaults to
                False.
            **kwds (str | Callable[[], str] | None): Optional extras to be merged into
                the mapping dict. If a keyword passed here has the same name as a key in
                the mapping dict, the value here would be used instead.

        Raises:
            ValueError: If the substitutions cannot be made due to missing variables.

        Returns:
            str: The template string with the appropriate substitutions made.
        """
        # return _substitute(self.template, mapping, allow_missing, **kwds)
        if mapping is None:
            mapping = {}
        mapping = {**mapping, **kwds}

        def _sub_curly(match: Match[str]) -> str:
            # group(1) returns string without {}, group(0) returns with {}
            # result is we only do replacements that we can actually do.
            res = mapping.get(match.group(1))  # type: ignore
            if callable(res):
                res = res()
            if allow_missing and res is None:
                res = match.group(0)
            elif res is None:
                raise ValueError(
                    f"Substitution cannot be made for key '{match.group(1)}'"
                )
            return res

        def _sub_square(match: Match[str]) -> str:
            # make curly substitutions inside of square brackets or remove the whole
            # thing if substitutions cannot be made.
            try:
                resolved = re.sub(_CURLY_BRACKET_REGEX, _sub_curly, match.group(1))
                return resolved if resolved != match.group(1) else ""
            except ValueError:
                return ""

        squared = re.sub(_SQUARE_BRACKET_REGEX, _sub_square, self.template)
        resolved = re.sub(_CURLY_BRACKET_REGEX, _sub_curly, squared)

        return resolved


class Converter(Protocol):
    def __call__(
        self,
        filepath: Union[Path, str],
        variables: List[str],
        location: str,
        directory: Optional[Union[Path, str]] = None,
        **kwargs: Optional[Any],
    ) -> Union[Tuple[Path, ...], Path]: ...


class TimestreamPipeline:
    def __init__(
        self,
        triggers: List[Pattern[str]],
        converter: Converter,
        variables: List[str],
        bucket_name: str,
        storage_root: Template,
    ) -> None:
        self.triggers = triggers
        self.converter = converter
        self.variables = variables
        self.bucket_name = bucket_name
        self.storage_root = storage_root

        self.bucket_region = "us-west-2"

    def __repr_name__(self) -> str:
        return type(self).__name__

    @property
    def _session(self):
        return self._get_session(region="us-west-2", timehash=self._get_timehash())

    @staticmethod
    def _get_timehash(seconds: int = 3600) -> int:
        return round(time.time() / seconds)

    @staticmethod
    @lru_cache()
    def _get_session(region: str, timehash: int = 0):
        """------------------------------------------------------------------------------------
        Creates a boto3 Session or returns an active one.

        Borrowed approximately from https://stackoverflow.com/a/55900800/15641512.

        Args:
            region (str): The session region.
            timehash (int, optional): A time hash used to cache repeated calls to this
                function. This should be generated using tsdat.io.storage.get_timehash().

        Returns:
            boto3.session.Session: An active boto3 Session object.

        ------------------------------------------------------------------------------------
        """
        del timehash
        return boto3.session.Session(region_name=region)

    @property
    def _bucket(self):
        s3 = self._session.resource("s3", region_name=self.bucket_region)  # type: ignore
        return s3.Bucket(name=self.bucket_name)

    @classmethod
    def from_config(cls, config_file: Path):
        config = read_yaml(config_file)

        triggers = config.get("triggers", [])
        inputs = config.get("inputs", {})
        outputs = config.get("outputs", {})
        converter = inputs.get("converter", "")
        variables = inputs.get("variables", [])
        bucket_name = outputs.get("bucket_name", os.getenv("TSDAT_S3_BUCKET_NAME", ""))
        storage_root = Template(outputs.get("storage_root", ""))

        converter = import_string(converter)

        return cls(
            triggers=triggers,
            converter=converter,
            variables=variables,
            bucket_name=bucket_name,
            storage_root=storage_root,
        )

    def run(self, inputs: List[str]) -> None:
        date = datetime.date.today()
        time = datetime.datetime.now()
        with tempfile.TemporaryDirectory() as tmp_dir:
            for input_filepath in inputs:
                print("input_filepath",input_filepath)
                print("dataset", Path(input_filepath).parts[1])
                storage_root = Path(tmp_dir) / Path(
                    self.storage_root.substitute(
                        date=date.strftime("%Y%m%d"),
                        time=time.strftime("%H0000"),
                        dataset=Path(input_filepath).parts[1],
                    )
                )
                storage_root.mkdir(parents=True, exist_ok=True)
                location = Path(input_filepath).name.split(".")[0]
                self.converter(
                    filepath=input_filepath,
                    variables=self.variables,
                    location=location,
                    directory=storage_root,
                )

            for filepath in Path(tmp_dir).glob("**/*"):
                if filepath.is_dir():
                    continue
                s3_filepath = filepath.relative_to(tmp_dir).as_posix()
                self._bucket.upload_file(Filename=filepath.as_posix(), Key=s3_filepath)
                # logger.info(
                #     "Saved %s data file to s3://%s/%s",
                #     datastream,
                #     self.parameters.bucket,
                #     s3_filepath,
                # )
