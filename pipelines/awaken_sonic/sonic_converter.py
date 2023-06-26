from pathlib import Path
from typing import Any, List, Optional, Union
import pandas as pd


def from_csv_to_csv(
    filepath: Union[Path, str],
    variables: List[str],
    location: str,
    directory: Optional[Union[Path, str]] = None,
    **kwargs: Optional[Any],
) -> Path:
    target = filepath
    if directory is not None:
        target = Path(directory) / Path(filepath).name

    df = pd.read_csv(filepath, skiprows=[1])

    df["location"] = location

    df["time"] = pd.to_datetime(df[["year", "month", "day", "hour", "minute"]])

    df = df[variables + ["location"]]

    df.to_csv(target,date_format="%Y-%m-%d %H:%M:%S.%f")

    return Path(target)
