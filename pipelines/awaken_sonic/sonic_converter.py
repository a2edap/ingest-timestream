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
    df["measure_name"] = "data"

    df["time"] = pd.to_datetime(df[["year", "month", "day", "hour", "minute"]])
    df["time"] = df["time"].apply(lambda x: x.timestamp())

    df = df[variables + ["location", "measure_name"]]

    df.to_csv(target)

    return Path(target)
