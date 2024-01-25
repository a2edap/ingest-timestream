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

    df["measure_name"] = "data"
    df["location"] = location

    df["time"] = pd.to_datetime(df[["year", "month", "day", "hour", "minute"]])
    df["time"] = (df["time"] - pd.Timestamp("1970-01-01")) // pd.Timedelta("1ms")
    df = df[variables + ["measure_name", "location"]]
    df.columns = df.columns.str.replace(" ", "_")

    df.to_csv(target, index=False)

    return Path(target)
