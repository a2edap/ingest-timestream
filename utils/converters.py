from pathlib import Path
from typing import Any, List, Optional, Union

import xarray as xr
import pandas as pd


def from_netcdf_to_csv(
    filepath: Union[Path, str],
    variables: List[str],
    location: str,
    directory: Optional[Union[Path, str]] = None,
    **kwargs: Optional[Any],
) -> Path:
    from ncconvert.csv import to_csv

    target = filepath
    if directory is not None:
        target = Path(directory) / Path(filepath).name

    ds = xr.open_dataset(filepath)

    ds["location"] = location

    new_variables = []
    i = 0
    for variable in variables:
        if variable in ds.coords:
            new_variables.append(variable)
            i += 1
        else:
            break
    new_variables.append("location")
    new_variables.extend(variables[i:])

    ds = ds[new_variables]

    output_filepath, _ = to_csv(
        ds,
        filepath=target,
        metadata=False,
        to_csv_kwargs=dict(date_format="%Y-%m-%d %H:%M:%S.%f"),
    )

    return output_filepath


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

    df = pd.read_csv(filepath)

    df["location"] = location

    df = df[variables + ["location"]]

    df.to_csv(
        filepath=target,
        date_format="%Y-%m-%d %H:%M:%S.%f",
    )

    return Path(target)
