from pathlib import Path
from typing import Any, List, Optional, Union

import xarray as xr


def from_netcdf_to_csv(
    filepath: Union[Path, str],
    variables: List[str],
    directory: Optional[Union[Path, str]] = None,
    **kwargs: Optional[Any],
) -> Path:
    from ncconvert.csv import to_csv

    target = filepath
    if directory is not None:
        target = Path(directory) / Path(filepath).name

    ds = xr.open_dataset(filepath)
    ds = ds[variables]

    output_filepath, _ = to_csv(ds, filepath=target, metadata=False)

    return output_filepath
