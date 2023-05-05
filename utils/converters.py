from pathlib import Path
from typing import Any, List, Optional, Union


def from_netcdf(
    filepath: Union[Path, str],
    variable: List[str],
    **kwargs: Optional[Any],
) -> Path:
    from ncconvert import to_csv

    # TODO: open file and wrap to_csv call
    # to_csv()

    return Path()
