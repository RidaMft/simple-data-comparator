
from __future__ import annotations
import pandas as pd
from typing import Optional, Dict
from ..utils.normalize import apply_smart_string

def load_from_csv(
    path: str,
    sep: str = ',',
    encoding: Optional[str] = None,
    dtype: Optional[Dict] = None,
    na_values: Optional[list] = None,
    keep_default_na: bool = True,
) -> pd.DataFrame:
    if dtype is None:
        dtype = 'string'
    df = pd.read_csv(path, sep=sep, encoding=encoding, dtype=dtype,
                     na_values=na_values, keep_default_na=keep_default_na)
    # smart formatting
    df = apply_smart_string(df)
    return df
