
from __future__ import annotations
import pandas as pd
import numpy as np

def canonical_cell(value, float_tol: float = 1e-9):
    if pd.isna(value):
        return '<<NA>>'
    if isinstance(value, bool):
        return value
    if isinstance(value, (np.integer, int)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return round(float(value), max(0, int(abs(np.log10(float_tol)))) if float_tol > 0 else 9)
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    s = str(value).strip()
    if s == '':
        return ''
    has_leading_zero = len(s) > 1 and s[0] == '0' and s[1:].isdigit()
    if not has_leading_zero:
        try:
            if '.' in s or 'e' in s.lower():
                return round(float(s), max(0, int(abs(np.log10(float_tol)))) if float_tol > 0 else 9)
            if s.lstrip('-').isdigit():
                return int(s)
        except Exception:
            pass
    return s

def canonical_row(row: pd.Series, columns, float_tol: float = 1e-9, ignore_column_order: bool = True):
    cols = sorted(columns) if ignore_column_order else list(columns)
    return tuple(canonical_cell(row[c], float_tol=float_tol) for c in cols)

def normalize_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    for col in df2.columns:
        if df2[col].dtype == 'object' or pd.api.types.is_string_dtype(df2[col]):
            df2[col] = df2[col].astype(str).str.strip()
    return df2

# ---- Smart string formatting (no trailing .0) ----
def smart_str(value):
    """Return a display string without adding trailing .0 for integer-like numbers.
    - ints -> '123'
    - floats with .0 -> '123'
    - floats with decimals -> trimmed trailing zeros (e.g. 20.500000 -> '20.5')
    - strings preserved (including leading zeros)
    - NaN -> ''
    """
    if pd.isna(value):
        return ''
    # Preserve existing strings verbatim (trim spaces only)
    if isinstance(value, str):
        return value.strip()
    # Int-like
    if isinstance(value, (np.integer, int)):
        return str(int(value))
    # Float-like
    if isinstance(value, (np.floating, float)):
        f = float(value)
        if np.isfinite(f) and f.is_integer():
            return str(int(f))
        # format without scientific and trim trailing zeros
        s = f"{f:f}"
        s = s.rstrip('0').rstrip('.')
        return s
    # Timestamp
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    # Fallback
    return str(value)

def apply_smart_string(df: pd.DataFrame) -> pd.DataFrame:
    """Map all cells to smart_str and cast to Pandas string dtype."""
    if df.empty:
        return df
    df2 = df.applymap(smart_str)
    # Cast to string dtype for consistency (nullable)
    try:
        df2 = df2.astype('string')
    except Exception:
        pass
    # Trim again to be safe
    for c in df2.columns:
        if pd.api.types.is_string_dtype(df2[c]):
            df2[c] = df2[c].str.strip()
    return df2
