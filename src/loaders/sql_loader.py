
from __future__ import annotations
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional
from ..utils.normalize import apply_smart_string

def load_from_sql(
    conn_str: str,
    sql_query: str,
    chunksize: Optional[int] = None,
    as_string: bool = True,
) -> pd.DataFrame:
    engine = create_engine(conn_str)
    if chunksize:
        dfs = []
        with engine.connect() as conn:
            for chunk in pd.read_sql_query(text(sql_query), conn, chunksize=chunksize):
                dfs.append(chunk)
        df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    else:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(sql_query), conn)
    if as_string:
        df = apply_smart_string(df)
    else:
        # minimal trim for object columns
        for c in df.columns:
            if pd.api.types.is_string_dtype(df[c]):
                df[c] = df[c].str.strip()
    return df
