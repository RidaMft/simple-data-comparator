
from __future__ import annotations
import pandas as pd
from typing import Optional
from ..utils.normalize import apply_smart_string

try:
    import pytds as tds
except ImportError:
    import tds  # fallback module name

def load_from_sqlserver_pytds(
    host: str,
    database: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    port: int = 1433,
    query: str = '',
    as_string: bool = True,
    timeout: Optional[int] = None,
) -> pd.DataFrame:
    if not query:
        raise ValueError('Requête SQL vide')
    conn = tds.connect(server=host, database=database, user=user, password=password, port=port, timeout=timeout, login_timeout=timeout, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
    finally:
        conn.close()
    df = pd.DataFrame(rows, columns=cols)
    if as_string:
        df = apply_smart_string(df)
    else:
        for c in df.columns:
            if pd.api.types.is_string_dtype(df[c]):
                df[c] = df[c].str.strip()
    return df
