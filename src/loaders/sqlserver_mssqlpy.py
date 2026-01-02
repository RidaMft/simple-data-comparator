
from __future__ import annotations
import importlib
import pandas as pd
from typing import Optional
from ..utils.normalize import apply_smart_string

def _import_mssql_module():
    for name in ('mssql', 'mssql_python', 'mssqlpython'):
        try:
            return importlib.import_module(name)
        except ImportError:
            continue
    raise ImportError("Le package 'mssql-python' n'est pas disponible. Installez-le via 'pip install mssql-python'.")

def load_from_sqlserver_mssqlpy(
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
    mssql = _import_mssql_module()
    # Essayer différentes signatures de connect
    conn = None
    last_err = None
    for kwargs in (
        dict(server=host, database=database, user=user, password=password, port=port, timeout=timeout),
        dict(host=host, database=database, user=user, password=password, port=port, timeout=timeout),
        dict(server=host, db=database, uid=user, pwd=password, port=port, timeout=timeout),
    ):
        try:
            conn = mssql.connect(**{k: v for k, v in kwargs.items() if v is not None})
            break
        except Exception as e:
            last_err = e
            conn = None
    if conn is None:
        raise RuntimeError(f"Impossible d'établir la connexion mssql-python: {last_err}")
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
    finally:
        try:
            conn.close()
        except Exception:
            pass
    df = pd.DataFrame(rows, columns=cols)
    if as_string:
        df = apply_smart_string(df)
    return df
