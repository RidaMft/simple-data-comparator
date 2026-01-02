
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import List, Optional, Dict, Any
from collections import Counter
from ..utils.normalize import canonical_row, normalize_object_columns

def compare_dataframes(
    left: pd.DataFrame,
    right: pd.DataFrame,
    keys: Optional[List[str]] = None,
    ignore_column_order: bool = True,
    float_tol: float = 1e-9,
    sample_size: int = 50,
) -> Dict[str, Any]:
    res: Dict[str, Any] = {}
    res['left_count'] = int(len(left))
    res['right_count'] = int(len(right))
    res['row_count_equal'] = res['left_count'] == res['right_count']
    res['left_columns'] = list(left.columns)
    res['right_columns'] = list(right.columns)
    res['columns_equal'] = set(res['left_columns']) == set(res['right_columns'])
    res['data_equal'] = False
    res['differences'] = {}

    left_n = normalize_object_columns(left)
    right_n = normalize_object_columns(right)

    if not keys:
        if not res['columns_equal']:
            res['differences']['missing_in_left'] = list(set(res['right_columns']) - set(res['left_columns']))
            res['differences']['missing_in_right'] = list(set(res['left_columns']) - set(res['right_columns']))
            return res
        cols = sorted(left_n.columns) if ignore_column_order else list(left_n.columns)
        left_fps = [canonical_row(r, cols, float_tol=float_tol, ignore_column_order=False) for _, r in left_n[cols].iterrows()]
        right_fps = [canonical_row(r, cols, float_tol=float_tol, ignore_column_order=False) for _, r in right_n[cols].iterrows()]
        c_left = Counter(left_fps)
        c_right = Counter(right_fps)
        res['data_equal'] = (c_left == c_right)
        only_left_counter = c_left - c_right
        only_right_counter = c_right - c_left
        res['differences']['only_in_left_count'] = sum(only_left_counter.values())
        res['differences']['only_in_right_count'] = sum(only_right_counter.values())
        # anti-join samples: cast to str for join keys to avoid dtype mismatches
        left_s = left_n[cols].copy()
        right_s = right_n[cols].copy()
        for c in cols:
            left_s[c] = left_s[c].astype(str)
            right_s[c] = right_s[c].astype(str)
        merged_lr = left_s.merge(right_s, how='outer', indicator=True, on=cols)
        only_left_df = merged_lr[merged_lr['_merge'] == 'left_only'][cols]
        only_right_df = merged_lr[merged_lr['_merge'] == 'right_only'][cols]
        res['differences']['only_in_left_sample'] = only_left_df.head(sample_size).to_dict(orient='records')
        res['differences']['only_in_right_sample'] = only_right_df.head(sample_size).to_dict(orient='records')
        return res
    else:
        for k in keys:
            if k not in left_n.columns or k not in right_n.columns:
                res['differences']['missing_key'] = f"Colonne clé manquante: {k}"
                return res
        # Harmonize dtypes of keys by casting to string
        for k in keys:
            if left_n[k].dtype != right_n[k].dtype:
                left_n[k] = left_n[k].astype('string').str.strip()
                right_n[k] = right_n[k].astype('string').str.strip()
        non_key_cols = [c for c in set(left_n.columns).intersection(set(right_n.columns)) if c not in keys]
        left_k = left_n.set_index(keys)
        right_k = right_n.set_index(keys)
        left_only_keys = left_k.index.difference(right_k.index)
        right_only_keys = right_k.index.difference(left_k.index)
        res['differences']['left_only_keys_count'] = int(len(left_only_keys))
        res['differences']['right_only_keys_count'] = int(len(right_only_keys))
        common_idx = left_k.index.intersection(right_k.index)
        left_common = left_k.loc[common_idx, non_key_cols].sort_index()
        right_common = right_k.loc[common_idx, non_key_cols].sort_index()
        def round_floats(df):
            df2 = df.copy()
            for c in df2.columns:
                if pd.api.types.is_float_dtype(df2[c]):
                    df2[c] = df2[c].round(int(abs(np.log10(float_tol))) if float_tol > 0 else 9)
            return df2
        left_common_r = round_floats(left_common)
        right_common_r = round_floats(right_common)
        equal_mask = left_common_r.eq(right_common_r)
        mismatch_rows = ~equal_mask.all(axis=1)
        mismatches = []
        for idx in left_common_r.index[mismatch_rows]:
            row_left = left_common_r.loc[idx]
            row_right = right_common_r.loc[idx]
            diff_cols = [c for c in non_key_cols if (pd.isna(row_left[c]) != pd.isna(row_right[c])) or (row_left[c] != row_right[c])]
            if diff_cols:
                key_vals = idx if isinstance(idx, tuple) else (idx,)
                mismatches.append({
                    'keys': dict(zip(keys, key_vals)),
                    'columns': diff_cols,
                    'left_values': {c: row_left[c] for c in diff_cols},
                    'right_values': {c: row_right[c] for c in diff_cols},
                })
        res['differences']['mismatched_rows_count'] = len(mismatches)
        res['differences']['mismatched_rows_sample'] = mismatches[:sample_size]
        res['columns_equal'] = set(left_n.columns) == set(right_n.columns)
        res['data_equal'] = (
            res['columns_equal'] and
            res['differences']['left_only_keys_count'] == 0 and
            res['differences']['right_only_keys_count'] == 0 and
            res['differences']['mismatched_rows_count'] == 0
        )
        return res
