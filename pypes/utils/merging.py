import pandas as pd


def merge_on_identity_intersection_or_cross(
    df0: pd.DataFrame,
    df1: pd.DataFrame,
    *,
    how: str = "outer",
    validate: str | None = "one_to_one",
    prefer: str = "left",  # for restoring original key objects when key intersection is non-empty
    keep_order: bool = True,  # preserve df0 order, then df1-only order (outer only)
) -> pd.DataFrame:
    """
    Merge df0 and df1 on the intersection of their columns, using identity semantics
    for key values via id(), so non-orderable Python objects are safe.

    - If the column intersection is non-empty: performs a join on those columns via
      a proxy `_k` key (tuple of ids), and restores original key objects afterward.
    - If the column intersection is empty: performs a cross join.

    If keep_order and how == "outer", the output row order is:
      1) keys in df0, in df0 appearance order
      2) keys only in df1, in df1 appearance order
    """
    if prefer not in {"left", "right"}:
        raise ValueError("prefer must be 'left' or 'right'")

    key_cols = list(df0.columns.intersection(df1.columns))

    if not key_cols:
        # Empty intersection: cross join
        return df0.merge(df1, how="cross")

    def make_k(df: pd.DataFrame) -> pd.Series:
        return pd.Series(list(zip(*(df[c].map(id) for c in key_cols))), index=df.index, name="_k")

    k0 = make_k(df0)
    k1 = make_k(df1)

    # Uniqueness checks (fail fast; don't silently dedupe)
    if not k0.is_unique:
        dup = k0[k0.duplicated(keep=False)]
        raise ValueError(f"df0 has duplicate composite keys; example: {dup.iloc[0]!r}")
    if not k1.is_unique:
        dup = k1[k1.duplicated(keep=False)]
        raise ValueError(f"df1 has duplicate composite keys; example: {dup.iloc[0]!r}")

    # Build proxy->original lookup for restoring key objects
    lookup0 = df0.assign(_k=k0).set_index("_k")[key_cols]
    lookup1 = df1.assign(_k=k1).set_index("_k")[key_cols]
    lookup = lookup0.combine_first(lookup1) if prefer == "left" else lookup1.combine_first(lookup0)
    lookup_dict = lookup.to_dict(orient="index")

    # Proxy DFs: replace key cols with _k and drop originals (so no overlaps remain)
    kdf0 = df0.assign(_k=k0).drop(columns=key_cols)
    kdf1 = df1.assign(_k=k1).drop(columns=key_cols)

    # Desired stable order (only meaningful for outer joins)
    if keep_order and how == "outer":
        left_rank = pd.Series(range(len(df0)), index=k0.values)
        right_rank = pd.Series(range(len(df1)), index=k1.values)

    merged = kdf0.merge(kdf1, on="_k", how=how, sort=False, validate=validate)

    # Enforce order: df0 order first, then df1-only order (outer only)
    if keep_order and how == "outer":
        ord0 = merged["_k"].map(left_rank)
        ord1 = merged["_k"].map(right_rank)
        merged["_ord"] = ord0.where(ord0.notna(), len(df0) + ord1)
        merged = merged.sort_values("_ord", kind="mergesort").drop(columns="_ord")

    # Restore original key columns
    real_keys = merged["_k"].map(lookup_dict)
    for c in key_cols:
        merged[c] = real_keys.map(lambda d: d.get(c) if isinstance(d, dict) else pd.NA)

    other_cols = [c for c in merged.columns if c not in {"_k", *key_cols}]
    return merged[key_cols + other_cols]
