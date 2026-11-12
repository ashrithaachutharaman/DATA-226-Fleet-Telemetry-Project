"""B26: privacy for published summaries (k-anonymity and differential privacy).

Two independent techniques, so the report can compare them:

1. k-anonymity by small-group suppression (`k_anonymize_summary`)
   Publish "distinct vehicles per group" only for groups with at least k
   distinct vehicles. This is minimum-group-size (threshold) suppression on
   aggregate counts, the summary-table version of k-anonymity. Optionally coarsen coordinates first
   (`coarsen_coordinates`) so groups are big enough to survive. This is
   deterministic. It does not protect against someone differencing several
   releases.

2. Differential privacy by Laplace noise (`dp_distinct_vehicle_counts`)
   Publish noisy "distinct vehicles per group" counts. With contributions
   bounded per vehicle (`bound_contributions`) and the noise scale set to
   max_cells_per_vehicle / epsilon, one release satisfies epsilon-differential
   privacy at the vehicle level (adding or removing one vehicle changes the
   histogram by at most max_cells_per_vehicle). Releasing the same data several
   times spends budget: epsilons add up.

   Privacy unit: one vehicle. The release protects whether a vehicle appears
   at all in this release (sensitivity = max_cells_per_vehicle).

   Choosing max_cells_per_vehicle is a bias/noise trade-off. Too small and
   vehicles that legitimately appear in many cells (a bus is in every hour it
   is in service) get cut, so counts are biased low. Too large and the noise
   scale grows. Set it from public knowledge of the data, for example the
   number of service hours in the window when grouping by hour, not from the
   private data itself.

Notes
- Both functions expect pseudonymized vehicle IDs (B13). Hashing alone is not a
  privacy guarantee for published summaries, which is why this module exists.
- k-anonymity is not stacked on top of DP here. Suppressing cells based on the
  true counts depends on the data, so the combination is not covered by the
  DP guarantee above.
- The DP function only knows the cells present in the data unless you pass
  `domain`. For a strict guarantee, pass the full public list of possible
  groups (for example every route and hour) so empty cells are noised too.
- Missing group values (for example a null `route_id`, which the schema
  allows) are kept as their own group by both functions, never silently
  dropped. With `domain`, include a null row if that group should be released.
- Column names `vehicle_id`, `route_id`, `lat` and `lon` follow the common
  vehicle-event schema (card B4).
"""

from typing import Optional, Sequence, Tuple

import numpy as np
import pandas as pd


def _get_rng(rng: Optional[np.random.Generator]) -> np.random.Generator:
    return np.random.default_rng() if rng is None else rng


# ---------------------------------------------------------------------------
# k-anonymity
# ---------------------------------------------------------------------------


def coarsen_coordinates(
    df: pd.DataFrame,
    lat_col: str = "lat",
    lon_col: str = "lon",
    decimals: int = 2,
) -> pd.DataFrame:
    """Return a copy with coordinates rounded to `decimals` places.

    Two decimals is about 1.1 km in latitude. Fewer decimals means bigger cells,
    so more groups reach k, at the cost of resolution.
    """
    out = df.copy()
    out[lat_col] = out[lat_col].round(decimals)
    out[lon_col] = out[lon_col].round(decimals)
    return out


def k_anonymize_summary(
    df: pd.DataFrame,
    group_cols: Sequence[str],
    k: int,
    id_col: str = "vehicle_id",
) -> Tuple[pd.DataFrame, int]:
    """Count distinct vehicles per group and drop groups with fewer than k.

    Returns (published, n_suppressed). `published` has the group columns plus
    `n_vehicles`, and every row covers at least k distinct vehicles.
    `n_suppressed` is the number of groups removed, useful for reporting the
    utility cost of a given k. Rows with a missing group value form their own
    group.
    """
    if k < 1:
        raise ValueError("k must be at least 1")

    counts = (
        df.groupby(list(group_cols), dropna=False)[id_col]
        .nunique()
        .rename("n_vehicles")
        .reset_index()
    )
    keep = counts["n_vehicles"] >= k
    return counts[keep].reset_index(drop=True), int((~keep).sum())


# ---------------------------------------------------------------------------
# Differential privacy
# ---------------------------------------------------------------------------


def laplace_noisy_counts(
    counts,
    epsilon: float,
    sensitivity: float = 1.0,
    rng: Optional[np.random.Generator] = None,
    clamp_at_zero: bool = True,
    round_to_int: bool = True,
) -> np.ndarray:
    """Add Laplace(sensitivity / epsilon) noise to each count.

    Clamping at zero and rounding are post-processing, so they do not weaken
    the guarantee. Turn them off if you need unbiased noisy values.
    """
    if epsilon <= 0:
        raise ValueError("epsilon must be positive")
    if sensitivity <= 0:
        raise ValueError("sensitivity must be positive")

    rng = _get_rng(rng)
    values = np.asarray(counts, dtype=float)
    noisy = values + rng.laplace(0.0, sensitivity / epsilon, size=values.shape)
    if clamp_at_zero:
        noisy = np.maximum(noisy, 0.0)
    if round_to_int:
        noisy = np.rint(noisy).astype(int)
    return noisy


def bound_contributions(
    df: pd.DataFrame,
    group_cols: Sequence[str],
    max_cells: int,
    id_col: str = "vehicle_id",
    rng: Optional[np.random.Generator] = None,
) -> pd.DataFrame:
    """Keep rows from at most `max_cells` randomly chosen groups per vehicle.

    A vehicle can show up in many groups (different routes, hours, areas).
    Capping that number is what makes the DP sensitivity equal to `max_cells`.
    There is no default: see the module docstring for how to choose it.
    Missing group values count as a group.
    """
    if max_cells < 1:
        raise ValueError("max_cells must be at least 1")

    rng = _get_rng(rng)
    group_cols = list(group_cols)
    cells = df[[id_col] + group_cols].drop_duplicates()
    cells = cells.iloc[rng.permutation(len(cells))]
    kept = cells.groupby(id_col, sort=False).head(max_cells)
    return df.merge(kept, on=[id_col] + group_cols, how="inner")


def dp_distinct_vehicle_counts(
    df: pd.DataFrame,
    group_cols: Sequence[str],
    epsilon: float,
    max_cells_per_vehicle: int,
    id_col: str = "vehicle_id",
    domain: Optional[pd.DataFrame] = None,
    rng: Optional[np.random.Generator] = None,
    include_true_counts: bool = False,
) -> pd.DataFrame:
    """Epsilon-DP count of distinct vehicles per group (one release).

    Privacy unit is one vehicle: the release protects whether a vehicle appears
    at all in it. Sensitivity is `max_cells_per_vehicle`.

    max_cells_per_vehicle: most groups one vehicle may count toward. Required,
        because the right value depends on the grouping: grouping by route and
        hour, a bus in service 12 hours needs 12 or its counts are cut. Pick it
        from public knowledge (service hours, number of routes), not from the
        private data. Smaller means less noise but more undercounting.
    domain: DataFrame of all possible groups (the group columns). When given,
        every group in it is released, with zero for empty ones, and groups
        outside it are dropped. Use this for a strict guarantee. Without it,
        the groups present in `df` are released.
    include_true_counts: adds an `n_vehicles` column with the real distinct
        vehicle counts from `df`, computed before contribution bounding, so an
        accuracy evaluation sees both the bias from bounding and the noise.
        Only for evaluating accuracy (for example B28 trade-off curves). Never
        publish it.

    Missing group values (a null `route_id`) are kept as their own group.

    Returns the group columns plus `n_vehicles_dp`.
    """
    rng = _get_rng(rng)
    group_cols = list(group_cols)

    def distinct_counts(frame: pd.DataFrame, name: str) -> pd.DataFrame:
        return (
            frame.groupby(group_cols, dropna=False)[id_col]
            .nunique()
            .rename(name)
            .reset_index()
        )

    true_counts = distinct_counts(df, "n_vehicles")
    bounded = bound_contributions(df, group_cols, max_cells_per_vehicle, id_col, rng)
    bounded_counts = distinct_counts(bounded, "n_bounded")

    if domain is not None:
        cells = domain[group_cols].drop_duplicates()
    else:
        cells = true_counts[group_cols]
    counts = (
        cells.merge(bounded_counts, on=group_cols, how="left")
        .merge(true_counts, on=group_cols, how="left")
        .fillna({"n_bounded": 0, "n_vehicles": 0})
    )

    counts["n_vehicles_dp"] = laplace_noisy_counts(
        counts["n_bounded"].astype(int),
        epsilon=epsilon,
        sensitivity=max_cells_per_vehicle,
        rng=rng,
    )
    counts = counts.drop(columns="n_bounded")
    if include_true_counts:
        counts["n_vehicles"] = counts["n_vehicles"].astype(int)
    else:
        counts = counts.drop(columns="n_vehicles")
    return counts.reset_index(drop=True)
