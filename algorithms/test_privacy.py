"""Tests for B26 (run from the repo root: python -m pytest algorithms/test_privacy.py)."""

import numpy as np
import pandas as pd
import pytest

from algorithms.privacy import (
    bound_contributions,
    coarsen_coordinates,
    dp_distinct_vehicle_counts,
    k_anonymize_summary,
    laplace_noisy_counts,
)


def make_events():
    """Route A: 5 vehicles. Route B: 2 vehicles. Route C: 1 vehicle.

    Vehicle a1 appears many times, to check that distinct vehicles are counted,
    not rows.
    """
    rows = []
    for v in ["a1", "a2", "a3", "a4", "a5"]:
        rows += [{"vehicle_id": v, "route_id": "A"}] * 3
    for v in ["b1", "b2"]:
        rows.append({"vehicle_id": v, "route_id": "B"})
    rows.append({"vehicle_id": "c1", "route_id": "C"})
    return pd.DataFrame(rows)


# ---- k-anonymity -----------------------------------------------------------


def test_k_anonymize_drops_small_groups_and_counts_distinct_vehicles():
    published, suppressed = k_anonymize_summary(make_events(), ["route_id"], k=3)
    assert published.to_dict("records") == [{"route_id": "A", "n_vehicles": 5}]
    assert suppressed == 2


def test_k_anonymize_every_published_group_has_at_least_k():
    published, _ = k_anonymize_summary(make_events(), ["route_id"], k=2)
    assert (published["n_vehicles"] >= 2).all()
    assert set(published["route_id"]) == {"A", "B"}


def test_k_equal_one_keeps_everything():
    published, suppressed = k_anonymize_summary(make_events(), ["route_id"], k=1)
    assert len(published) == 3
    assert suppressed == 0


def test_k_zero_rejected():
    with pytest.raises(ValueError):
        k_anonymize_summary(make_events(), ["route_id"], k=0)


def test_coarsen_coordinates_rounds_and_does_not_mutate():
    df = pd.DataFrame({"lat": [37.33456, 37.33999], "lon": [-121.88612, -121.88001]})
    out = coarsen_coordinates(df, decimals=2)
    assert out["lat"].tolist() == [37.33, 37.34]
    assert out["lon"].tolist() == [-121.89, -121.88]
    assert df["lat"].tolist() == [37.33456, 37.33999]


def test_coarsening_lets_more_groups_reach_k():
    df = pd.DataFrame(
        {
            "vehicle_id": ["v1", "v2", "v3"],
            "lat": [37.3301, 37.3322, 37.3349],
            "lon": [-121.8801, -121.8822, -121.8849],
        }
    )
    fine, _ = k_anonymize_summary(df, ["lat", "lon"], k=3)
    coarse, _ = k_anonymize_summary(coarsen_coordinates(df, decimals=2), ["lat", "lon"], k=3)
    assert len(fine) == 0
    assert coarse["n_vehicles"].tolist() == [3]


# ---- Laplace noise ---------------------------------------------------------


def test_noise_has_expected_mean_and_scale():
    rng = np.random.default_rng(0)
    n = 200_000
    noisy = laplace_noisy_counts(
        np.full(n, 100.0),
        epsilon=0.5,
        sensitivity=1.0,
        rng=rng,
        clamp_at_zero=False,
        round_to_int=False,
    )
    # Laplace(b): mean 0, standard deviation sqrt(2) * b, with b = 1 / 0.5 = 2.
    assert abs(noisy.mean() - 100.0) < 0.05
    assert noisy.std() == pytest.approx(np.sqrt(2) * 2.0, rel=0.02)


def test_noise_scale_grows_with_sensitivity_and_shrinks_with_epsilon():
    def std(eps, sens):
        out = laplace_noisy_counts(
            np.zeros(100_000),
            eps,
            sens,
            rng=np.random.default_rng(1),
            clamp_at_zero=False,
            round_to_int=False,
        )
        return out.std()

    assert std(1.0, 3.0) / std(1.0, 1.0) == pytest.approx(3.0, rel=0.03)
    assert std(0.5, 1.0) / std(1.0, 1.0) == pytest.approx(2.0, rel=0.03)


def test_clamp_and_round():
    out = laplace_noisy_counts(np.zeros(1000), 0.1, rng=np.random.default_rng(2))
    assert out.dtype.kind == "i"
    assert (out >= 0).all()


def test_same_seed_same_noise():
    a = laplace_noisy_counts([5, 10], 1.0, rng=np.random.default_rng(7))
    b = laplace_noisy_counts([5, 10], 1.0, rng=np.random.default_rng(7))
    assert (a == b).all()


@pytest.mark.parametrize("eps", [0, -1])
def test_bad_epsilon_rejected(eps):
    with pytest.raises(ValueError):
        laplace_noisy_counts([1], eps)


def test_bad_sensitivity_rejected():
    with pytest.raises(ValueError):
        laplace_noisy_counts([1], 1.0, sensitivity=0)


# ---- Contribution bounding -------------------------------------------------


def test_bound_contributions_caps_cells_per_vehicle():
    df = pd.DataFrame(
        {
            "vehicle_id": ["v1"] * 4 + ["v2"] * 2,
            "route_id": ["A", "B", "C", "D", "A", "B"],
        }
    )
    out = bound_contributions(df, ["route_id"], max_cells=2, rng=np.random.default_rng(3))
    per_vehicle = out.groupby("vehicle_id")["route_id"].nunique()
    assert per_vehicle["v1"] == 2
    assert per_vehicle["v2"] == 2  # already within the cap, untouched
    assert len(out[out["vehicle_id"] == "v2"]) == 2


def test_bound_contributions_rejects_zero():
    with pytest.raises(ValueError):
        bound_contributions(make_events(), ["route_id"], max_cells=0)


# ---- DP summary ------------------------------------------------------------


def test_dp_counts_are_close_to_truth_with_huge_epsilon():
    out = dp_distinct_vehicle_counts(
        make_events(),
        ["route_id"],
        epsilon=1e6,
        max_cells_per_vehicle=1,
        rng=np.random.default_rng(4),
        include_true_counts=True,
    )
    assert (out["n_vehicles_dp"] == out["n_vehicles"]).all()
    assert out.set_index("route_id")["n_vehicles"].to_dict() == {"A": 5, "B": 2, "C": 1}


def test_true_counts_hidden_by_default():
    out = dp_distinct_vehicle_counts(
        make_events(), ["route_id"], epsilon=1.0, max_cells_per_vehicle=1,
        rng=np.random.default_rng(5),
    )
    assert "n_vehicles" not in out.columns
    assert "n_vehicles_dp" in out.columns


def test_domain_includes_empty_cells_and_drops_unknown_ones():
    domain = pd.DataFrame({"route_id": ["A", "B", "Z"]})  # Z is empty, C is not in domain
    out = dp_distinct_vehicle_counts(
        make_events(),
        ["route_id"],
        epsilon=1e6,
        max_cells_per_vehicle=1,
        domain=domain,
        rng=np.random.default_rng(6),
        include_true_counts=True,
    )
    assert out.set_index("route_id")["n_vehicles"].to_dict() == {"A": 5, "B": 2, "Z": 0}


def test_dp_summary_with_two_group_columns():
    df = pd.DataFrame(
        {
            "vehicle_id": ["v1", "v2", "v3", "v1"],
            "route_id": ["A", "A", "B", "B"],
            "hour": [8, 8, 9, 9],
        }
    )
    out = dp_distinct_vehicle_counts(
        df,
        ["route_id", "hour"],
        epsilon=1e6,
        max_cells_per_vehicle=2,
        rng=np.random.default_rng(8),
        include_true_counts=True,
    )
    truth = {(r.route_id, r.hour): r.n_vehicles for r in out.itertuples()}
    assert truth == {("A", 8): 2, ("B", 9): 2}


def make_buses(n_buses=40, hours=range(6, 18)):
    """n_buses split over routes 22 and 23, each in service every hour."""
    rows = []
    for i in range(n_buses):
        route = "22" if i % 2 == 0 else "23"
        for h in hours:
            rows += [{"vehicle_id": f"bus{i}", "route_id": route, "hour": h}] * 2
    return pd.DataFrame(rows)


def test_true_counts_are_from_before_bounding():
    # Each bus is in 12 hourly cells, so max_cells=1 keeps only 1 of them.
    out = dp_distinct_vehicle_counts(
        make_buses(),
        ["route_id", "hour"],
        epsilon=1e6,
        max_cells_per_vehicle=1,
        rng=np.random.default_rng(9),
        include_true_counts=True,
    )
    assert len(out) == 24  # every (route, hour) present in the data
    assert (out["n_vehicles"] == 20).all()  # real counts, not the bounded ones
    assert out["n_vehicles_dp"].sum() == 40  # each bus kept in exactly one cell


def test_max_cells_matching_service_hours_removes_bounding_bias():
    out = dp_distinct_vehicle_counts(
        make_buses(),
        ["route_id", "hour"],
        epsilon=1e6,
        max_cells_per_vehicle=12,
        rng=np.random.default_rng(10),
        include_true_counts=True,
    )
    assert (out["n_vehicles_dp"] == out["n_vehicles"]).all()
    assert (out["n_vehicles"] == 20).all()


def test_max_cells_has_no_default():
    with pytest.raises(TypeError):
        dp_distinct_vehicle_counts(make_events(), ["route_id"], epsilon=1.0)
    with pytest.raises(TypeError):
        bound_contributions(make_events(), ["route_id"])


def make_events_with_missing_route():
    df = make_events()
    extra = pd.DataFrame({"vehicle_id": ["n1", "n2"], "route_id": [None, None]})
    return pd.concat([df, extra], ignore_index=True)


def test_missing_route_kept_as_own_group_in_k_anonymity():
    published, _ = k_anonymize_summary(make_events_with_missing_route(), ["route_id"], k=1)
    missing = published[published["route_id"].isna()]
    assert missing["n_vehicles"].tolist() == [2]


def test_missing_route_kept_as_own_group_in_dp():
    out = dp_distinct_vehicle_counts(
        make_events_with_missing_route(),
        ["route_id"],
        epsilon=1e6,
        max_cells_per_vehicle=1,
        rng=np.random.default_rng(11),
        include_true_counts=True,
    )
    missing = out[out["route_id"].isna()]
    assert missing["n_vehicles"].tolist() == [2]
    assert missing["n_vehicles_dp"].tolist() == [2]
