"""The interval estimates and tests behind the report tables (experiments/stats.py)."""

from __future__ import annotations

import pytest

from experiments.stats import bootstrap_mean, mann_whitney, two_proportion_z, wilson


def test_wilson_matches_reference_values() -> None:
    # Reference: Wilson (1927) interval for 0 of 300 and 6 of 100, z = 1.959964.
    zero = wilson(0, 300)
    assert zero.lo == 0.0
    assert zero.hi == pytest.approx(0.01264, abs=1e-4)
    six = wilson(6, 100)
    assert (six.lo, six.hi) == pytest.approx((0.02778, 0.12476), abs=1e-4)


def test_two_proportion_z_is_symmetric_and_zero_for_equal_rates() -> None:
    assert two_proportion_z(10, 100, 10, 100).statistic == 0.0
    a = two_proportion_z(90, 100, 50, 100)
    b = two_proportion_z(50, 100, 90, 100)
    assert a.statistic == pytest.approx(-b.statistic)
    assert a.statistic == pytest.approx(6.172, abs=1e-3)
    assert a.p_value < 1e-6


def test_mann_whitney_sign_and_ties() -> None:
    low, high = [1.0, 2.0, 3.0, 4.0, 5.0], [6.0, 7.0, 8.0, 9.0, 10.0]
    t = mann_whitney(low, high)
    assert t.statistic < -2.5
    assert mann_whitney(high, low).statistic == pytest.approx(-t.statistic)
    assert mann_whitney([1.0, 1.0], [1.0, 1.0]).statistic == 0.0


def test_bootstrap_mean_is_deterministic_and_brackets_the_mean() -> None:
    values = [float(v) for v in range(20)]
    a = bootstrap_mean(values, 2000, seed=1)
    assert a == bootstrap_mean(values, 2000, seed=1)
    assert a.lo < a.mean < a.hi
    assert a.se == pytest.approx(1.3229, abs=1e-3)
