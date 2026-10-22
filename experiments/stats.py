"""Interval estimates and two-sample tests used by the report tables (pure functions)."""

from __future__ import annotations

import math
import random
from collections.abc import Sequence
from dataclasses import dataclass
from statistics import NormalDist

Z95 = NormalDist().inv_cdf(0.975)


@dataclass(frozen=True, slots=True)
class Rate:
    """A proportion ``k / n`` with its 95% Wilson score interval."""

    k: int
    n: int
    lo: float
    hi: float

    @property
    def p(self) -> float:
        return self.k / self.n if self.n else float("nan")

    def to_json(self) -> dict[str, float | int]:
        return {"k": self.k, "n": self.n, "p": self.p, "lo": self.lo, "hi": self.hi}


def wilson(k: int, n: int, z: float = Z95) -> Rate:
    """Wilson (1927) score interval for a binomial proportion."""
    if n == 0:
        return Rate(k, n, float("nan"), float("nan"))
    p = k / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return Rate(k, n, max(0.0, centre - half), min(1.0, centre + half))


@dataclass(frozen=True, slots=True)
class Mean:
    """A sample mean with its standard error and a 95% percentile bootstrap interval."""

    mean: float
    se: float
    lo: float
    hi: float
    n: int

    def to_json(self) -> dict[str, float | int]:
        return {"mean": self.mean, "se": self.se, "lo": self.lo, "hi": self.hi, "n": self.n}


def bootstrap_mean(values: Sequence[float], resamples: int, seed: int) -> Mean:
    """Mean, standard error (s / sqrt(n)) and a percentile bootstrap interval of the mean."""
    n = len(values)
    if n == 0:
        nan = float("nan")
        return Mean(nan, nan, nan, nan, 0)
    m = sum(values) / n
    sd = math.sqrt(sum((v - m) ** 2 for v in values) / (n - 1)) if n > 1 else 0.0
    rng = random.Random(seed)
    means = sorted(sum(rng.choices(values, k=n)) / n for _ in range(resamples))
    lo = means[int(0.025 * (resamples - 1))]
    hi = means[int(0.975 * (resamples - 1))]
    return Mean(m, sd / math.sqrt(n), lo, hi, n)


@dataclass(frozen=True, slots=True)
class TestResult:
    statistic: float
    p_value: float
    critical: float

    def to_json(self) -> dict[str, float]:
        return {"z": self.statistic, "p_value": self.p_value, "critical": self.critical}


def two_proportion_z(k1: int, n1: int, k2: int, n2: int) -> TestResult:
    """Two-proportion z-test with the pooled variance; H0: the two rates are equal."""
    p = (k1 + k2) / (n1 + n2)
    se = math.sqrt(p * (1 - p) * (1 / n1 + 1 / n2))
    z = 0.0 if se == 0 else (k1 / n1 - k2 / n2) / se
    return TestResult(z, 2 * (1 - NormalDist().cdf(abs(z))), Z95)


def mann_whitney(a: Sequence[float], b: Sequence[float]) -> TestResult:
    """Mann-Whitney U test, normal approximation with tie correction; H0: equal distributions.

    The statistic is the z-score of U for sample ``a``; a positive z means ``a`` tends to be larger.
    """
    n1, n2 = len(a), len(b)
    pooled = sorted([(v, 0) for v in a] + [(v, 1) for v in b])
    ranks = [0.0] * len(pooled)
    ties = 0.0
    i = 0
    while i < len(pooled):
        j = i
        while j + 1 < len(pooled) and pooled[j + 1][0] == pooled[i][0]:
            j += 1
        r = (i + j) / 2 + 1
        for k in range(i, j + 1):
            ranks[k] = r
        t = j - i + 1
        ties += t**3 - t
        i = j + 1
    r1 = sum(r for r, (_, g) in zip(ranks, pooled, strict=True) if g == 0)
    u1 = r1 - n1 * (n1 + 1) / 2
    n = n1 + n2
    var = n1 * n2 / 12 * ((n + 1) - ties / (n * (n - 1)))
    z = 0.0 if var <= 0 else (u1 - n1 * n2 / 2) / math.sqrt(var)
    return TestResult(z, 2 * (1 - NormalDist().cdf(abs(z))), Z95)
