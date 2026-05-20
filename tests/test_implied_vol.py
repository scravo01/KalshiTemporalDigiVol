import math

import pytest
from scipy.stats import norm

from src.etl.silver.implied_vol import PRICE_MAX, PRICE_MIN, invert_iv

_T0 = 21 / 8760  # standard T for most tests


def _reprice(sigma: float, S: float, K: float, T: float) -> float:
    """Re-price a digital call from recovered sigma to verify round-trip."""
    d2 = (math.log(S / K) - 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
    return norm.cdf(d2) * 100  # back to cents


class TestGuards:
    def test_price_below_min_returns_none(self):
        assert invert_iv(PRICE_MIN - 1, 50_000.0, 50_000.0, _T0) is None

    def test_price_above_max_returns_none(self):
        assert invert_iv(PRICE_MAX + 1, 50_000.0, 50_000.0, _T0) is None

    def test_price_at_min_boundary_accepted(self):
        assert invert_iv(PRICE_MIN, 50_000.0, 55_000.0, _T0) is not None

    def test_price_at_max_boundary_accepted(self):
        assert invert_iv(PRICE_MAX, 50_000.0, 45_000.0, _T0) is not None

    def test_zero_time_returns_none(self):
        assert invert_iv(50, 50_000.0, 50_000.0, 0.0) is None

    def test_negative_time_returns_none(self):
        assert invert_iv(50, 50_000.0, 50_000.0, -0.001) is None

    def test_zero_spot_returns_none(self):
        assert invert_iv(50, 0.0, 50_000.0, _T0) is None

    def test_zero_strike_returns_none(self):
        assert invert_iv(50, 50_000.0, 0.0, _T0) is None


class TestSignOfResult:
    def test_otm_positive_iv(self):
        result = invert_iv(35, 50_000.0, 52_000.0, _T0)
        assert result is not None
        assert result > 0

    def test_itm_positive_iv(self):
        result = invert_iv(70, 50_000.0, 48_000.0, _T0)
        assert result is not None
        assert result > 0

    def test_atm_brentq_fallback_positive(self):
        result = invert_iv(48, 50_000.0, 50_000.0, _T0)
        assert result is not None
        assert result > 0


class TestRoundTrip:
    """Invert, then re-price — recovered price should match original within 0.01 cents."""

    def test_round_trip_otm(self):
        p_cents = 38
        sigma = invert_iv(p_cents, 95_000.0, 96_000.0, _T0)
        assert sigma is not None
        repriced = _reprice(sigma, 95_000.0, 96_000.0, _T0)
        assert abs(repriced - p_cents) < 0.01

    def test_round_trip_itm(self):
        p_cents = 63
        sigma = invert_iv(p_cents, 95_000.0, 94_000.0, _T0)
        assert sigma is not None
        repriced = _reprice(sigma, 95_000.0, 94_000.0, _T0)
        assert abs(repriced - p_cents) < 0.01

    def test_round_trip_deep_otm(self):
        p_cents = 10
        sigma = invert_iv(p_cents, 95_000.0, 100_000.0, _T0)
        assert sigma is not None
        repriced = _reprice(sigma, 95_000.0, 100_000.0, _T0)
        assert abs(repriced - p_cents) < 0.05  # slightly looser for deep OTM

    def test_round_trip_deep_itm(self):
        p_cents = 90
        sigma = invert_iv(p_cents, 95_000.0, 90_000.0, _T0)
        assert sigma is not None
        repriced = _reprice(sigma, 95_000.0, 90_000.0, _T0)
        assert abs(repriced - p_cents) < 0.05

    @pytest.mark.parametrize("snap,expected_t", [
        ("T-1", 22 / 8760),
        ("T0",  21 / 8760),
        ("T+1", 20 / 8760),
    ])
    def test_round_trip_all_snapshots(self, snap, expected_t):
        p_cents = 35  # 45 is above the max achievable price for 1%-OTM; use 35
        sigma = invert_iv(p_cents, 95_000.0, 96_000.0, expected_t)
        assert sigma is not None
        repriced = _reprice(sigma, 95_000.0, 96_000.0, expected_t)
        assert abs(repriced - p_cents) < 0.01
