import math
from typing import Optional

from scipy.optimize import brentq
from scipy.stats import norm

PRICE_MIN = 2
PRICE_MAX = 98
ATM_TOL = 0.001  # |ln(S/K)| below this → ATM path (brentq)

def invert_iv(p: float, S: float, K: float, T: float) -> Optional[float]:
    """
    Invert digital Black-Scholes to recover implied vol.

    Parameters
    ----------
    p : float  — Kalshi close price in cents (0–100, raw UInt8 value)
    S : float  — BTC spot price in dollars
    K : float  — strike price in dollars
    T : float  — time to expiry in years

    Returns
    -------
    Annualised implied vol (e.g. 0.8 = 80%) or None if inversion fails.

    Method
    ------
    price = N(d2),  d2 = [ln(S/K) − σ²T/2] / (σ√T)
    Let u = σ√T. Substituting d2* = N⁻¹(price) and rearranging gives a
    quadratic in u:  u² + 2·d2*·u − 2·ln(S/K) = 0
    Discriminant: d2*² + 2·ln(S/K)
    Root selection:
      ITM (S > K): unique positive root  →  u = −d2* + √disc
      OTM (S < K): two positive roots   →  take smaller: u = −d2* − √disc
      ATM (|ln(S/K)| < ATM_TOL): quadratic degenerates; use brentq.
    """
    if not (PRICE_MIN <= p <= PRICE_MAX):
        return None
    if T <= 0 or S <= 0 or K <= 0:
        return None

    price = p / 100.0
    d2_star = norm.ppf(price)
    log_sk = math.log(S / K)
    disc = d2_star**2 + 2.0 * log_sk

    if disc < 0:
        return None

    if abs(log_sk) < ATM_TOL:
        # ATM: quadratic gives u=0 (degenerate) — solve numerically
        sqrt_T = math.sqrt(T)

        def objective(sigma: float) -> float:
            d2 = (log_sk - 0.5 * sigma**2 * T) / (sigma * sqrt_T)
            return norm.cdf(d2) - price

        try:
            sigma = brentq(objective, 1e-6, 50.0, xtol=1e-8, maxiter=200)
        except ValueError:
            return None
        return float(sigma)

    sqrt_disc = math.sqrt(disc)
    if S > K:  # ITM — unique positive root
        u = -d2_star + sqrt_disc
    else:  # OTM — two positive roots; take the smaller (lower vol)
        u = -d2_star - sqrt_disc

    if u <= 0:
        return None

    return float(u / math.sqrt(T))
