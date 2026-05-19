# Implied Volatility Module

## Purpose

Inverts the digital (cash-or-nothing) Black-Scholes pricing formula to recover annualised implied volatility from a Kalshi binary contract price. Uses a closed-form quadratic solution for ITM and OTM cases, with a `scipy.optimize.brentq` fallback for ATM contracts where the quadratic degenerates.

## File Location

`src/etl/silver/implied_vol.py`

## Dependencies

| Import | Purpose |
|--------|---------|
| `scipy.stats.norm` | `norm.ppf` (inverse CDF) and `norm.cdf` |
| `scipy.optimize.brentq` | Numerical root-finding for ATM case |
| `math` | `log`, `sqrt` |

## Module-Level Constants

| Name | Value | Description |
|------|-------|-------------|
| `PRICE_MIN` | `2` | Minimum valid Kalshi price in cents; below this IV is not computed |
| `PRICE_MAX` | `98` | Maximum valid Kalshi price in cents |
| `ATM_TOL` | `0.001` | `abs(ln(S/K)) < ATM_TOL` triggers the ATM (brentq) path |
| `_T_VALUES` | `{"T-1": 22/8760, "T0": 21/8760, "T+1": 20/8760}` | Legacy hardcoded T values; not used by SilverETL |

## Mathematical Derivation

Kalshi BTC binary markets are **cash-or-nothing digital calls** with `r = 0` (appropriate for sub-day expiry):

```
price = N(d2)
d2 = [ln(S/K) - σ²T/2] / (σ√T)
```

Where `N` is the standard normal CDF, `S` is spot, `K` is strike, `σ` is implied vol, and `T` is time to expiry in years.

### Closed-Form Inversion

Let `u = σ√T`. Substituting into the d2 equation:

```
d2 = [ln(S/K) - (u²/2)] / u  =  ln(S/K)/u  -  u/2
```

Setting `d2* = N⁻¹(price)` (the observed d2) and rearranging:

```
d2* = ln(S/K)/u - u/2

Multiply both sides by u:
u · d2* = ln(S/K) - u²/2

Rearrange to quadratic form:
u² + 2·d2*·u - 2·ln(S/K) = 0
```

Discriminant: `disc = d2*² + 2·ln(S/K)`

Roots: `u = [-d2* ± √disc] / 1`  (coefficient of u² is 1, so denominator is 1)

### Root Selection

| Condition | Branch | Root chosen | Rationale |
|-----------|--------|-------------|-----------|
| `S > K` (ITM) | `log_sk > 0` | `u = -d2* + √disc` | Only one positive root; the minus root would be negative or zero |
| `S < K` (OTM) | `log_sk < 0` | `u = -d2* - √disc` | Two positive roots exist; the smaller (lower vol) is physically meaningful for a liquid market |
| `S ≈ K` (ATM) | `abs(log_sk) < ATM_TOL` | brentq numerical solve | The quadratic degenerates near ATM (disc ≈ d2*² → one root near zero); brentq avoids the instability |

After root selection: `σ = u / √T`.

If `u ≤ 0` after selection, `None` is returned (no valid solution).

If `disc < 0`, `None` is returned (no real solution — typically means price is inconsistent with any real vol).

## Functions

### `invert_iv(p, S, K, T) -> Optional[float]`

**Primary function — called by `SilverETL.transform` and `VolSurfaceETL.transform` for every contract row.**

| Parameter | Type | Description |
|-----------|------|-------------|
| `p` | `float` | Kalshi close price in cents (raw `UInt8` value, e.g. `45`) |
| `S` | `float` | BTC spot price in dollars |
| `K` | `float` | Strike price in dollars |
| `T` | `float` | Time to expiry in years (computed dynamically by the silver ETL) |

Returns the annualised implied vol as a float (e.g. `0.85` = 85%), or `None` if:
- `p < PRICE_MIN` or `p > PRICE_MAX` (2–98 cents only)
- `T <= 0`, `S <= 0`, or `K <= 0`
- `disc < 0` (no real root)
- `u <= 0` after root selection
- ATM brentq raises `ValueError` (root not bracketed — rare)

**Performance**: O(1) per call for ITM/OTM (closed form). ATM case adds one `brentq` call, typically ~100 µs.

**ATM brentq search space**: `σ ∈ [1e-6, 50.0]` (tolerates vols from near-zero to 5000% annualised). `xtol=1e-8`, `maxiter=200`.

### `compute_t(snapshot) -> float`

**Legacy helper — not called by SilverETL in production.**

Maps snapshot label strings to hardcoded time-to-expiry values:
- `"T-1"` → `22 / 8760`
- `"T0"` → `21 / 8760`
- `"T+1"` → `20 / 8760`

These were valid for the original three-snapshot design where Kalshi contracts settled at 21:00 UTC. The silver ETL now computes T dynamically from `(expiry_time - snapshot_ts)`. `compute_t` is retained for test compatibility only.

## Edge Cases

| Case | Behaviour |
|------|-----------|
| `p == 0` or `p == 100` | Returns `None` (below PRICE_MIN / above PRICE_MAX) |
| `p == 1` or `p == 99` | Proceeds; d2* will be extreme (±2.3); typically returns a valid high vol |
| `disc < 0` | Returns `None`. Occurs when the price is inconsistent given S/K — more common with extreme moneyness |
| `S == K` exactly | Falls into ATM branch (log_sk == 0 < ATM_TOL) |
| Large T (e.g. daily contract with hours remaining) | Handled; brentq upper bound of 50.0 is generous |
| Very small T (sub-minute) | T might be computed as near-zero from dynamic calc; the `T <= 0` guard prevents division by zero |

## Usage Example

```python
from src.etl.silver.implied_vol import invert_iv

# BTC at $87,000, strike $87,000, 45 min to expiry (ATM)
T = 45 / (365.25 * 24 * 60)  # 45 minutes in years
iv = invert_iv(p=52, S=87_000, K=87_000, T=T)
# Returns approximately 0.85 (85% annualised vol)

# OTM case
iv_otm = invert_iv(p=15, S=87_000, K=90_000, T=T)
# Returns a valid vol or None if discriminant is negative
```

## Known Limitations

- The model assumes `r = 0` (no risk-free rate). For sub-day expiry this is negligible, but any backtest extending to longer-dated instruments should add a rate term.
- No carry / dividend adjustment (BTC has no dividends; futures basis is ignored).
- The OTM root choice (smaller root) is a convention. Literature is not unanimous — some practitioners take the larger root. The choice has minimal impact near-the-money and larger impact deep OTM where market prices are thin.
- `_T_VALUES` in `compute_t` encodes the old assumption that all contracts settle at 21:00 UTC (4pm ET). As of 2026, Kalshi markets are hourly (settling every UTC hour) — `compute_t` should not be used for new data.
