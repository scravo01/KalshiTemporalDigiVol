# Investment Research Memo: Kalshi BTC Digital Vol

**Strategy:** Delta-Hedged Short Implied Volatility — Kalshi BTC Hourly Binary Options  
**Sample:** 61 days, March 21 – May 18, 2026 · 1,419 hourly observations  
**Status:** Research complete; out-of-sample validation included

---

## Executive Summary

Kalshi BTC hourly binary options systematically overcharge for implied volatility relative to realized vol, with ATM IV exceeding 5-minute realized vol by ~16 annualized vol points in 73% of observations. A delta-hedged short-OTM strategy with a modest IV/RV filter generates a Sharpe of 7.15 over the full sample and holds out of sample. Restricting trades to the Asia session (01:00–10:59 UTC) improves the Sharpe to 10.53 and cuts maximum drawdown by 77% — at the cost of fewer opportunities. Market capacity is the binding constraint: the strategy is viable up to ~$50–100k AUM before slippage erodes the edge.

---

## Research Questions

1. Does Kalshi ATM implied vol systematically exceed 5-minute realized vol, and is the premium statistically significant?
2. Is the log(IV)/log(RV) relationship stable, or does it vary structurally by hour of day?
3. Can the premium be extracted via a delta-hedged short-OTM strategy? Does restricting to Asia-session hours improve risk-adjusted returns?

---

## Methodology

**Universe:** All BTC hourly binary options on Kalshi from March 21 to May 18, 2026. Contracts are cash-or-nothing digital calls settling at each UTC hour with ~188 available strikes.

**Realized vol:** 5-minute and hourly RV computed as the square root of summed squared log returns of BTCUSDT 1-minute klines (Binance), annualized by ×105,120 (5-min) and ×8,760 (hourly).

**Implied vol:** Digital Black-Scholes inversion using the closed-form quadratic-in-u method. ATM IV selected via nearest-neighbor delta matching (|prob_itm − 0.50| minimized). IV filtered to 20%–500% annualized range.

**Cointegration analysis:** OLS regression of log(IV) on log(RV), pooled and hour-specific. F-test and likelihood-ratio test compare restricted (pooled) vs unrestricted (per-hour α, β) models.

**Backtest rules:**
- Position: Short 1,000 Kalshi BTC binary calls at +25Δ OTM (~25¢ entry premium)
- Entry filter: IV/RV ratio ≥ 1.20; minimum premium ≥ 10¢
- Delta hedge: Rebalance BTC position every 5 minutes
- Early exit: Close at 50% of entry premium (gamma management)
- Costs: 5 bps on contract entry, 1 bps per BTC rebalance, 10 bps slippage on premium
- Walk-forward split: In-sample March–April; out-of-sample May

---

## Findings: Vol Premium

| Metric | Value |
|--------|-------|
| Mean IV − RV (annualized) | +0.161 (~16 vol points) |
| Median IV − RV | −0.106 |
| % observations IV > RV | 73% |
| One-sided t-test | p ≈ 0 (highly significant) |
| Mean log-spread exp(α) | ~1.02–1.03× IV > RV in log-space |

The vol premium is persistent across all three calendar months in the sample (March, April, May 2026).

**Hour-specific cointegration (Q3):** The unrestricted model with per-hour intercepts (α_h) and slopes (β_h) is strongly preferred over the pooled model:

- F-test: significant
- LR test χ²: significant
- Interpretation: The IV/RV relationship is structurally heterogeneous across UTC hours — some hours show 2–3× stronger log-premiums than others

Hours 14–15 UTC are persistently loss-making. Hours 2, 4, 9, and 11 UTC (Asia session) show the strongest per-hour log-premiums.

---

## Findings: Backtest — All Hours

Short +25Δ OTM binary calls, all 24 UTC expiry hours:

| Metric | Full Period | In-Sample (Mar–Apr) | Out-of-Sample (May) |
|--------|-------------|---------------------|---------------------|
| N Trades | 82 | 64 | 18 |
| Total Return | $5,960 | $2,890 | $3,069 |
| Annualized Return | 53.1% | — | — |
| Sharpe Ratio | 7.15 | 4.42 | 21.98 |
| Win Rate | 79.3% | 73.4% | 100% |
| Max Drawdown | −$3,318 | — | $0 |
| Calmar Ratio | 1.80 | — | — |
| ITM Expiry Rate | 11.0% | — | — |
| Early Exit Rate | 82.9% | — | — |
| Avg P&L / Trade | $72.68 | — | — |
| Avg Hedge P&L / Trade | $48.58 | — | — |
| Total Transaction Costs | −$4,292 | — | — |

**Bootstrap validation** (1,000 iterations, 50% subsamples):

| Stat | Value |
|------|-------|
| P5 Sharpe | 2.06 |
| Median Sharpe | 6.25 |
| P95 Sharpe | 14.52 |
| % subsamples > 0 | 99.8% |

The strategy is not driven by a handful of lucky trades; 99.8% of random half-sample draws are profitable.

---

## Findings: Backtest — Asia-Hours Filter (01:00–10:59 UTC)

Same rules, trades only during Asia session:

| Metric | Asia Only | All Hours | Improvement |
|--------|-----------|-----------|-------------|
| N Trades | 31 | 82 | −62% |
| Total Return | $3,248 | $5,960 | −46% |
| Annualized Return | 51.5% | 53.1% | −1.6 pp |
| Sharpe Ratio | **10.53** | 7.15 | **+47%** |
| Win Rate | **90.3%** | 79.3% | **+11 pp** |
| Max Drawdown | −$761 | −$3,318 | **−77%** |
| Calmar Ratio | **4.27** | 1.80 | **+137%** |
| Avg P&L / Trade | $104.77 | $72.68 | +44% |
| Total Transaction Costs | −$1,239 | −$4,292 | −71% |

**Walk-forward (Asia only):**

| Period | Trades | Sharpe | Win Rate | Max DD |
|--------|--------|--------|----------|--------|
| In-sample (Mar–Apr) | 23 | 8.02 | 87.0% | −$761 |
| Out-of-sample (May) | 8 | 24.90 | 100% | $0 |

**Bootstrap validation** (Asia only):

| Stat | Value |
|------|-------|
| P5 Sharpe | 3.99 |
| Median Sharpe | 11.49 |
| P95 Sharpe | 31.41 |
| % subsamples > 0 | **100%** |

Every random half-sample draw of the Asia-only strategy is profitable.

**Top Asia hours by Sharpe:**

| Hour UTC | Trades | Sharpe | Win Rate |
|----------|--------|--------|----------|
| 4 | 4 | 36.27 | 100% |
| 9 | 5 | 47.71 | 100% |
| 2 | 6 | 30.02 | 100% |

---

## Risk Factors and Limitations

**Sample:** 61 days covering a single BTC regime. Results are not guaranteed to persist across different volatility regimes or market structure changes.

**Capacity:** Kalshi bid-ask spreads are 3–5¢ per contract (12–20 bps), wider than the modeled 10 bps. Practical AUM ceiling is ~$50–100k before market impact materially erodes the edge (~$250–$500 notional per trade).

**Execution:** The model assumes perfect 5-minute delta hedge rebalance at 1 bps cost. In practice, BTC liquidity is deep but Kalshi option liquidity is thin; simultaneous option and BTC leg execution may incur additional slippage.

**Out-of-sample sample size:** The May hold-out contains 8–18 trades. The 100% win rate and zero drawdown in May are encouraging but statistically thin. The bootstrap validation partially addresses this by subsampling the full period.

**Capital utilization:** The Asia-only filter yields ~1 trade per 2 days on average; idle capital between opportunities reduces effective annualized returns at the portfolio level.

**Hours 14–15 UTC:** These two hours are consistently loss-making across both backtests and both halves of the walk-forward. The cause is not yet determined (possible market microstructure change or systematic adverse selection at those expiries).

**Model risk:** IV inversion uses a closed-form quadratic assuming digital Black-Scholes with r=0. This is appropriate for sub-hourly expiries but may not capture all option pricing dynamics.

---

## Conclusions

1. **Vol premium is real and highly significant.** Kalshi ATM IV exceeds 5-minute RV in 73% of observations with p ≈ 0. The premium is not constant — it varies structurally by UTC hour, and the hour-specific cointegration model significantly outperforms the pooled model.

2. **The premium is extractable via delta hedging.** The all-hours short-OTM strategy achieves Sharpe 7.15 over 61 days and holds out of sample (Sharpe 21.98 in May, though on a thin sample).

3. **The Asia session is the sweet spot.** Restricting to 01:00–10:59 UTC — consistent with the elevated per-hour log-premiums in the cointegration analysis — improves Sharpe by 47%, cuts max drawdown by 77%, and raises the win rate to 90.3%. Bootstrap P5 Sharpe of 3.99 with 100% profitable subsamples makes this the recommended operating mode.

4. **Capacity is the binding constraint.** The strategy is viable but small-scale. Market impact limits practical deployment to ~$50–100k AUM.

---

## Recommended Next Steps

1. **Extend the dataset** to 6+ months covering multiple BTC volatility regimes before drawing regime-generalizable conclusions.
2. **Model realistic spread curves** as a function of notional size to quantify the capacity ceiling precisely.
3. **Investigate hours 14–15 UTC** separately — determine whether the persistent losses reflect a market structure difference (thinner book, different participant mix) or random noise.
4. **Test multi-leg entries** across multiple OTM strikes simultaneously to increase capital utilization without increasing per-trade directional risk.
5. **Empirical rehedge slippage:** Measure actual BTC rebalance costs from live execution rather than using a fixed 1 bps model assumption.
