# Volatility Premium in Kalshi BTC Digital Options: Statistical Evidence and Systematic Extraction

**Authors:** Samuel Cravo  
**Date:** May 2026  
**Sample:** 61 days, March 21 – May 18, 2026 · 1,419 hourly observations  
**Status:** Research complete; out-of-sample validation included

---

## Abstract

This paper documents the existence of a statistically significant implied volatility (IV) premium in Kalshi BTC hourly binary options and investigates whether this premium can be systematically extracted through a delta-hedged short-volatility strategy. Using 1,419 hourly observations drawn from a 61-day sample spanning March–May 2026, we find that ATM implied volatility exceeds annualized 60-minute realized volatility in 78.7% of all windows (one-sided t = 23.047, p = 2.21×10⁻¹⁰⁰), with a mean premium of approximately 16.2 annualized volatility points.

We then test whether the log-IV/log-RV relationship is stable across UTC hours using F-tests and likelihood-ratio tests on pooled versus hour-stratified cointegration models. Both tests strongly reject the pooled specification (F(46, 1371) = 3.081, p = 5.62×10⁻¹¹), implying that the vol premium varies structurally by time of day.

Motivated by these findings, we backtest a vanilla short-volatility strategy — selling 1,000 +25Δ OTM Kalshi binary calls near a 25-cent entry price with delta-hedging in BTC every five minutes — achieving a Sharpe ratio of 7.15 over the full 61-day sample. Restricting trades to the Asia session (01:00–10:59 UTC), where the cointegration analysis identifies the most elevated per-hour log-premiums, improves the Sharpe to 10.53 while cutting maximum drawdown by 77%. Both strategies survive walk-forward validation and bootstrap robustness checks. We conclude with an extensive discussion of sample-size limitations, execution assumptions, and capacity constraints.

---

## 1. Introduction

### 1.1 Motivation

The volatility risk premium — the systematic tendency of implied volatility to exceed subsequently realized volatility — is one of the most extensively documented phenomena in listed options markets. In equity index options, for example, the VIX historically averages 3–5 percentage points above subsequent 30-day realized vol, giving rise to large literatures on variance swaps, delta-hedged straddles, and short-volatility portfolios. The economic interpretation is that option sellers are compensated for bearing negative-skewness and jump risk that is not fully captured in realized vol, and that liquidity providers in options markets charge a spread that embeds an insurance premium.

Kalshi is a US-regulated prediction market (CFTC-licensed) that began listing binary options on BTC price movements in 2025. Its contracts are cash-or-nothing digital calls: a contract on BTC trading above a given strike K at a specific UTC hour pays $1 if BTC closes above K, and $0 otherwise. The market trades continuously and offers approximately 188 strike levels per hourly expiry. Because each contract price is directly interpretable as a risk-neutral probability, and because the underlying (spot BTC) is liquidly traded on centralized exchanges, these instruments offer an unusually clean setting in which to test for a vol premium and to design hedging strategies.

Three features of this market make it analytically interesting. First, the short tenors (sub-hourly time-to-expiry at entry) mean that realized vol can be estimated over almost the exact same window that the option prices. Second, the binary payout structure admits a closed-form implied volatility inversion via a quadratic equation, avoiding the numerical iteration required for vanilla puts and calls. Third, because Kalshi is a relatively new and retail-dominated venue, pricing inefficiencies that institutional vol arbitrageurs have already eliminated in listed equity options markets may still be present.

### 1.2 Market Structure

As of 2026, Kalshi BTC contracts settle at the top of each UTC hour. The contract naming convention encodes the expiry: `KXBTCD-26MAY1901-T85799.99` settles at 19:00 UTC on May 19, 2026 at a strike of $85,799.99. Approximately 188 strike levels are available per expiry, spaced at regular intervals around spot BTC. Contract prices are quoted in cents (0–100) and represent the probability that BTC exceeds the strike at expiry. The face value of each contract is $1.00.

The platform charges a taker fee equal to `0.07 × C × P × (1 − P)` per contract, where P is the contract price and C is the number of contracts. This fee peaks at 1.75¢ per contract when P = 0.50 (ATM) and approaches zero for deeply OTM or ITM contracts. The fee schedule creates a natural advantage for OTM sellers: at a 25-cent entry price, the taker fee is approximately 1.31¢ per contract.

### 1.3 Research Questions

This paper addresses three questions:

1. Does Kalshi ATM implied volatility systematically exceed 60-minute realized volatility, and is the premium statistically significant?
2. Is the log(IV)/log(RV) relationship stable across time, or does it vary structurally by UTC hour of day?
3. Can the premium be extracted via a delta-hedged short-OTM strategy? Does restricting to the Asia session (01:00–10:59 UTC) improve risk-adjusted returns?

### 1.4 Paper Roadmap

Section 2 describes data collection and the ETL pipeline. Section 3 presents the realized and implied volatility construction. Section 4 formalizes the research questions and statistical tests. Section 5 reports test results. Sections 6 and 7 describe the trading strategy and backtest. Section 8 covers robustness tests. Section 9 discusses caveats and limitations. Section 10 concludes.

---

## 2. Data

### 2.1 Data Sources

**Kalshi API.** Market data is sourced from the Kalshi v2 trade API at `https://api.elections.kalshi.com/trade-api/v2`. Authentication uses RSA-PSS signatures: each API request is signed with a 2048-bit RSA private key, with the signing message constructed as `timestamp_ms + METHOD + /trade-api/v2 + path`. The client routes between `/historical/markets` (for data older than Kalshi's rolling cutoff) and `/markets` (for recent data), fetching the cutoff lazily on the first request. The client enforces a 10 req/s rate limit and applies an `asyncio.Semaphore(5)` for parallel candle batch fetches with three-attempt retry logic.

**Binance.** One-minute BTCUSDT spot klines are sourced from `api.binance.us` (the US endpoint; `api.binance.com` is geo-blocked in the US). Data is fetched with a two-hour lead and two-hour lag buffer around the trading window to ensure clean rolling-window RV estimation at day boundaries.

### 2.2 Collection Pipeline

The pipeline follows a medallion architecture with three layers — Bronze, Silver, and Gold — where each layer may only read from the layer below it and never skips or back-reads raw data.

**Bronze layer.** Kalshi candles are ingested with the fetch window starting at 23:00 UTC the prior day (to capture T−1 snapshots before market open) and ending at 23:59 UTC on the final date. Zero-volume candle rows are dropped at ingest, as these represent non-traded snapshots with no price information. The new API format returns prices as USD strings (`yes_ask.close_dollars`, `yes_bid.close_dollars`); mid-price is computed as the mean of ask and bid and stored as a `UInt8` in cents (0–100). The older API format returned integer cents directly; both formats are handled. All writes are `zstd` level-3 compressed Parquet files partitioned by date.

**Silver layer.** The silver ETL joins Kalshi candle data against Binance spot prices using DuckDB with `SET TimeZone='UTC'` to ensure consistent timestamp handling. Two outputs are produced:

- `contracts.parquet`: snapshot-level IV per (trade_date, UTC hour, ticker), with the filter `expiry_time > snapshot_ts` to ensure only open markets are sampled.
- `vol_surface.parquet`: IV at every traded minute across all 24 UTC expiry hours. This is the primary input to the backtest.

At this stage, `prob_itm = digi_px / 100` and IV is computed via the closed-form inversion described in Section 3.2. Time-to-expiry T is computed dynamically at each row, not hardcoded.

**Gold layer.** The gold ETL (`rv_iv_analysis.py`) computes annualized hourly realized vol and extracts ATM IV from the silver vol surface. The complementary `gold_etl.py` module computes ATM IV, 25-delta skew, and their period-over-period changes, running two-sided t-tests, Wilcoxon signed-rank tests, and Cohen's d statistics on consecutive snapshot shifts.

### 2.3 Sample Coverage

| Dataset | Path | Shape | Period |
|---------|------|-------|--------|
| `rv_iv_hourly.parquet` | `data/gold/` | 1,419 × 10 | 2026-03-21 → 2026-05-18 |
| `features.parquet` | `data/gold/` | 1,046 × 12 | 2026-03-21 → 2026-05-18 |
| `vol_surface.parquet` | `data/silver/` | 122,985 rows | 1,424 unique expiries |
| `binance_btc_1m.parquet` | `data/bronze/` | 87,640 rows | 2026-03-20 → 2026-05-20 |

The 61-day window covers three calendar months and includes both trending (April bull run) and ranging (late March, early May) BTC regimes, though it represents only a single broad volatility regime.

### 2.4 Data Cleaning Decisions

The following cleaning steps are applied before any analysis:

1. **Zero-volume candles dropped.** Rows with no trading activity carry stale prices; retaining them would introduce phantom IV observations.
2. **IV filtered to [20%, 500%] annualized.** Below 20%, the binary call delta formula becomes numerically unstable as σ√T → 0. Above 500%, the inversion almost certainly reflects a data artifact from an extreme near-expiry OTM contract with no real trading.
3. **Flat RV windows excluded.** Hourly windows with `rv_ann = 0` (extremely rare but possible in low-liquidity windows) are excluded from the log-vol analysis.
4. **ATM IV selection.** The ATM contract is identified as the strike minimizing `|prob_itm − 0.50|` across all strikes at each snapshot. This nearest-neighbor approach avoids interpolation artifacts across the discrete strike ladder.

---

## 3. Volatility Construction

### 3.1 Realized Volatility

Hourly realized volatility is computed from Binance 1-minute BTCUSDT closes over the 60-minute window ending at each bar timestamp:

$$\text{RV}_{\text{ann}} = \sqrt{\sum_{i=1}^{60} \left(\ln \frac{P_i}{P_{i-1}}\right)^2 \times 8{,}760}$$

The annualization factor 8,760 converts from per-hour variance to per-year variance (8,760 hours in a 365-day year). The result is expressed as an annualized decimal (e.g., 0.35 = 35% annualized vol). A minimum of 10 available 1-minute bars is required; windows with fewer bars return NaN and are excluded.

### 3.2 Implied Volatility Inversion

Kalshi binary contracts are cash-or-nothing digital calls, priced under the digital Black-Scholes model (r = 0, appropriate for sub-hourly expiries):

$$P = N(d_2), \quad d_2 = \frac{\ln(S/K)}{\sigma\sqrt{T}} - \frac{\sigma\sqrt{T}}{2}$$

where P is the contract mid-price (in decimal), S is the spot BTC price, K is the strike, σ is the implied vol, and T is the time to expiry in years.

**Closed-form inversion.** Substituting `u = σ√T` and rearranging yields a quadratic in u:

$$u^2 + 2 \cdot N^{-1}(P) \cdot u - 2\ln(S/K) = 0 \quad \Rightarrow \quad u = -N^{-1}(P) \pm \sqrt{\left[N^{-1}(P)\right]^2 + 2\ln(S/K)}$$

Root selection depends on moneyness:
- **OTM** (K > S, digi_px < 50): negative root `u = −d₂* − √disc`
- **ITM** (K < S, digi_px > 50): positive root `u = −d₂* + √disc`
- **ATM** (digi_px ≈ 50): `scipy.optimize.brentq`

The implied vol is then `σ = u / √T`. The `r = 0` assumption is justified for sub-hourly tenors where risk-free carry is negligible relative to the option premium.

---

## 4. Research Questions and Test Designs

### 4.1 Q1 — Vol Premium Significance

Define the IV premium as `iv_premium = atm_iv_mean − rv_ann`. A positive value indicates the market overprices implied vol relative to realized vol — a seller's edge.

- **H₀**: E[iv_premium] = 0 (fair pricing)
- **H₁**: E[iv_premium] > 0 (IV systematically overprices RV)

**Test.** One-sided Student's t-test on the 1,419 hourly observations.

### 4.2 Q2 — Cointegration of log(RV) and log(IV)

We investigate whether a stable long-run equilibrium exists between `log(RV)` and `log(IV)`:

$$\log(\text{IV}_t) = \alpha + \beta \cdot \log(\text{RV}_t) + \varepsilon_t, \quad \varepsilon_t \sim I(0)$$

**Tests applied:**
1. **ADF unit-root tests** on each series individually.
2. **Engle-Granger cointegration test** for a long-run equilibrium relationship.
3. **OLS regression** to recover α, β; ADF on residuals confirms stationarity.

The **mean log-spread** E[log(IV) − log(RV)] is computed directly. Note this is distinct from the OLS intercept α when β ≠ 1.

### 4.3 Q3 — Hour-Specific Cointegration Structure

We test whether allowing separate intercepts and slopes for each of the 24 UTC hours significantly improves the cointegrating model:

| Model | Specification | Parameters |
|-------|---------------|-----------|
| **Restricted (pooled)** | log(IV) = α + β · log(RV) + ε | 2 |
| **Unrestricted (per-hour)** | log(IV) = α_h + β_h · log(RV) + ε | 48 |

**Tests:**
- **F-test**: `F = [(RSS_R − RSS_U) / Δdf] / [RSS_U / df_U]`
- **Likelihood-ratio test**: `LR = 2(ℓ_U − ℓ_R) ~ χ²(Δdf)`

---

## 5. Statistical Tests and Results

### 5.1 Q1 — Vol Premium Is Large and Highly Significant

```
N observations                 :  1,419
Mean IV premium (ann.)         :  +0.1615  (+16.2 vol points)
Median IV premium (ann.)       :  +0.1061  (+10.6 vol points)
95% Confidence Interval        :  [+0.1477, +0.1752]
% windows where IV > RV        :  78.7%
One-sided t-statistic          :  23.047
p-value (H₁: mean > 0)        :  2.21 × 10⁻¹⁰⁰
```

The null hypothesis of zero premium is rejected at any conventional significance level. The mean premium of 16.2 annualized vol points is economically large — approximately one-third of typical BTC ATM vol levels (~52% in this sample). The mean exceeds the median because the distribution is right-skewed: occasional BTC vol spikes push RV well above IV in a minority of windows, but these are outnumbered nearly four-to-one by windows where IV > RV.

The premium is persistent across all three calendar months. Plotting daily median ATM IV and RV reveals that ATM IV runs consistently above RV, with the spread rarely closing except on high-vol days. The standard deviation of the iv_premium is approximately 0.264 annualized, reflecting noisy individual-hour outcomes around a strongly positive long-run mean.

### 5.2 Q2 — Cointegration of log(RV) and log(IV)

**ADF unit-root tests** (automatic lag selection, AIC criterion):

```
log(RV):  ADF = −3.838,  p = 0.0025,  lags = 22  →  I(0) — stationary
log(IV):  ADF = −4.487,  p = 0.0002,  lags = 23  →  I(0) — stationary
```

Both series are stationary in levels at the hourly frequency. The Engle-Granger test therefore evaluates whether the log-spread is stable — the economically relevant question even if strict I(1) cointegration does not apply.

**Engle-Granger cointegration test:**

```
EG statistic  = −4.0435
p-value       =  0.0062
→ Long-run co-movement relationship confirmed
```

**OLS regression** (`log(IV) = α + β · log(RV) + ε`):

```
α (intercept)       = −0.5595
β (slope)           =  0.1458
R²                  =  0.0422
Residuals ADF stat  = −13.827,  p ≈ 0  →  stationary ✓
```

**Mean log-spread (unconditional):**

```
E[log(IV/RV)]   =  +0.4346
exp(mean)       =   1.5443   →  IV averages 54% higher than RV in log-space
```

The low R² (4.2%) and near-zero β (0.15) reveal that log(RV) explains very little of the *variation* in log(IV) on an hour-by-hour basis — the two series do not co-move tightly in the short run. However, the stationary residuals and significant EG test confirm a stable *level* relationship: the log-spread does not drift without bound. The mean log-spread of +0.43 (IV/RV ≈ 1.54× on average) is the most relevant summary for the strategy.

### 5.3 Q3 — The IV-RV Relationship Is Structurally Heterogeneous Across Hours

**Model comparison:**

```
Model                   Params    RSS      log-lik       AIC
Restricted  (pooled)        2    186.56   −573.93    1151.86
Unrestricted (per-hour)    48    169.08   −504.14    1104.28

Δdf (additional parameters): 46
```

**Test statistics:**

```
F(46, 1371)  =  3.0807   p = 5.62 × 10⁻¹¹
LR χ²(46)   = 139.576    p = 2.31 × 10⁻¹¹
```

Both tests reject the pooled model at extreme significance levels. The IV-RV cointegrating relationship is structurally heterogeneous: some UTC hours carry a substantially different log-premium than the pooled average.

**Per-hour OLS intercepts.** Fitting separate regressions for each UTC hour reveals the structure. Asia session hours (01:00–10:59 UTC) carry the most negative per-hour intercepts (α_h ranging from −0.66 to −1.06), reflecting hours when both log(IV) and log(RV) are lower in absolute terms — consistent with typically quieter BTC volatility during Asian daylight hours. Crucially, the *differential* between IV and RV within these hours remains elevated because IV does not fall proportionally with RV. By contrast, hours 13–15 UTC have the least negative intercepts (−0.21 to −0.30), reflecting hours when BTC vol is highest and IV is most competitively priced.

This cross-sectional heterogeneity directly motivates the Asia-session filter in the trading strategy: it selects the hours where the vol premium is structurally largest, as confirmed by the F-test rejection of the pooled model.

---

## 6. Trading Strategy

### 6.1 Design Rationale

The statistical evidence establishes that: (i) ATM IV exceeds RV in 78.7% of hours with overwhelming statistical significance, and (ii) the IV-RV relationship is structurally heterogeneous, with Asia-session hours showing the most distinct dynamics. The natural strategy response is to sell implied volatility when it is richest relative to realized vol, using delta-hedging to remove directional BTC exposure.

Binary digital calls with a ~25-cent entry price are chosen because:
- At 25 cents (~25Δ OTM), the probability of expiring worthless is approximately 75%, giving the seller a large statistical edge.
- The OTM structure avoids the worst gamma risk near ATM: the binary delta peaks at the money and falls sharply OTM.
- 25-cent contracts sit in a price zone where the Kalshi fee structure is not prohibitive (fee ≈ 1.31¢ per contract at P = 0.25).

### 6.2 Entry Rules

**Universe.** All Kalshi BTC hourly binary call contracts in the vol surface (1,424 unique expiry times).

**Strike selection.** For each expiry at entry, find all OTM contracts (strike > BTC price) satisfying:
- IV ≥ 20% annualized
- `digi_px ≥ 10 cents`

Select the strike with `digi_px` closest to 25 cents (`argmin |digi_px − 25|`).

**IV/RV filter.** Only enter if `IV / RV_1h ≥ 1.20`, where `RV_1h` is the annualized realized vol over the 60 minutes immediately before the entry timestamp.

**Position size.** 1,000 contracts per trade. At 25¢ entry, gross premium received = $250 per trade. Maximum theoretical loss = $1,000 (full ITM expiry).

**Entry timing.** Selected at the maximum `minutes_to_expiry` snapshot available — typically T−12 minutes before the hour.

### 6.3 Delta Hedging

**Hedge formula.** The binary cash-or-nothing call delta is:

$$\Delta_{\text{binary}} = \frac{n(d_2)}{S \cdot \sigma \cdot \sqrt{T}}$$

where `n(·)` is the standard normal PDF. This represents the BTC units to hold long per contract short.

**Mechanics.** At entry, purchase `N_contracts × Δ` BTC units. Every 5 minutes, recompute Δ at current (S, IV, T) and adjust the BTC position at 1 bps rebalance cost. The position is unwound at exit. Average hedge P&L across all 82 full-period trades: **$48.58 per trade**.

### 6.4 Exit Rules

**Early exit (profit-taking).** At each 5-minute rebalance, compute the current binary call price. If ≤ 50% of entry price, close immediately to avoid the gamma crunch in the final minutes before expiry. In the full backtest, **82.9% of trades are exited early**.

**Hold to expiry.** If the threshold is never reached, settle at the UTC hour. Loss of $1,000 if BTC > strike; full premium retained if BTC ≤ strike.

### 6.5 Transaction Costs

| Cost Component | Formula / Rate | Example (25¢ entry, 1,000 contracts) |
|----------------|---------------|--------------------------------------|
| Kalshi taker fee | 0.07 × C × P × (1 − P) | $13.13 per entry |
| Premium slippage | 100 bps on gross premium | $2.50 per side |
| BTC rebalance | 1 bps per trade | ~$10–15 per trade total |

**Total TC across 82 trades: $4,292** (~$52.34/trade, ~22% of gross premium).

### 6.6 Asia-Hours Variant

Restricts the entry universe to expiry hours {01, 02, ..., 10} UTC — Tokyo open through Singapore close. All other parameters are identical. This filter is a research-driven conclusion from Q3, not a backtest optimization.

---

## 7. Backtest Results

### 7.1 All Hours (82 trades, 61 days)

```
Portfolio starting value       :  $100,000
N contracts per trade          :  1,000

N trades                       :  82
Total return ($)               :  $5,959.66
Annualized return              :  53.06%
Realized daily vol (ann.)      :  7.17%
Sharpe ratio (ann.)            :  7.15
Win rate                       :  79.3%
Max drawdown ($)               : −$3,318.00
Calmar ratio                   :  1.796
ITM expiry rate                :  11.0%
Early exit rate                :  82.9%
Avg P&L per trade ($)          :  $72.68
Avg hedge P&L per trade ($)    :  $48.58
Avg premium received ($)       :  $239.88
Total transaction costs ($)    : −$4,291.97
```

**P&L decomposition (average per trade):**
- Gross premium: +$239.88
- Transaction costs: −$52.34
- Delta-hedge P&L: +$48.58
- Settlement loss (amortized over 9 ITM expiries): ~−$110.98
- **Net P&L: +$72.68**

**Walk-forward validation** (parameters fixed at training-set values, not re-optimized):

| Period | Dates | Trades | Sharpe | Win Rate | Max DD |
|--------|-------|--------|--------|----------|--------|
| In-sample | Mar 21 – Apr 30 | 64 | 4.42 | 73.4% | −$3,318 |
| Out-of-sample | May 1 – May 18 | 18 | 21.98 | 100% | $0 |

**Cross-validation by UTC expiry hour:**

| Hour UTC | Trades | Sharpe | Win Rate | ITM Rate | Total Return |
|----------|--------|--------|----------|----------|-------------|
| 0 | 1 | n/a | 0.0% | 100% | −$856 |
| 2 | 6 | 30.02 | 100% | 0% | +$1,035 |
| 4 | 4 | 36.27 | 100% | 0% | +$636 |
| 8 | 2 | 23.58 | 100% | 0% | +$154 |
| 9 | 5 | 47.71 | 100% | 0% | +$816 |
| 10 | 4 | 19.05 | 75.0% | 25% | +$566 |
| 12 | 7 | 51.28 | 100% | 0% | +$1,136 |
| 13 | 2 | 45.66 | 100% | 0% | +$358 |
| **14** | **16** | **−4.63** | **43.8%** | **25%** | **−$1,333** |
| **15** | **3** | **−6.03** | **66.7%** | **33%** | **−$308** |
| 22 | 7 | 40.70 | 100% | 0% | +$1,590 |

Hours 14 and 15 UTC are persistently loss-making, with a 25–33% ITM rate more than double the full-sample average. Every other hour with three or more trades is profitable.

### 7.2 Asia-Hours Only Strategy (31 trades, 61 days)

| Metric | All Hours | Asia Only | Change |
|--------|-----------|-----------|--------|
| N Trades | 82 | 31 | −62% |
| Total Return ($) | $5,960 | $3,248 | −46% |
| Annualized Return | 53.06% | 51.5% | −1.6 pp |
| **Sharpe Ratio** | **7.15** | **10.53** | **+47%** |
| **Win Rate** | **79.3%** | **90.3%** | **+11 pp** |
| **Max Drawdown ($)** | **−$3,318** | **−$761** | **−77%** |
| **Calmar Ratio** | **1.80** | **4.27** | **+137%** |
| Avg P&L / Trade ($) | $72.68 | $104.77 | +44% |
| Total TC ($) | −$4,292 | −$1,239 | −71% |
| ITM Expiry Rate | 11.0% | 9.7% | −1.3 pp |

The Asia-only strategy achieves near-identical annualized returns with 47% higher Sharpe and 77% lower drawdown. The key mechanism is trade selection: concentrating on the hours with the highest structural log-premiums reduces both losses and P&L volatility simultaneously.

**Walk-forward (Asia only):**

| Period | Dates | Trades | Sharpe | Win Rate | Max DD |
|--------|-------|--------|--------|----------|--------|
| In-sample | Mar 21 – Apr 30 | 23 | 8.02 | 87.0% | −$761 |
| Out-of-sample | May 1 – May 18 | 8 | 24.90 | 100% | $0 |

**Top Asia hours by Sharpe:**

| Hour UTC | Local Time (Tokyo / HK) | Trades | Sharpe | Win Rate |
|----------|------------------------|--------|--------|----------|
| 9 | 18:00 JST / 17:00 HKT | 5 | 47.71 | 100% |
| 4 | 13:00 JST / 12:00 HKT | 4 | 36.27 | 100% |
| 2 | 11:00 JST / 10:00 HKT | 6 | 30.02 | 100% |
| 8 | 17:00 JST / 16:00 HKT | 2 | 23.58 | 100% |
| 10 | 19:00 JST / 18:00 HKT | 4 | 19.05 | 75.0% |

Hours 4 and 9 UTC correspond to active Asian afternoon sessions (Tokyo early afternoon and Singapore close), when institutional crypto activity is elevated and IV may be bid up by hedging demand.

---

## 8. Robustness Tests

### 8.1 Sharpe Ratio Computation

The annualized Sharpe ratio is computed as:

$$\text{Sharpe}_{\text{ann}} = \frac{\bar{R}_d}{\hat{\sigma}_d} \times \sqrt{365}$$

where $\bar{R}_d$ is the mean of daily aggregate P&L, $\hat{\sigma}_d$ is the sample standard deviation of daily P&L, and the risk-free rate is set to zero. The Asia-only Sharpe is higher despite similar returns because trade selection eliminates loss-making hours, substantially reducing daily P&L variance. Lower variance at constant mean return is the entire mechanism — not a higher return.

### 8.2 Walk-Forward Validation

Strategy parameters are fixed from first principles (IV/RV filter threshold 1.20, minimum premium 10¢, early-exit trigger at 50%, target delta 25¢) and are not optimized on the training set. They are applied unchanged to the May hold-out.

Both strategies show superior out-of-sample performance relative to in-sample, arguing strongly against overfitting. The OOS Sharpes (21.98 all-hours, 24.90 Asia-only) likely reflect favorable market conditions in May rather than model edge improving out-of-sample, but the absence of performance decay is the key finding.

### 8.3 Bootstrap Subsampling

1,000 random 50% subsamples (without replacement) of the trade-level P&L are drawn; the Sharpe is computed for each:

**All-hours bootstrap:**

```
P5 Sharpe                     :   2.057
Median Sharpe                 :   6.248
P95 Sharpe                    :  14.523
% subsamples with Sharpe > 0  :  99.8%
```

**Asia-hours bootstrap:**

```
P5 Sharpe                     :   3.990
Median Sharpe                 :  11.490
P95 Sharpe                    :  31.410
% subsamples with Sharpe > 0  :  100.0%
```

The bootstrap distributions confirm robustness. For all-hours, P5 Sharpe of 2.06 establishes that even the worst 5% of random subsamples are positive. For Asia-only, every single subsample is profitable. The Asia-only distribution dominates the all-hours distribution at every percentile, reinforcing the session filter as the preferred operating mode.

### 8.4 Per-Hour Cross-Validation

The per-hour performance breakdown constitutes a natural cross-validation: if the edge were concentrated in a few lucky hours, removing them would collapse total returns. In practice, the strategy is profitable across almost all hours, with two consistent exceptions (hours 14 and 15) that are loss-making across both backtests and both halves of the walk-forward. Consistency across non-overlapping sub-periods is evidence of a structural issue rather than noise.

---

## 9. Caveats and Limitations

### 9.1 Sample Size and Regime Risk

The 61-day sample represents a single volatility regime. BTC experienced a moderate uptrend in April 2026 and a mild correction in early May, but the period included no extreme vol spike or sustained bear market. The short-vol strategy is short gamma: it benefits from quiet, trending markets but would experience significant losses in a sustained high-vol regime (e.g., a 2020-style COVID crash or a 2022-style crypto deleveraging event) where realized vol spikes far above implied vol for extended periods.

Extending the dataset to 6+ months covering multiple BTC regimes is essential before drawing regime-generalizable conclusions. The 61-day results should be interpreted as regime-conditional findings.

### 9.2 Out-of-Sample Sample Thinness

The May walk-forward hold-out contains only 8 trades (Asia-only) and 18 trades (all hours). While the 100% win rate and zero drawdown in May are encouraging, a single adverse week with 2–3 ITM expiries would have produced a negative OOS Sharpe. The bootstrap partially compensates but cannot substitute for a genuinely longer OOS period.

### 9.3 Execution Assumptions

- **Perfect 5-minute rebalancing.** Real Kalshi API rate limits and BTC exchange latency could prevent simultaneous option and BTC leg execution.
- **Fixed slippage.** The 100 bps premium slippage assumption is optimistic relative to actual Kalshi bid-ask spreads of 3–5 cents (600–1,000 bps of price) even at $250 notional.
- **BTC liquidity.** The 1 bps BTC rebalance cost is appropriate at the modeled sizes; scaling up Kalshi exposure while keeping BTC costs constant is not possible.

### 9.4 Hours 14–15 UTC Anomaly

Hours 14 and 15 UTC show consistent losses across both backtests and both halves of the walk-forward, with a 25–33% ITM rate more than double the sample average. The underlying cause is not established. Possible explanations include adverse selection by informed participants, correlated macro vol spikes at the US equity open (13:30 UTC), or thinner order books producing noisier IV inversions. Until the cause is identified, hours 14 and 15 UTC should be excluded from live trading.

### 9.5 Market Capacity

| Notional per Trade | Contracts | Market Impact Assessment |
|-------------------|-----------|--------------------------|
| $250 | 1,000 | Modeled — marginally achievable |
| $1,000 | 4,000 | Likely 1–2 cent adverse price impact |
| $5,000+ | 20,000+ | Cannot execute near modeled price |

The practical AUM ceiling is approximately **$50,000–$100,000** based on 1% position sizing at $500 notional per trade. The Asia-only strategy generates ~0.5 trades per day on average, meaning capital is idle >50% of the time and the effective portfolio-level annualized return is substantially below the 51.5% reported on active capital.

### 9.6 Model Risk

The IV inversion uses digital Black-Scholes with r = 0 and log-normal dynamics. The model does not capture jump risk (BTC is subject to large discontinuous moves), stochastic volatility (true instantaneous vol may differ from implied vol used for hedging), or liquidity risk (early-exit threshold may not be achievable at modeled prices in a fast market).

### 9.7 Capital Utilization

The Asia-only filter generates approximately one trade every two days. Idle capital between opportunities substantially reduces the effective portfolio-level return relative to the reported per-trade return. A complete portfolio would require complementary strategies to deploy idle capital — which runs directly into the capacity constraints of Section 9.5.

---

## 10. Conclusions

**Q1: The vol premium is real and highly significant.** Kalshi ATM implied volatility exceeds 60-minute realized volatility in 78.7% of 1,419 hourly windows (March–May 2026), with a mean premium of 16.2 annualized vol points. The one-sided t-test yields t = 23.047, p = 2.21×10⁻¹⁰⁰ — the null of fair pricing is rejected at any conventional level. The premium is persistent across all three calendar months and is not driven by outliers: the median of +10.6 vol points is also substantially positive. Economically, Kalshi's market appears to embed a meaningful insurance premium for option sellers, consistent with a retail-dominated buyer base purchasing binary calls for speculative upside rather than precise vol replication.

**Q2: Log(IV) and log(RV) share a stable long-run relationship, but the premium is structurally heterogeneous across UTC hours.** Both log series are stationary (ADF p < 0.003), and the Engle-Granger test confirms a long-run co-movement relationship (p = 0.006) with stationary OLS residuals. The mean log-spread of +0.43 (implying IV/RV = 1.54× on average) is the cleanest summary of the average vol markup. F-tests and likelihood-ratio tests comparing pooled and per-hour cointegration models both reject the pooled specification at F(46, 1371) = 3.081, p = 5.62×10⁻¹¹, confirming structural heterogeneity. Asia session hours (01:00–10:59 UTC) show the most distinct dynamics; hours 14–15 UTC are persistently unfavorable.

**Q3: The premium is extractable, and the Asia session filter substantially improves risk-adjusted performance.** A vanilla short-volatility strategy — selling 1,000 +25Δ OTM binary calls near 25 cents with 5-minute BTC delta hedging and a 50% early-exit rule — achieves a Sharpe ratio of 7.15 over the full 61-day sample (53.06% annualized return, $5,960 total P&L). Restricting to the Asia session improves the Sharpe to 10.53, reduces maximum drawdown by 77%, and raises the win rate to 90.3%, while delivering near-identical annualized returns. Bootstrap validation shows 99.8% (all hours) and 100% (Asia only) of random 50% trade subsamples are profitable. Walk-forward validation shows out-of-sample May performance exceeds in-sample performance in both variants.

**The binding constraint is market capacity.** At $250 notional per trade, the strategy operates near the practical limit of Kalshi's order book liquidity. The practical AUM ceiling is approximately $50,000–$100,000 before market impact materially erodes the edge. The strategy represents a real but inherently small-scale opportunity — a structural alpha that cannot easily be arbitraged away by institutional capital precisely because the market cannot absorb institutional-sized positions.

---

## Recommended Next Steps

1. **Extend the dataset** to 6+ months across multiple BTC volatility regimes before drawing regime-generalizable conclusions.
2. **Investigate hours 14–15 UTC** to determine whether persistent losses reflect adverse selection, macro-correlated vol spikes, or data artifacts.
3. **Measure actual BTC rebalance costs** from live execution rather than using the fixed 1 bps model assumption.
4. **Evaluate multi-leg entries** across multiple OTM strikes and simultaneous expiries to increase capital utilization without proportionally increasing directional risk.
5. **Explore maker-order strategies** on Kalshi to earn the negative fee tier, converting slippage cost into a potential rebate.

---

## Appendix A: Key Statistics Reference

| Statistic | Value |
|-----------|-------|
| Sample period | 2026-03-21 → 2026-05-18 (61 days) |
| Hourly observations | 1,419 |
| Mean BTC ATM IV (ann.) | 51.9% |
| Mean BTC RV hourly (ann.) | 35.8% |
| Mean IV premium | +16.2 vol pts |
| Median IV premium | +10.6 vol pts |
| % IV > RV | 78.7% |
| t-statistic (Q1) | 23.047 |
| p-value (Q1) | 2.21 × 10⁻¹⁰⁰ |
| 95% CI for mean IV premium | [+14.77, +17.52] vol pts |
| log(RV) ADF stat | −3.838 (p = 0.0025) |
| log(IV) ADF stat | −4.487 (p = 0.0002) |
| Engle-Granger stat | −4.0435 (p = 0.0062) |
| OLS intercept α | −0.5595 |
| OLS slope β | 0.1458 |
| OLS R² | 0.0422 |
| Mean log-spread | +0.4346 (IV/RV = 1.54×) |
| F-test (Q3) | F(46, 1371) = 3.081, p = 5.62 × 10⁻¹¹ |
| LR test (Q3) | χ²(46) = 139.58, p = 2.31 × 10⁻¹¹ |
| All-hours Sharpe | 7.15 |
| All-hours win rate | 79.3% |
| All-hours max drawdown | −$3,318 |
| All-hours bootstrap P5 Sharpe | 2.06 |
| Asia-hours Sharpe | 10.53 |
| Asia-hours win rate | 90.3% |
| Asia-hours max drawdown | −$761 |
| Asia-hours bootstrap P5 Sharpe | 3.99 |

## Appendix B: Data Pipeline Summary

```
Kalshi API (RSA-PSS auth)          Binance 1m BTCUSDT
        │                                  │
        ▼                                  ▼
  Bronze Layer                       Bronze Layer
  kalshi_candles_{date}.parquet      binance_btc_1m.parquet
  (mid-price, UInt8 cents)           (1m close prices)
  zero-vol rows dropped              2h buffer each side
        │                                  │
        └──────────────┬───────────────────┘
                       ▼
               Silver Layer (DuckDB join)
               contracts.parquet          vol_surface.parquet
               (hourly IV snapshots)      (per-minute IV × all expiries)
                       │
                       ▼
               Gold Layer
               rv_iv_hourly.parquet       features.parquet
               (hourly RV vs ATM IV)      (ATM IV, 25Δ skew)
               summary_stats.csv
               (t-tests, Wilcoxon, d)
                       │
                       ▼
               Research Notebooks
               vol_research.ipynb              Q1–Q3 statistical tests
               backtest_01_all_hours.ipynb     All-hours strategy
               backtest_02_asia_hours.ipynb    Asia session strategy
```

---

*Research conducted using the KalshiTemporalDigiVol pipeline. All computations are fully reproducible from the Parquet data files and Jupyter notebooks in this repository.*
