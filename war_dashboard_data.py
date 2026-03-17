"""
war_dashboard_data.py
Downloads and consolidates economic series impacted by the Iran conflict
into clean CSVs ready for Power BI import.
"""

import os
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

# ─── CONFIG ────────────────────────────────────────────────────────────────────
CONFLICT_START = "2025-10-01"
OUTPUT_DIR = Path("./output")
LOOKBACK_DAYS = 90

# ─── SERIES DEFINITIONS ────────────────────────────────────────────────────────
ENERGY_COMMODITIES = {
    "BZ=F":  "brent_usd",
    "CL=F":  "wti_usd",
    "NG=F":  "natgas_usd",
    "GC=F":  "gold_usd",
    "SI=F":  "silver_usd",
}

EQUITY_INDICES = {
    "^GSPC":     "sp500",
    "^IXIC":     "nasdaq",
    "^STOXX50E": "eurostoxx50",
    "^GDAXI":    "dax",
    "^FTSE":     "ftse100",
    "^N225":     "nikkei225",
    "000001.SS": "shanghai",
}

VOLATILITY_RATES_FX = {
    "^VIX":     "vix",
    "DX=F":     "dxy",      # DXY Dollar Index (DX-Y.NYB broken on Yahoo Finance)
    "EURUSD=X": "eurusd",
    "JPY=X":    "jpyusd",
}

FRED_SERIES = {
    "DGS10":                    "us10y_yield",
    "DGS2":                     "us2y_yield",
    "FEDFUNDS":                 "fed_funds_rate",
    "BAMLEMRECRPIGLFLCRPIOAS":  "embi_spread",
}

# ─── DATE HELPERS ──────────────────────────────────────────────────────────────
conflict_dt = pd.Timestamp(CONFLICT_START)
start_dt = conflict_dt - timedelta(days=LOOKBACK_DAYS)
end_dt = pd.Timestamp(datetime.today().date())

START_STR = start_dt.strftime("%Y-%m-%d")
END_STR   = end_dt.strftime("%Y-%m-%d")


def compute_metrics(series: pd.Series, col_name: str) -> pd.DataFrame:
    """Given a price series, compute indexed_d0 and delta_vs_d0_pct."""
    df = series.to_frame(name=col_name)
    df.index = pd.to_datetime(df.index)
    df = df[~df.index.duplicated()].sort_index()

    # D0 value: closest trading day on or after CONFLICT_START
    post = df.loc[df.index >= conflict_dt]
    if post.empty:
        d0_value = None
    else:
        d0_value = post.iloc[0][col_name]

    if d0_value and d0_value != 0:
        df[f"{col_name}_indexed_d0"]    = (df[col_name] / d0_value) * 100
        df[f"{col_name}_delta_vs_d0_pct"] = ((df[col_name] - d0_value) / d0_value) * 100
    else:
        df[f"{col_name}_indexed_d0"]      = None
        df[f"{col_name}_delta_vs_d0_pct"] = None

    return df


# ─── DOWNLOAD: yfinance ────────────────────────────────────────────────────────
def _fetch_single(ticker: str, col_name: str) -> pd.DataFrame | None:
    """Download a single ticker via yfinance and return a metrics DataFrame."""
    try:
        raw = yf.download(
            ticker,
            start=START_STR,
            end=END_STR,
            auto_adjust=True,
            progress=False,
        )
        if raw.empty:
            print(f"  [WARN] {ticker} ({col_name}): empty result")
            return None

        # yfinance >= 0.2.x returns MultiIndex columns even for single tickers
        if isinstance(raw.columns, pd.MultiIndex):
            series = raw[("Close", ticker)].dropna()
        else:
            series = raw["Close"].dropna()

        series.index = pd.to_datetime(series.index).tz_localize(None)
        return compute_metrics(series, col_name)
    except Exception as e:
        print(f"  [WARN] {ticker} ({col_name}): {e}")
        return None


def download_yfinance(tickers_map: dict) -> dict[str, pd.DataFrame]:
    """Download Close prices for a dict of {ticker: col_name}, one by one."""
    results = {}
    for ticker, col_name in tickers_map.items():
        df = _fetch_single(ticker, col_name)
        if df is not None:
            results[col_name] = df
    return results


# ─── DOWNLOAD: FRED ────────────────────────────────────────────────────────────
def download_fred(series_map: dict) -> dict[str, pd.DataFrame]:
    """Download series from FRED using fredapi."""
    try:
        from fredapi import Fred
    except ImportError:
        print("[ERROR] fredapi not installed. Run: pip install fredapi")
        return {}

    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        print("[ERROR] FRED_API_KEY environment variable not set. Skipping FRED series.")
        return {}

    fred = Fred(api_key=api_key)
    results = {}

    for fred_id, col_name in series_map.items():
        try:
            series = fred.get_series(
                fred_id,
                observation_start=START_STR,
                observation_end=END_STR,
            )
            series = series.dropna()
            series.index = pd.to_datetime(series.index).tz_localize(None)
            series.name = col_name
            results[col_name] = compute_metrics(series, col_name)
        except Exception as e:
            print(f"  [WARN] FRED/{fred_id} ({col_name}): {e}")

    return results


# ─── MERGE & SAVE ──────────────────────────────────────────────────────────────
def merge_group(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Outer-join all DataFrames on date index."""
    if not frames:
        return pd.DataFrame()
    dfs = list(frames.values())
    merged = dfs[0]
    for df in dfs[1:]:
        merged = merged.join(df, how="outer")
    merged = merged.sort_index()
    merged.index.name = "date"
    return merged


def save_csv(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path)
    print(f"  Saved: {path}  ({len(df)} rows)")


# ─── SUMMARY TABLE ─────────────────────────────────────────────────────────────
def print_summary(all_frames: dict[str, pd.DataFrame]):
    rows = []
    for col_name, df in all_frames.items():
        if col_name not in df.columns:
            continue
        s = df[col_name].dropna()
        if s.empty:
            continue
        delta_col = f"{col_name}_delta_vs_d0_pct"
        delta = df[delta_col].dropna().iloc[-1] if delta_col in df.columns and not df[delta_col].dropna().empty else None

        full_range = pd.date_range(s.index.min(), s.index.max(), freq="B")
        missing = len(full_range) - len(s)

        rows.append({
            "series":       col_name,
            "latest_value": round(s.iloc[-1], 4),
            "delta_d0_%":   f"{delta:+.2f}%" if delta is not None else "N/A",
            "first_date":   s.index.min().date(),
            "last_date":    s.index.max().date(),
            "missing_days": missing,
        })

    summary = pd.DataFrame(rows)
    if summary.empty:
        print("\n[No data to summarize]")
        return

    print("\n" + "=" * 80)
    print("SERIES SUMMARY")
    print("=" * 80)
    print(summary.to_string(index=False))
    print("=" * 80 + "\n")


# ─── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\nWAR BOARD — Data Download")
    print(f"  Conflict start : {CONFLICT_START}")
    print(f"  Download range : {START_STR}  to  {END_STR}")
    print(f"  Output folder  : {OUTPUT_DIR.resolve()}\n")

    # 1. Download
    print("Downloading energy & commodities...")
    energy_frames = download_yfinance(ENERGY_COMMODITIES)

    print("Downloading equity indices...")
    equity_frames = download_yfinance(EQUITY_INDICES)

    print("Downloading volatility, rates & FX...")
    vol_frames = download_yfinance(VOLATILITY_RATES_FX)

    print("Downloading FRED series...")
    fred_frames = download_fred(FRED_SERIES)

    # 2. Merge per group and save CSVs
    print("\nSaving group CSVs...")
    energy_df  = merge_group(energy_frames)
    equity_df  = merge_group(equity_frames)
    vol_df     = merge_group(vol_frames)
    fred_df    = merge_group(fred_frames)

    save_csv(energy_df, OUTPUT_DIR / "energy_commodities.csv")
    save_csv(equity_df, OUTPUT_DIR / "equity_indices.csv")
    save_csv(vol_df,    OUTPUT_DIR / "volatility_rates_fx.csv")
    if not fred_df.empty:
        save_csv(fred_df, OUTPUT_DIR / "us_rates_spreads.csv")
    else:
        print("  Skipped us_rates_spreads.csv (no FRED data)")

    # 3. Build master CSVs (wide)
    all_frames = {**energy_frames, **equity_frames, **vol_frames, **fred_frames}

    # Raw prices only
    price_cols = {k: df[[k]] for k, df in all_frames.items() if k in df.columns}
    wide_prices = merge_group(price_cols)
    save_csv(wide_prices, OUTPUT_DIR / "all_series_wide.csv")

    # Indexed columns only
    indexed_cols = {}
    for k, df in all_frames.items():
        idx_col = f"{k}_indexed_d0"
        if idx_col in df.columns:
            indexed_cols[idx_col] = df[[idx_col]]
    if indexed_cols:
        wide_indexed = merge_group({k: v for k, v in indexed_cols.items()})
        wide_indexed.index.name = "date"
        save_csv(wide_indexed, OUTPUT_DIR / "all_series_indexed.csv")

    # 4. Summary
    print_summary(all_frames)


if __name__ == "__main__":
    main()
