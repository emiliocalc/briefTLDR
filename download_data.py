"""
Financial Advisor — Data Downloader
Descarga y cachea datos de mercado relevantes para "The Global Compounder"
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta

# ── Configuración ────────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

TODAY = datetime.today().strftime("%Y-%m-%d")
START_LONG  = "2018-01-01"   # historial suficiente para walk-forward
START_SHORT = "2024-01-01"   # datos recientes para contexto

HOLDINGS = ['VT', 'AVUV', 'IAU', 'IBIT', 'AVDV']
LEGACY   = ['AAPL', 'NVDA', 'SLV', 'VGT']
INDICES  = ['^VIX', 'GC=F', 'CL=F', 'DX-Y.NYB', '^GSPC', '^IXIC', '^TNX']
MACRO    = ['TLT', 'HYG', 'LQD', 'GLD']

ALL_TICKERS = HOLDINGS + LEGACY + INDICES + MACRO

TICKER_LABELS = {
    'VT':       'Vanguard Total World',
    'AVUV':     'Avantis US Small Cap Value',
    'IAU':      'iShares Gold',
    'IBIT':     'iShares Bitcoin',
    'AVDV':     'Avantis Intl Small Cap Value',
    'AAPL':     'Apple (legacy)',
    'NVDA':     'Nvidia (legacy)',
    'SLV':      'Silver (legacy)',
    'VGT':      'Vanguard IT (legacy)',
    '^VIX':     'VIX Fear Index',
    'GC=F':     'Gold Futures',
    'CL=F':     'Crude Oil WTI',
    'DX-Y.NYB': 'DXY Dollar Index',
    '^GSPC':    'S&P 500',
    '^IXIC':    'Nasdaq Composite',
    '^TNX':     'US 10Y Yield',
    'TLT':      'iShares 20Y Treasury',
    'HYG':      'iShares High Yield Corp',
    'LQD':      'iShares Investment Grade Corp',
    'GLD':      'SPDR Gold',
}

# Precio promedio de compra del usuario
USER_AVG_PRICES = {
    'VT':   None,
    'AVUV': None,
    'IAU':  None,
    'IBIT': None,
    'AVDV': None,
}

PORTFOLIO_WEIGHTS = {
    'VT':   0.40,
    'AVUV': 0.20,
    'IAU':  0.15,
    'IBIT': 0.15,
    'AVDV': 0.10,
}


def ema(series: pd.Series, window: int) -> pd.Series:
    return series.ewm(span=window, adjust=False).mean()


def pct_from_ath(series: pd.Series) -> float:
    """% de caída desde el ATH histórico de la serie."""
    ath = series.max()
    current = series.iloc[-1]
    return (current - ath) / ath * 100


def ytd_return(series: pd.Series) -> float:
    year_start = f"{datetime.today().year}-01-01"
    try:
        start_price = series.loc[series.index >= year_start].iloc[0]
        return (series.iloc[-1] - start_price) / start_price * 100
    except IndexError:
        return float('nan')


def one_year_return(series: pd.Series) -> float:
    one_yr_ago = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
    try:
        start_price = series.loc[series.index >= one_yr_ago].iloc[0]
        return (series.iloc[-1] - start_price) / start_price * 100
    except IndexError:
        return float('nan')


def download_all():
    print(f"\n{'='*60}")
    print(f"  Financial Data Downloader  —  {TODAY}")
    print(f"{'='*60}\n")

    # ── 1. Descarga histórica larga (para backtests / EMAs) ─────
    print("Descargando historial largo (2018–hoy)...")
    raw = yf.download(
        ALL_TICKERS,
        start=START_LONG,
        end=TODAY,
        auto_adjust=True,
        progress=False,
        threads=True,
    )

    # Guardamos precios de cierre completos
    closes = raw['Close'] if 'Close' in raw else raw.xs('Close', axis=1, level=0)
    closes.index = pd.to_datetime(closes.index)
    closes.to_csv(os.path.join(DATA_DIR, "closes_long.csv"))
    print(f"  OK closes_long.csv  ({len(closes)} filas x {len(closes.columns)} tickers)")

    # ── 2. Snapshot de hoy — métricas por ticker ────────────────
    print("\nCalculando snapshot de mercado...")
    snapshot = {}

    for ticker in ALL_TICKERS:
        if ticker not in closes.columns:
            print(f"  ✗ {ticker}: sin datos")
            continue

        s = closes[ticker].dropna()
        if len(s) < 10:
            continue

        current = s.iloc[-1]
        e20  = ema(s, 20).iloc[-1]
        e50  = ema(s, 50).iloc[-1]
        e200 = ema(s, 200).iloc[-1]

        snapshot[ticker] = {
            "label":         TICKER_LABELS.get(ticker, ticker),
            "price":         round(float(current), 4),
            "ema20":         round(float(e20), 4),
            "ema50":         round(float(e50), 4),
            "ema200":        round(float(e200), 4),
            "pct_vs_ema20":  round((current - e20)  / e20  * 100, 2),
            "pct_vs_ema50":  round((current - e50)  / e50  * 100, 2),
            "pct_vs_ema200": round((current - e200) / e200 * 100, 2),
            "pct_from_ath":  round(pct_from_ath(s), 2),
            "ytd_return":    round(ytd_return(s), 2),
            "1y_return":     round(one_year_return(s), 2),
            "as_of":         TODAY,
        }

    # Guardar snapshot
    snapshot_path = os.path.join(DATA_DIR, "snapshot_today.json")
    with open(snapshot_path, "w") as f:
        json.dump(snapshot, f, indent=2)
    print(f"  ✓ snapshot_today.json")

    # ── 3. Correlaciones recientes (1 año) ──────────────────────
    one_yr_ago = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
    recent_closes = closes.loc[closes.index >= one_yr_ago, HOLDINGS].dropna()
    if len(recent_closes) > 20:
        corr = recent_closes.pct_change().dropna().corr().round(3)
        corr.to_csv(os.path.join(DATA_DIR, "correlations_1y.csv"))
        print(f"  ✓ correlations_1y.csv")

    # ── 4. Retornos diarios holdings ────────────────────────────
    holdings_closes = closes[HOLDINGS].dropna(how='all')
    returns = holdings_closes.pct_change().dropna()
    returns.to_csv(os.path.join(DATA_DIR, "returns_daily.csv"))
    print(f"  ✓ returns_daily.csv")

    # ── 5. Tabla resumen legible ─────────────────────────────────
    print("\n" + "─"*70)
    print(f"{'TICKER':<12} {'PRECIO':>8} {'YTD%':>7} {'1Y%':>7} "
          f"{'EMA20%':>7} {'EMA200%':>8} {'ATH%':>7}")
    print("─"*70)

    for group_name, group in [
        ("HOLDINGS", HOLDINGS),
        ("LEGACY",   LEGACY),
        ("MACRO/INDICES", INDICES[:4] + MACRO),
    ]:
        print(f"\n  [{group_name}]")
        for t in group:
            if t not in snapshot:
                continue
            d = snapshot[t]
            alert = ""
            if t == '^VIX' and d['price'] > 30:
                alert = " ⚠ VIX ALTO"
            elif d['pct_vs_ema200'] < -10:
                alert = " ⚠ bajo EMA200"
            elif d['pct_from_ath'] < -20:
                alert = " ⚠ -20% ATH"

            print(f"  {t:<12} {d['price']:>8.2f} {d['ytd_return']:>7.1f}% "
                  f"{d['1y_return']:>7.1f}% {d['pct_vs_ema20']:>7.1f}% "
                  f"{d['pct_vs_ema200']:>8.1f}% {d['pct_from_ath']:>7.1f}%{alert}")

    # ── 6. Señal de mercado ─────────────────────────────────────
    print("\n" + "─"*70)
    print("SEÑAL DE MERCADO")
    print("─"*70)

    vix = snapshot.get('^VIX', {}).get('price', 0)
    dxy_ytd = snapshot.get('DX-Y.NYB', {}).get('ytd_return', 0)
    oil = snapshot.get('CL=F', {}).get('price', 0)
    tlt_ytd = snapshot.get('TLT', {}).get('ytd_return', 0)
    sp500_ytd = snapshot.get('^GSPC', {}).get('ytd_return', 0)
    gold_ytd = snapshot.get('GLD', {}).get('ytd_return', 0)

    print(f"\n  VIX:          {vix:.1f}  {'🔴 PÁNICO' if vix > 50 else '⚠  ATENCIÓN' if vix > 30 else '🟢 Normal'}")
    print(f"  DXY YTD:      {dxy_ytd:+.1f}%")
    print(f"  WTI Oil:      ${oil:.1f}")
    print(f"  TLT YTD:      {tlt_ytd:+.1f}%")
    print(f"  S&P500 YTD:   {sp500_ytd:+.1f}%")
    print(f"  Gold YTD:     {gold_ytd:+.1f}%")

    # Diagnóstico automático
    signals = []
    if vix > 30:
        signals.append(f"VIX en {vix:.0f} — mercado en modo miedo")
    if dxy_ytd > 3:
        signals.append(f"DXY +{dxy_ytd:.1f}% YTD — flight to safety activo")
    elif dxy_ytd < -3:
        signals.append(f"DXY {dxy_ytd:.1f}% YTD — debilidad dólar, favorable a ex-USA")
    if oil > 100:
        signals.append(f"Oil ${oil:.0f} — riesgo stagflación")
    if tlt_ytd < -5 and sp500_ytd < -5:
        signals.append("TLT y equities cayendo juntos — posible stagflación")
    if gold_ytd > 10:
        signals.append(f"Gold +{gold_ytd:.1f}% YTD — hedge activo, riesgo sistémico percibido")

    if signals:
        print("\n  Alertas activas:")
        for s in signals:
            print(f"    • {s}")
    else:
        print("\n  Sin alertas críticas. Mercado en rango normal.")

    # Portfolio holdings resumen
    print("\n" + "─"*70)
    print("PORTFOLIO — THE GLOBAL COMPOUNDER")
    print("─"*70)
    for t in HOLDINGS:
        if t not in snapshot:
            continue
        d = snapshot[t]
        w = PORTFOLIO_WEIGHTS.get(t, 0)
        print(f"  {t:<6} ({w*100:.0f}%)  precio {d['price']:.2f}  "
              f"YTD {d['ytd_return']:+.1f}%  ATH {d['pct_from_ath']:.1f}%  "
              f"vs EMA200 {d['pct_vs_ema200']:+.1f}%")

    print(f"\n  Datos guardados en: {DATA_DIR}")
    print(f"  Fecha: {TODAY}\n")

    return snapshot


if __name__ == "__main__":
    snapshot = download_all()
