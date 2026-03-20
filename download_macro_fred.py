"""
Descarga datos macro desde FRED (Federal Reserve Economic Data).
API gratuita â€” registrarse en https://fred.stlouisfed.org/docs/api/api_key.html
"""

import requests
import pandas as pd
import json
import os
from datetime import datetime

# â”€â”€ CONFIGURACIÃ“N â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def _load_env():
    """Carga variables desde .env si existe (sin dependencias externas)."""
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip())
_load_env()

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "macro")
os.makedirs(DATA_DIR, exist_ok=True)

# Series clave para el contexto macro
FRED_SERIES = {
    # InflaciÃ³n
    "CPIAUCSL":    "CPI YoY (inflaciÃ³n general USA)",
    "PCEPI":       "PCE (inflaciÃ³n â€” mÃ©trica favorita Fed)",
    "T5YIE":       "InflaciÃ³n implÃ­cita 5 aÃ±os (breakeven)",

    # Tasas y polÃ­tica monetaria
    "FEDFUNDS":    "Fed Funds Rate efectivo",
    "DFF":         "Fed Funds diario",
    "T10Y2Y":      "Spread 10Y-2Y (curva inversiÃ³n = recesiÃ³n warning)",
    "DGS10":       "Treasury 10Y yield",
    "DGS2":        "Treasury 2Y yield",

    # Empleo
    "UNRATE":      "Tasa desempleo USA",
    "PAYEMS":      "Non-Farm Payrolls (NFP)",
    "ICSA":        "Reclamos desempleo semanales",

    # Actividad econÃ³mica
    "GDP":         "GDP USA (trimestral)",
    "NAPM":        "ISM Manufacturing PMI",
    "NMFNMI":      "ISM Services PMI",
    "INDPRO":      "ProducciÃ³n industrial",
    "UMCSENT":     "Confianza consumidor Michigan",

    # CrÃ©dito y liquidez
    "BAMLH0A0HYM2": "High Yield spread (OAS) â€” stress crediticio",
    "BAMLC0A0CM":   "US IG Corporate OAS",
    "M2SL":        "M2 Money Supply",
    "WALCL":       "Balance sheet Fed (QE/QT tracker)",
}


def fetch_series(series_id: str, start: str = "2018-01-01") -> pd.Series | None:
    if not FRED_API_KEY:
        return None
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}"
        f"&observation_start={start}"
        f"&api_key={FRED_API_KEY}"
        f"&file_type=json"
    )
    r = requests.get(url, timeout=10)
    if r.status_code != 200:
        return None
    obs = r.json().get("observations", [])
    s = pd.Series(
        {o["date"]: float(o["value"]) if o["value"] != "." else None for o in obs},
        name=series_id,
    ).dropna()
    s.index = pd.to_datetime(s.index)
    return s


def download_fred():
    if not FRED_API_KEY:
        print("ERROR: FRED_API_KEY no configurada.")
        print("  1. Registrate gratis en https://fred.stlouisfed.org/docs/api/api_key.html")
        print("  2. Setea la variable: set FRED_API_KEY=tu_clave")
        print("  3. Vuelve a correr este script")
        return

    print(f"\nDescargando datos FRED â€” {datetime.today().strftime('%Y-%m-%d')}")
    all_data = {}
    latest = {}

    for series_id, label in FRED_SERIES.items():
        s = fetch_series(series_id)
        if s is None or len(s) == 0:
            print(f"  SKIP {series_id}")
            continue

        s.to_csv(os.path.join(DATA_DIR, f"{series_id}.csv"), header=True)
        all_data[series_id] = s
        latest[series_id] = {
            "label": label,
            "value": round(float(s.iloc[-1]), 4),
            "date": s.index[-1].strftime("%Y-%m-%d"),
            "prev": round(float(s.iloc[-2]), 4) if len(s) > 1 else None,
            "change": round(float(s.iloc[-1] - s.iloc[-2]), 4) if len(s) > 1 else None,
        }
        print(f"  OK {series_id}: {latest[series_id]['value']} ({latest[series_id]['date']})")

    # Guardar snapshot macro
    with open(os.path.join(DATA_DIR, "macro_snapshot.json"), "w") as f:
        json.dump(latest, f, indent=2)

    # DiagnÃ³stico automÃ¡tico
    print("\nâ”€â”€ DIAGNÃ“STICO MACRO â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€")

    if "T10Y2Y" in latest:
        spread = latest["T10Y2Y"]["value"]
        print(f"  Curva 10Y-2Y: {spread:+.2f}%  {'INVERTIDA - recesion signal' if spread < 0 else 'Normal'}")

    if "FEDFUNDS" in latest:
        rate = latest["FEDFUNDS"]["value"]
        print(f"  Fed Funds:    {rate:.2f}%")

    if "CPIAUCSL" in latest:
        cpi = latest["CPIAUCSL"]["value"]
        print(f"  CPI:          {cpi:.1f} (indice)")

    if "UNRATE" in latest:
        unemp = latest["UNRATE"]["value"]
        print(f"  Desempleo:    {unemp:.1f}%  {'ALERTA >5%' if unemp > 5 else 'OK'}")

    if "BAMLH0A0HYM2" in latest:
        hy_spread = latest["BAMLH0A0HYM2"]["value"]
        print(f"  HY Spread:    {hy_spread:.0f}bps  {'STRESS >500bps' if hy_spread > 500 else 'Normal'}")

    print(f"\n  Datos guardados en: {DATA_DIR}")


if __name__ == "__main__":
    download_fred()
