"""
Daily Pipeline — The Global Compounder
py daily_pipeline.py

Output:
  data/daily_summaries/YYYY-MM-DD.md
  data/daily_summaries/YYYY-MM-DD.pdf
  data/narrative_log.csv
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import os, json, requests, warnings, re
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import feedparser
from fpdf import FPDF

warnings.filterwarnings('ignore')


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

# ── Config ────────────────────────────────────────────────────────
BASE      = os.path.dirname(__file__)
DATA_DIR  = os.path.join(BASE, "data")
SUMM_DIR  = os.path.join(DATA_DIR, "daily_summaries")
MACRO_DIR = os.path.join(DATA_DIR, "macro")
os.makedirs(SUMM_DIR, exist_ok=True)
# Evita errores de cache SQLite en entornos con permisos restringidos
try:
    yf.set_tz_cache_location(os.path.join(DATA_DIR, "yf_cache"))
except Exception:
    pass

FRED_KEY = os.environ.get("FRED_API_KEY", "")
TODAY    = datetime.today().strftime("%Y-%m-%d")

MACRO_TICKERS = {
    '^VIX':     'VIX',
    'CL=F':     'WTI Oil',
    'GC=F':     'Gold',
    'DX-Y.NYB': 'DXY',
    '^GSPC':    'S&P 500',
    '^IXIC':    'Nasdaq',
    'TLT':      'TLT 20Y',
    '^TNX':     '10Y Yield',
}
EXTRA_TICKERS = ['USDCLP=X', 'HG=F']  # Para sección CLP (cobre + peso chileno)

def _load_portfolio_config():
    """Carga PORTFOLIO, LEGACY y HOLDINGS desde data/portfolio.json. Fallback hardcodeado."""
    _pf = {
        'VT':   {'shares': 5.22474414, 'avg': 143.37},
        'AVUV': {'shares': 3.45692295, 'avg': 108.19},
        'IAU':  {'shares': 2.86417296, 'avg': 98.11},
        'IBIT': {'shares': 8.97313033, 'avg': 38.61},
        'AVDV': {'shares': 1.81835051, 'avg': 103.47},
    }
    _leg = {
        'AAPL': {'shares': 0.24143144, 'avg': 270.14},
        'NVDA': {'shares': 0.54497311, 'avg': 184.71},
        'SLV':  {'shares': 0.79254123, 'avg': 75.71},
        'VGT':  {'shares': 0.74719613, 'avg': 733.49},
    }
    _hold = {'VT': 0.40, 'AVUV': 0.20, 'IAU': 0.15, 'IBIT': 0.15, 'AVDV': 0.10}
    try:
        p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "portfolio.json")
        with open(p, encoding='utf-8') as f:
            data = json.load(f)
        _pf  = {t: {'shares': v['shares'], 'avg': v['avg_price']}
                for t, v in data.get('holdings', {}).items()}
        _leg = {t: {'shares': v['shares'], 'avg': v['avg_price']}
                for t, v in data.get('legacy_a_liquidar', {}).items()}
        _hold = {t: v.get('target_weight', 0)
                 for t, v in data.get('holdings', {}).items()}
    except Exception:
        pass
    return _pf, _leg, _hold

PORTFOLIO, LEGACY, HOLDINGS = _load_portfolio_config()
NEWS_FEEDS = [
    ("Reuters Business",  "https://feeds.reuters.com/reuters/businessNews"),
    ("Reuters Markets",   "https://feeds.reuters.com/reuters/UKmarkets"),
    ("CNBC",              "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("MarketWatch",       "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines"),
    ("Yahoo Finance",     "https://finance.yahoo.com/news/rssindex"),
]
NEWS_KEYWORDS = [
    'fed','federal reserve','fomc','powell','warsh','rate','inflation','cpi','pce','gdp',
    'recession','employment','nfp','iran','hormuz','oil','crude','opec','bitcoin','btc',
    'crypto','ibit','gold','treasury','yield','vix','s&p','nasdaq','market','stocks',
    'tariff','trade','china','dollar','dxy','equity','metals','silver',
]
UPCOMING_EVENTS = [
    ("2026-04-03", "NFP empleo marzo",              "HIGH",  "Define salud del mercado laboral"),
    ("2026-04-10", "CPI marzo",                     "HIGH",  "Primer CPI post-Hormuz. Define path Fed"),
    ("2026-04-14", "Q1 Earnings: JPM + WFC",        "MED",   "Salud bancaria -> proxy credito"),
    ("2026-04-28", "FOMC — ultima reunion Powell",  "HIGH",  "Ultimo mensaje Powell antes de salir"),
    ("2026-05-08", "NFP abril",                     "MED",   "Confirmacion tendencia empleo"),
    ("2026-05-12", "CPI abril",                     "HIGH",  "Confirma/niega pass-through de oil"),
    ("2026-05-15", "Powell sale. Entra Warsh",      "HIGH",  "Cambio Fed Chair = incertidumbre politica"),
    ("2026-06-15", "G7 Evian — Iran agenda",        "MED",   "Posible senal diplomatica Iran"),
    ("2026-07-07", "NATO Ankara",                   "MED",   "Cohesion alianza vs Iran"),
]


# ── Helpers basicos ───────────────────────────────────────────────
def ema(s, w):  return s.ewm(span=w, adjust=False).mean()
def pct_ath(s): return (s.iloc[-1] - s.max()) / s.max() * 100
def ytd_ret(s):
    try:
        p0 = s.loc[s.index >= f"{datetime.today().year}-01-01"].iloc[0]
        return (s.iloc[-1] - p0) / p0 * 100
    except: return float('nan')
def d1_ret(s):
    return (s.iloc[-1] - s.iloc[-2]) / s.iloc[-2] * 100 if len(s) > 1 else float('nan')

def calc_rsi(s, period=14):
    d = s.diff()
    g = d.clip(lower=0); l = (-d).clip(lower=0)
    ag = g.ewm(com=period-1, adjust=False).mean()
    al = l.ewm(com=period-1, adjust=False).mean()
    rs = ag / al
    return 100 - (100 / (1 + rs))

def clean(text):
    """Fuerza latin-1 para fpdf."""
    return (str(text)
            .replace('\u2014','-').replace('\u2013','-').replace('\u2019',"'")
            .replace('\u201c','"').replace('\u201d','"').replace('\u2018',"'")
            .replace('\u2022','*').replace('\u00b0','')
            .encode('latin-1', errors='replace').decode('latin-1'))

def get_val(closes, t): return float(closes[t].dropna().iloc[-1]) if t in closes.columns else None


# ── DATA LAYER ────────────────────────────────────────────────────
def get_prices():
    all_t = list(HOLDINGS) + list(LEGACY) + list(MACRO_TICKERS) + EXTRA_TICKERS
    try:
        raw = yf.download(all_t, period='1y', auto_adjust=True, progress=False, threads=True)
        closes = (raw['Close'] if 'Close' in raw.columns.get_level_values(0)
                  else raw.xs('Close', axis=1, level=0))
        closes.index = pd.to_datetime(closes.index)
    except Exception as e:
        print(f"  WARNING: yfinance error ({e}), cargando desde cache local")
        closes = pd.DataFrame()
    # Fallback desde closes_long.csv para tickers ausentes o sin datos
    missing = [t for t in all_t if t not in closes.columns or closes[t].dropna().empty]
    if missing:
        cache_path = os.path.join(DATA_DIR, "closes_long.csv")
        if os.path.exists(cache_path):
            try:
                cached = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                cutoff = pd.Timestamp.today() - pd.DateOffset(years=1)
                for t in missing:
                    if t in cached.columns:
                        closes[t] = cached.loc[cached.index >= cutoff, t]
                        print(f"  WARNING: {t} usando ultimo valor cacheado")
            except Exception:
                pass
    return closes

def get_cnn_fg():
    cache_path = os.path.join(DATA_DIR, "cnn_fg_cache.json")
    try:
        headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36',
            'Accept':'application/json,text/plain,*/*',
            'Accept-Language':'en-US,en;q=0.9',
            'Referer':'https://edition.cnn.com/markets/fear-and-greed',
            'Origin':'https://edition.cnn.com'
        }
        r = requests.get("https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
                         timeout=10, headers=headers)
        fg = r.json()['fear_and_greed']
        s = fg['score']
        out = {'score': round(s,1), 'rating': fg['rating'],
                'change': round(s - fg['previous_close'], 1),
                'prev_1w': round(fg.get('previous_1_week', s), 1),
                'prev_1m': round(fg.get('previous_1_month', s), 1)}
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(out, f)
        except:
            pass
        return out
    except:
        # Fallback a cache local si existe
        if os.path.exists(cache_path):
            try:
                with open(cache_path, encoding='utf-8') as f:
                    cached = json.load(f)
                cached.setdefault('rating', 'N/A')
                cached.setdefault('change', None)
                return cached
            except:
                pass
        # Fallback a ultimo resumen diario
        try:
            files = sorted([f for f in os.listdir(SUMM_DIR) if f.endswith('.md') and f < f"{TODAY}.md"], reverse=True)
            if files:
                with open(os.path.join(SUMM_DIR, files[0]), encoding='utf-8') as f:
                    txt = f.read()
                m = re.search(r'CNN Fear\s*&\s*Greed.*?:\s*([0-9]+(?:\.[0-9])?)', txt, re.IGNORECASE)
                if m:
                    return {'score': float(m.group(1)), 'rating':'CACHED', 'change': None, 'prev_1w': None, 'prev_1m': None}
        except:
            pass
        return {'score': None, 'rating':'N/A', 'change': None}

def get_btc_fg():
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=2", timeout=8)
        d = r.json()['data']
        return {'score': int(d[0]['value']), 'rating': d[0]['value_classification'],
                'prev': int(d[1]['value']), 'change': int(d[0]['value'])-int(d[1]['value'])}
    except: return {'score': None, 'rating':'N/A', 'change': None}

def get_fred():
    p = os.path.join(MACRO_DIR, "macro_snapshot.json")
    if not os.path.exists(p): return {}
    with open(p) as f: return json.load(f)

def get_news(max_items=6):
    seen, articles = set(), []
    for source, url in NEWS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:20]:
                title = e.get('title','').strip()
                key   = title.lower()[:60]
                if key in seen: continue
                seen.add(key)
                combined = (title + ' ' + e.get('summary','')).lower()
                if not any(kw in combined for kw in NEWS_KEYWORDS): continue
                score = sum(1 for kw in NEWS_KEYWORDS if kw in combined)
                articles.append({'title': title, 'summary': e.get('summary','')[:200],
                                  'source': source, 'score': score})
        except: continue
    articles.sort(key=lambda x: x['score'], reverse=True)
    return articles[:max_items]

def read_prev():
    files = sorted([f for f in os.listdir(SUMM_DIR)
                    if f.endswith('.md') and f < f"{TODAY}.md"], reverse=True)
    if not files: return None, None
    with open(os.path.join(SUMM_DIR, files[0]), encoding='utf-8') as f:
        return files[0].replace('.md',''), f.read()

def build_three_month_view(closes, fred, drivers, pdf_text=None):
    # Texto base esperado para 3M view (macro-tactico)
    view_lines = [
        "Base case (mas probable): Inflation shock + geopolitics se mantiene. Oil sigue como driver central; por eso el mercado sostiene higher-for-longer y los multiples se comprimen. Equities con sesgo lateral-bajista, rebotes tacticos posibles pero mas cortos.",
        "Riesgo a la baja: si el shock de petroleo se mantiene, el crecimiento se debilita y el sentimiento extremo no se traduce en rally duradero. VIX alto + tasas reales altas = rebotes fragiles.",
        "Riesgo al alza (pero menos probable): normalizacion gradual de oil. Eso alivia inflacion implicita -> yields aflojan -> equities pueden rebotar con mas sustain, especialmente si credito sigue estable.",
        "Claves de confirmacion/cambio (1-3M): Oil baja fuerte (umbral ~$86) cambia regimen; HY spreads se abren rapido -> risk-off real; Liquidez se revierte (Fed/M2) -> presion en equities y crypto.",
        "Lectura final: inflation shock con credito estable sugiere desaceleracion sin crisis (por ahora), sesgo defensivo y rallies tacticos, no bull sostenido salvo normalizacion de oil."
    ]
    # Version para PDF: dividir en segmentos mas cortos para evitar overflow
    pdf_lines = []
    for line in view_lines:
        parts = []
        for chunk in line.split(". "):
            if ";" in chunk:
                parts.extend([p.strip() for p in chunk.split("; ") if p.strip()])
            else:
                parts.append(chunk.strip())
        pdf_lines.extend([p if p.endswith(".") else p + "." for p in parts if p])
    view = " | ".join(view_lines)
    reason = "Basado en oil como driver, higher-for-longer, credito estable y liquidez en transicion."
    if pdf_text:
        reason = "Basado en lectura del PDF + drivers clave (oil, tasas, credito, liquidez)."
    # Compare with previous
    prev_date, prev_text = read_prev()
    changed, prev_view, why = False, None, None
    if prev_text:
        m = re.search(r'\\[3M\\].*?VIEW\\n-\\s*(.+)', prev_text, re.IGNORECASE)
        if m:
            prev_view = m.group(1).strip()
            if prev_view != view:
                changed = True
                why = f"Cambio por: {reason}"
    return {"view": view, "reason": reason, "changed": changed, "prev_view": prev_view, "why": why, "lines": view_lines, "pdf_lines": pdf_lines}


def build_three_month_view_groq(closes, fred, drivers, regime=None, tensions=None, cnn=None, btc=None, pdata=None, total_val=0):
    """3M view generado por Groq. Fallback al hardcodeado si falla."""
    if not os.environ.get("GROQ_API_KEY", "") or not regime:
        return build_three_month_view(closes, fred, drivers)

    oil_ytd  = ytd_ret(closes['CL=F'].dropna())  if 'CL=F'  in closes.columns else 0
    sp_ytd   = ytd_ret(closes['^GSPC'].dropna()) if '^GSPC' in closes.columns else 0
    gold_ytd = ytd_ret(closes['GC=F'].dropna())  if 'GC=F'  in closes.columns else 0
    vix      = get_val(closes, '^VIX') or 20
    dgs10    = fred.get('DGS10',{}).get('value', 4.2)
    t5yie    = fred.get('T5YIE',{}).get('value', 2.5)
    hy       = fred.get('BAMLH0A0HYM2',{}).get('value', 3.0)
    cnn_s    = cnn.get('score','N/D') if cnn else 'N/D'
    btc_s    = btc.get('score','N/D') if btc else 'N/D'
    total_inv = sum(PORTFOLIO[t]['avg'] * PORTFOLIO[t]['shares'] for t in PORTFOLIO)
    pnl_p    = (total_val - total_inv) / total_inv * 100 if total_inv else 0
    drivers_txt  = " | ".join(f"{d['label']}: {d['detail']}" for d in drivers[:3]) or "Sin drivers dominantes"
    tensions_txt = " | ".join(tensions[:2]) if tensions else "Sin tensiones dominantes"

    prompt = f"""Eres un analista macro y de mercados de largo plazo. Genera un 3M VIEW (perspectiva proximos 3 meses) basado en datos reales.

Datos del dia {TODAY}:
- Regimen: {regime['label']} — {regime['desc']}
- Drivers principales: {drivers_txt}
- Mercado: VIX {vix:.1f} | Oil YTD {oil_ytd:+.1f}% | S&P YTD {sp_ytd:+.1f}% | Gold YTD {gold_ytd:+.1f}%
- Tasas: 10Y {dgs10:.2f}% | Inflacion impl 5Y {t5yie:.2f}% | HY spreads {hy*100:.0f}bps
- Sentimiento: CNN F&G {cnn_s} | BTC F&G {btc_s}
- Macro: Crecimiento {summarize_growth(fred)} | Liquidez {summarize_liquidity(fred)} | Credito {summarize_credit(fred)}
- Portfolio VT/AVUV/IAU/IBIT/AVDV: ${total_val:,.0f} ({pnl_p:+.1f}% vs costo)
- Tensiones detectadas: {tensions_txt}

Genera exactamente 5 bullets en espanol, uno por linea, empezando con "- ":
1. Base case (~60%): escenario mas probable en 3 meses con cifras concretas
2. Bear case (~20%): que podria salir peor y por que
3. Bull case (~15%): sorpresa positiva y condicion necesaria
4. Claves a monitorear: 2-3 indicadores con umbrales concretos
5. Postura portfolio: accion sugerida para VT/AVUV/IAU/IBIT/AVDV

Sin introduccion, sin titulos, solo los 5 bullets."""

    text = _groq_call(prompt, max_tokens=600)
    if text:
        lines = [l.lstrip("-• *").strip() for l in text.split('\n') if l.strip()]
        if len(lines) >= 3:
            print(f"     [Groq] 3M view generado ({len(lines)} puntos)")
            pdf_lines = []
            for line in lines:
                parts = [p.strip() for p in line.split('. ') if p.strip()]
                pdf_lines.extend([p if p.endswith('.') else p + '.' for p in parts])
            view = " | ".join(lines)
            prev_date, prev_text = read_prev()
            changed, prev_view, why = False, None, None
            if prev_text:
                m = re.search(r'\[3M\].*?\n-\s*(.+)', prev_text, re.IGNORECASE)
                if m:
                    prev_view = m.group(1).strip()
                    changed = prev_view != lines[0]
                    if changed: why = "Cambio de escenario segun datos del dia."
            return {"view": view, "reason": "Generado por Groq/Llama.",
                    "changed": changed, "prev_view": prev_view, "why": why,
                    "lines": lines, "pdf_lines": pdf_lines}

    return build_three_month_view(closes, fred, drivers)


def extract_pdf_text(path):
    try:
        from PyPDF2 import PdfReader
    except Exception:
        return None
    try:
        reader = PdfReader(path)
        parts = []
        for p in reader.pages:
            t = p.extract_text() or ""
            parts.append(t)
        return "\n".join(parts)
    except Exception:
        return None


# ── INTELLIGENCE LAYER ────────────────────────────────────────────
def detect_regime(closes, fred):
    vix      = get_val(closes, '^VIX') or 20
    oil_ytd  = ytd_ret(closes['CL=F'].dropna())  if 'CL=F' in closes.columns else 0
    gold_ytd = ytd_ret(closes['GC=F'].dropna())  if 'GC=F' in closes.columns else 0
    sp_ytd   = ytd_ret(closes['^GSPC'].dropna()) if '^GSPC' in closes.columns else 0
    tlt_ytd  = ytd_ret(closes['TLT'].dropna())   if 'TLT' in closes.columns else 0
    t10y2y   = fred.get('T10Y2Y',{}).get('value', 0.5)
    t5yie    = fred.get('T5YIE', {}).get('value', 2.5)
    unrate   = fred.get('UNRATE', {}).get('value', 4.0)

    if oil_ytd > 30 and t5yie > 2.4 and sp_ytd < -2:
        return {'code':'INFLATION_SHOCK', 'color':(200,0,0),
                'label':'Inflation Shock + Geopolitical Risk',
                'desc': f'Oil +{oil_ytd:.0f}% YTD genera presion inflacionaria ({t5yie:.2f}% impl.). '
                        f'Equities sufren compresion de multiples. Stagflacion-lite activa.'}
    if sp_ytd < -5 and tlt_ytd < -2 and gold_ytd > 8:
        return {'code':'STAGFLATION', 'color':(200,60,0),
                'label':'Stagflation / Flight to Hard Assets',
                'desc': 'Equities Y bonos cayendo juntos. Solo oro actua como refugio. '
                        'La Fed no puede cortar (inflacion) ni subir (economia debil).'}
    if vix > 25 and sp_ytd < -5 and tlt_ytd > 2:
        return {'code':'RISK_OFF', 'color':(180,80,0),
                'label':'Risk-Off / Recession Fear',
                'desc': 'Equities bajan, bonos suben. Flight to safety clasico. '
                        'El mercado esta pricingando desaceleracion significativa.'}
    if vix < 18 and sp_ytd > 8 and t5yie < 2.4:
        return {'code':'RISK_ON', 'color':(0,140,0),
                'label':'Risk-On / Bull Market',
                'desc': 'Condiciones favorables: VIX bajo, equities al alza, '
                        'inflacion controlada. Momentum positivo sostenido.'}
    return {'code':'TRANSITION', 'color':(160,120,0),
            'label':'Transicion / Incertidumbre',
            'desc': 'Senales mixtas. El mercado esta procesando narrativas en competencia. '
                    'Cautela en sizing, no en tesis.'}

def rank_drivers(closes, fred, cnn, btc):
    drivers = []
    oil_ytd = ytd_ret(closes['CL=F'].dropna())   if 'CL=F' in closes.columns else 0
    dxy_ytd = ytd_ret(closes['DX-Y.NYB'].dropna()) if 'DX-Y.NYB' in closes.columns else 0
    vix     = get_val(closes, '^VIX') or 20
    vix_ytd = ytd_ret(closes['^VIX'].dropna())   if '^VIX' in closes.columns else 0
    t5yie   = fred.get('T5YIE',{}).get('value', 2.5)
    dgs10   = fred.get('DGS10',{}).get('value', 4.2)
    ff      = fred.get('FEDFUNDS',{}).get('value', 3.64)
    cnn_s   = cnn.get('score'); btc_s = btc.get('score')

    if abs(oil_ytd) > 15:
        drivers.append({'icon':'[GEO]', 'color':(200,0,0) if oil_ytd>30 else (200,130,0),
                        'label':'Geopolitica / Petroleo',
                        'detail': f'WTI {oil_ytd:+.0f}% YTD — Hormuz cerrado, supply shock estructural',
                        'score': abs(oil_ytd)})
    if t5yie > 2.4:
        drivers.append({'icon':'[INF]', 'color':(200,0,0) if t5yie>2.7 else (200,130,0),
                        'label':'Inflacion implicita al alza',
                        'detail': f'Breakeven 5Y: {t5yie:.2f}% — Fed no puede cortar',
                        'score': t5yie * 20})
    if dgs10 > 4.0:
        drivers.append({'icon':'[RTS]', 'color':(200,130,0),
                        'label':'Tasas altas persistentes',
                        'detail': f'10Y {dgs10:.2f}% | Fed Funds {ff:.2f}% — costo capital elevado',
                        'score': dgs10 * 12})
    if cnn_s and cnn_s < 30:
        drivers.append({'icon':'[SNT]', 'color':(0,140,0),
                        'label':'Sentimiento extremo (contrarian)',
                        'detail': f'CNN {cnn_s} | BTC {btc_s} — historicamente bullish 1-3M',
                        'score': (30 - cnn_s) * 2.5})
    if abs(dxy_ytd) > 3:
        drivers.append({'icon':'[DXY]', 'color':(200,130,0),
                        'label': f'Dolar {"fuerte" if dxy_ytd>0 else "debil"}',
                        'detail': f'DXY {dxy_ytd:+.1f}% YTD',
                        'score': abs(dxy_ytd) * 8})
    if vix > 25:
        drivers.append({'icon':'[VOL]', 'color':(200,130,0),
                        'label':'Volatilidad elevada',
                        'detail': f'VIX {vix:.0f} ({vix_ytd:+.0f}% YTD) — opciones caras, bid-ask amplio',
                        'score': vix})

    drivers.sort(key=lambda x: x['score'], reverse=True)
    return drivers

def build_causal_chains(closes, fred):
    oil_ytd  = ytd_ret(closes['CL=F'].dropna())   if 'CL=F' in closes.columns else 0
    gold_ytd = ytd_ret(closes['GC=F'].dropna())   if 'GC=F' in closes.columns else 0
    gold_d1  = d1_ret(closes['GC=F'].dropna())    if 'GC=F' in closes.columns else 0
    sp_ytd   = ytd_ret(closes['^GSPC'].dropna())  if '^GSPC' in closes.columns else 0
    tlt_ytd  = ytd_ret(closes['TLT'].dropna())    if 'TLT' in closes.columns else 0
    dgs10    = fred.get('DGS10',{}).get('value', 4.2)
    chains = []
    if oil_ytd > 20:
        chains.append(f"Oil +{oil_ytd:.0f}% -> Inflacion -> Yields altos -> Equities comprimen multiples")
    if gold_d1 is not None and gold_d1 < -2 and dgs10 > 4.0:
        chains.append(f"Real yields altos ({dgs10:.2f}%) -> costo oportunidad Gold -> IAU presionado hoy")
    elif gold_ytd > 10:
        chains.append(f"Gold +{gold_ytd:.0f}% YTD -> bancos centrales desdolarizando + flight to safety")
    if sp_ytd < -3 and tlt_ytd < 0:
        chains.append("Equities DOWN + Bonos DOWN -> sin refugio en renta fija = senal stagflacion")
    elif sp_ytd < -3 and tlt_ytd > 0:
        chains.append(f"Equities down, TLT {tlt_ytd:+.1f}% -> senal mixta: flight to safety pese a inflacion")
    return chains

def build_feedback_loop(closes, fred):
    oil_ytd  = ytd_ret(closes['CL=F'].dropna())   if 'CL=F' in closes.columns else 0
    dgs10    = fred.get('DGS10',{}).get('value', 4.2)
    if oil_ytd > 15 and dgs10 > 4.0:
        return "Oil ↑ → inflacion ↑ → Fed hawkish → crecimiento ↓ → demanda ↓ → presion bajista en oil"
    return "Tasas ↑ → USD ↑ → condiciones financieras se endurecen → crecimiento ↓ → inflacion ↓ → tasas ↓"

def build_scenarios(closes, fred, cnn):
    oil_ytd = ytd_ret(closes['CL=F'].dropna()) if 'CL=F' in closes.columns else 0
    dgs10   = fred.get('DGS10',{}).get('value', 4.2)
    vix     = get_val(closes, '^VIX') or 20
    growth, credit, liquidity = summarize_growth(fred), summarize_credit(fred), summarize_liquidity(fred)
    if "contraccion" in growth and "estable" in credit:
        return [
            ("55%", "Desaceleracion sin crisis (credito estable)"),
            ("30%", "Inflacion persistente + tasas altas"),
            ("15%", "Deterioro crediticio -> hard-landing")
        ]
    if oil_ytd > 20 and dgs10 > 4.0:
        return [
            ("60%", "Inflacion persistente + tasas altas"),
            ("25%", "Normalizacion gradual del petroleo"),
            ("15%", "Recesion tecnica por shock energetico")
        ]
    if vix > 25:
        return [
            ("50%", "Risk-off tactico sin recesion profunda"),
            ("30%", "Rebote por alivio inflacionario"),
            ("20%", "Hard-landing")
        ]
    return [
        ("55%", "Crecimiento moderado + inflacion controlada"),
        ("30%", "Rebote inflacionario transitorio"),
        ("15%", "Riesgo de recesion leve")
    ]

def build_conviction(closes, fred, cnn):
    oil_ytd = ytd_ret(closes['CL=F'].dropna()) if 'CL=F' in closes.columns else 0
    dgs10   = fred.get('DGS10',{}).get('value', 4.2)
    vix     = get_val(closes, '^VIX') or 20
    if oil_ytd > 30 and dgs10 > 4.0 and vix > 22:
        return "MEDIA-ALTA (dependiente de oil)"
    if oil_ytd > 20 or dgs10 > 4.0:
        return "MEDIA (sensitiva a tasas reales)"
    return "BAJA-MEDIA (senales mixtas)"

def build_expectations(closes, fred):
    t5yie = fred.get('T5YIE', {}).get('value', 2.5)
    dgs10 = fred.get('DGS10',{}).get('value', 4.2)
    t10y2y = fred.get('T10Y2Y',{}).get('value', 0.5)
    hy_s  = fred.get('BAMLH0A0HYM2',{}).get('value', 3.0)
    vix   = get_val(closes, '^VIX') or 20
    items = []
    items.append("Mercado pricea higher-for-longer" if dgs10 > 4.0 else "Mercado pricea tasas estables/bajando")
    items.append(f"Breakevens sugieren inflacion {'alta' if t5yie>2.5 else 'controlada'} ({t5yie:.2f}%)")
    items.append(f"Curva {'invertida' if t10y2y<0 else 'normal'} ({t10y2y:+.2f}%) -> riesgo de recesion {'alto' if t10y2y<0 else 'moderado'}")
    items.append(f"HY spreads {'contenidos' if hy_s<4.0 else 'estresados'} ({hy_s*100:.0f}bps) -> credito {'estable' if hy_s<4.0 else 'tensionado'}")
    items.append(f"Volatilidad {'alta' if vix>25 else 'normal'} -> risk premia {'altas' if vix>25 else 'estables'}")
    return items

def classify_ism(v):
    if v is None: return None, None
    if v < 50: return "contraccion", "debilitandose"
    if v < 52: return "desaceleracion", "desacelerando"
    return "expansion", "expandiendo"

def build_growth_real(fred):
    lines = []
    ism_m = fred.get('NAPM',{}).get('value')
    ism_s = fred.get('NMFNMI',{}).get('value')
    if ism_m is not None:
        state, direction = classify_ism(ism_m)
        lines.append(f"ISM Manufacturing {ism_m:.1f} -> {state} -> crecimiento {direction}")
    if ism_s is not None:
        state, direction = classify_ism(ism_s)
        lines.append(f"ISM Services {ism_s:.1f} -> {state} -> demanda domestica {direction}")
    if not lines:
        lines.append("ISM Manufacturing: N/D -> crecimiento indeterminado")
    return lines[:3]

def build_liquidity(fred):
    lines = []
    walcl = fred.get('WALCL',{}).get('value')
    walcl_ch = fred.get('WALCL',{}).get('change')
    if walcl is not None:
        direction = "expandiendo" if walcl_ch and walcl_ch > 0 else "contrayendo" if walcl_ch and walcl_ch < 0 else "neutra"
        lines.append(f"Fed balance {walcl/1e6:.2f}T -> {direction} -> liquidez global {'apoya' if direction=='expandiendo' else 'presiona'}")
    m2 = fred.get('M2SL',{}).get('value')
    m2_ch = fred.get('M2SL',{}).get('change')
    if m2 is not None:
        direction = "expandiendo" if m2_ch and m2_ch > 0 else "contrayendo" if m2_ch and m2_ch < 0 else "neutra"
        lines.append(f"M2 {m2:,.0f} -> {direction} -> impulso monetario {direction}")
    if not lines:
        lines.append("Liquidez: N/D -> evaluar por proxies de mercado")
    return lines[:3]

def build_credit_system(fred):
    lines = []
    hy = fred.get('BAMLH0A0HYM2',{}).get('value')
    ig = fred.get('BAMLC0A0CM',{}).get('value')
    if hy is not None:
        risk = "bajo" if hy < 4.0 else "medio" if hy < 6.0 else "alto"
        lines.append(f"HY spreads {hy*100:.0f}bps -> credito {'estable' if hy<4.0 else 'tensionado'} -> riesgo sistemico {risk}")
    if ig is not None:
        risk = "bajo" if ig < 2.0 else "medio" if ig < 3.5 else "alto"
        lines.append(f"IG spreads {ig*100:.0f}bps -> riesgo {risk}")
    if not lines:
        lines.append("Credito: N/D -> sin señal de stress disponible")
    return lines[:3]

def summarize_growth(fred):
    ism_m = fred.get('NAPM',{}).get('value')
    ism_s = fred.get('NMFNMI',{}).get('value')
    vals = [v for v in (ism_m, ism_s) if v is not None]
    if not vals: return "indeterminado"
    avg = sum(vals) / len(vals)
    if avg < 50: return "contraccion"
    if avg < 52: return "desaceleracion"
    return "expansion"

def summarize_liquidity(fred):
    walcl_ch = fred.get('WALCL',{}).get('change')
    if walcl_ch is None: return "indeterminado"
    return "expandiendo" if walcl_ch > 0 else "contrayendo" if walcl_ch < 0 else "neutra"

def summarize_credit(fred):
    hy = fred.get('BAMLH0A0HYM2',{}).get('value')
    if hy is None: return "indeterminado"
    return "estable" if hy < 4.0 else "tensionado"

def build_bias_tactico(closes, fred):
    dgs10 = fred.get('DGS10',{}).get('value', 4.2)
    oil_ytd = ytd_ret(closes['CL=F'].dropna()) if 'CL=F' in closes.columns else 0
    real_yields_high = dgs10 > 4.0
    bias_eq = "neutral-bajista" if (real_yields_high or oil_ytd > 20) else "neutral"
    bias_gold = "debil" if real_yields_high else "neutral"
    btc = "debil estructural, potencial rebote tactico" if real_yields_high else "neutral"
    return [
        f"Equities: {bias_eq} (tasas reales + oil)",
        f"Gold: {bias_gold} (real yields)",
        f"BTC: {btc}"
    ]

def build_key_signal(closes):
    oil_ytd = ytd_ret(closes['CL=F'].dropna()) if 'CL=F' in closes.columns else 0
    vix = get_val(closes,'^VIX') or 20
    if oil_ytd > 20:
        return f"Oil {oil_ytd:+.0f}% YTD -> driver dominante del regimen"
    return f"VIX {vix:.0f} -> señal dominante de stress"

def build_wwcm(closes, fred):
    oil = get_val(closes, 'CL=F')
    vix = get_val(closes, '^VIX') or 20
    t5y = fred.get('T5YIE', {}).get('value', 2.5)
    items = []
    if oil:
        items.append(f"Oil < ${oil-10:.0f} -> invalida shock inflacionario")
    items.append("VIX > 35 -> capitulacion real y reset de risk")
    items.append("HY > 500bps -> stress crediticio real")
    if fred.get('WALCL',{}).get('change') is not None:
        items.append("Fed balance expandiendo -> cambia sesgo de liquidez")
    items.append("Breakevens < 2.2% -> desinflacion clara, cambia regimen")
    return items

def build_wwcm_groq(closes, fred, regime=None, drivers=None, tensions=None):
    """WWCM generado por Groq, adaptado al regimen del dia. Fallback rule-based."""
    if not os.environ.get("GROQ_API_KEY", "") or regime is None:
        return build_wwcm(closes, fred)

    oil    = get_val(closes, 'CL=F')
    vix    = get_val(closes, '^VIX') or 20
    dgs10  = fred.get('DGS10',{}).get('value', 4.2)
    t5yie  = fred.get('T5YIE',{}).get('value', 2.5)
    hy     = fred.get('BAMLH0A0HYM2',{}).get('value', 3.0)
    drivers_txt  = " | ".join(f"{d['label']}: {d['detail']}" for d in (drivers or [])[:3])
    tensions_txt = " | ".join(tensions[:2]) if tensions else "Sin tensiones"

    prompt = f"""Eres un analista macro de largo plazo. Genera exactamente 5 condiciones concretas que cambiarian tu postura de inversion.

Contexto actual {TODAY}:
- Regimen: {regime['label']} — {regime['desc']}
- Drivers: {drivers_txt}
- Oil: ${oil:.0f} | VIX: {vix:.1f} | 10Y: {dgs10:.2f}% | Infl impl 5Y: {t5yie:.2f}% | HY: {hy*100:.0f}bps
- Tensiones: {tensions_txt}
- Portfolio: VT/AVUV/IAU/IBIT/AVDV (largo plazo, sesgo value + gold + BTC)

Genera 5 condiciones en espanol, una por linea, empezando con "- ".
Cada condicion debe tener: [señal concreta con umbral numerico] -> [que implicaria para el portfolio]
Deben ser especificas al regimen actual ({regime['code']}), no genericas.
Sin introduccion, sin titulos, solo los 5 bullets."""

    text = _groq_call(prompt, max_tokens=400)
    if text:
        items = [l.lstrip("-• *").strip() for l in text.split('\n') if l.strip()]
        if len(items) >= 3:
            print(f"     [Groq] WWCM generado ({len(items)} condiciones)")
            return items[:5]

    return build_wwcm(closes, fred)


def build_portfolio_insights(pdata, total_val, closes):
    insights = []
    if not pdata or total_val <= 0:
        return ["Sin datos de portfolio disponibles."]
    weights = {t: d['val']/total_val for t, d in pdata.items()}
    eq_w = sum(weights.get(t,0) for t in ['VT','AVUV','AVDV'])
    hard_w = weights.get('IAU',0) + weights.get('IBIT',0)
    vix = get_val(closes, '^VIX') or 20
    insights.append(f"Equity heavy ({eq_w*100:.0f}%) -> expuesto a tasas altas")
    insights.append(f"BTC ({weights.get('IBIT',0)*100:.0f}%) -> drawdown alto, depende de liquidez")
    insights.append(f"Gold ({weights.get('IAU',0)*100:.0f}%) -> hedge, pero hoy presionado por real yields")
    if vix > 25:
        insights.append("Volatilidad alta -> rebalanceo gradual sobre entradas agresivas")
    return insights[:3]

def build_portfolio_comment(pdata, total_val):
    if not pdata or total_val <= 0:
        return "Sin datos suficientes para comentario de portfolio."
    weights = {t: d['val']/total_val for t, d in pdata.items()}
    eq_w = sum(weights.get(t,0) for t in ['VT','AVUV','AVDV'])
    btc_w = weights.get('IBIT',0)
    gold_w = weights.get('IAU',0)
    if eq_w >= 0.6 and btc_w >= 0.15:
        return "Comentario: exposicion alta a risk (equities+BTC) en regimen inflation shock; priorizar rebalanceo gradual y mantener liquidez tactica."
    if eq_w >= 0.6 and gold_w < 0.1:
        return "Comentario: equity heavy con hedge limitado; vulnerabilidad a tasas reales, considerar cobertura incremental."
    return "Comentario: exposicion balanceada, pero sensibilidad a oil/tasas sigue siendo el factor dominante."


def build_portfolio_comment_groq(pdata, total_val, regime=None, closes=None, fred=None, tensions=None):
    """Comentario de portfolio generado por Groq. Fallback a reglas si falla."""
    if not os.environ.get("GROQ_API_KEY", "") or regime is None or closes is None or not fred:
        return build_portfolio_comment(pdata, total_val)

    total_inv = sum(PORTFOLIO[t]['avg'] * PORTFOLIO[t]['shares'] for t in PORTFOLIO)
    pnl_p    = (total_val - total_inv) / total_inv * 100 if total_inv else 0
    weights  = {t: d['val']/total_val for t, d in pdata.items()} if total_val > 0 else {}
    vix      = get_val(closes, '^VIX') or 20
    dgs10    = fred.get('DGS10',{}).get('value', 4.2)
    tensions_txt = " | ".join(tensions[:2]) if tensions else "Sin tensiones"

    holdings_txt = "\n".join(
        f"  {t} ({weights.get(t,0)*100:.0f}%): precio ${d['price']:.2f}, PnL {d['pnl_p']:+.1f}%, YTD {d['ytd']:+.1f}%"
        for t, d in pdata.items()
    )

    prompt = f"""Eres un asesor de inversiones cuantitativo de largo plazo. Escribe un comentario de portfolio en 2-3 oraciones en espanol.

Contexto {TODAY}:
- Regimen macro: {regime['label']}
- Portfolio total: ${total_val:,.0f} ({pnl_p:+.1f}% vs costo promedio)
- Holdings:
{holdings_txt}
- VIX: {vix:.1f} | 10Y yield: {dgs10:.2f}%
- Tensiones: {tensions_txt}

Escribe 2-3 oraciones que cubran:
1. Estado actual del portfolio frente al regimen macro
2. Riesgo principal o oportunidad concreta hoy
3. Postura sugerida (mantener / acumular gradual / reducir riesgo)

Sin introduccion ni titulo, solo el texto corrido en espanol."""

    text = _groq_call(prompt, max_tokens=200)
    if text:
        print(f"     [Groq] Portfolio comment generado")
        return text

    return build_portfolio_comment(pdata, total_val)


def build_usdclp_comment_groq(closes, fred, regime=None, tensions=None):
    """Comentario USDCLP generado por Groq. Retorna string o None."""
    if 'USDCLP=X' not in closes.columns:
        return None
    s_clp = closes['USDCLP=X'].dropna()
    if s_clp.empty:
        return None

    clp_val = float(s_clp.iloc[-1])
    clp_d1  = d1_ret(s_clp)
    clp_ytd = ytd_ret(s_clp)
    hg_val  = get_val(closes, 'HG=F')
    hg_ytd  = ytd_ret(closes['HG=F'].dropna()) if 'HG=F' in closes.columns else 0
    dxy_val = get_val(closes, 'DX-Y.NYB')
    dxy_ytd = ytd_ret(closes['DX-Y.NYB'].dropna()) if 'DX-Y.NYB' in closes.columns else 0
    oil_val = get_val(closes, 'CL=F')
    oil_ytd = ytd_ret(closes['CL=F'].dropna()) if 'CL=F' in closes.columns else 0
    vix_val = get_val(closes, '^VIX') or 20
    sp_ytd  = ytd_ret(closes['^GSPC'].dropna()) if '^GSPC' in closes.columns else 0
    ff      = fred.get('FEDFUNDS', {}).get('value', 4.33)

    regime_txt   = regime['label'] if regime else 'Transicion'
    tensions_txt = " | ".join(tensions[:2]) if tensions else "Sin tensiones dominantes"

    hg_str  = f"${hg_val:.3f}/lb ({hg_ytd:+.1f}% YTD)" if hg_val else "N/D"
    dxy_str = f"{dxy_val:.1f} ({dxy_ytd:+.1f}% YTD)" if dxy_val else "N/D"
    oil_str = f"${oil_val:.1f} ({oil_ytd:+.1f}% YTD)" if oil_val else "N/D"

    prompt = f"""Eres un analista de divisas especializado en mercados emergentes latinoamericanos. Genera un comentario sobre el USDCLP.

Datos actuales ({TODAY}):
- USDCLP: {clp_val:.0f} ({clp_d1:+.1f}% hoy, {clp_ytd:+.1f}% YTD)
- Cobre HG=F: {hg_str} — Chile exporta cobre: cobre alto fortalece CLP (baja USDCLP)
- DXY: {dxy_str} — USD fuerte sube USDCLP
- Oil WTI: {oil_str} — Chile importa petroleo: oil alto debilita CLP
- VIX: {vix_val:.1f} — riesgo alto sube USDCLP (flight to USD)
- S&P 500 YTD: {sp_ytd:+.1f}% — risk-on fortalece CLP
- Fed Funds: {ff:.2f}% — tasa alta sostiene USD fuerte
- Regimen macro global: {regime_txt}
- Tensiones: {tensions_txt}

Escribe 2-3 oraciones en espanol cubriendo:
1. Factor dominante que explica el nivel actual de USDCLP
2. Expectativa de corto plazo (1-4 semanas) con rango orientativo
3. Riesgo principal (al alza o baja) a monitorear

Sin introduccion ni titulo, directo al punto."""

    text = _groq_call(prompt, max_tokens=220)
    if text:
        print(f"     [Groq] USDCLP comment generado")
        return text
    return None


def interpret_sentiment(cnn, btc, closes):
    vix = get_val(closes, '^VIX') or 20
    s = cnn.get('score'); b = btc.get('score')
    lines = []
    if s:
        if s < 20:
            lines.append(f"CNN {s:.0f} = Extreme Fear -> historicamente +6% a +15% en S&P en 1-3 meses")
            lines.append(f"Condicion: señal funciona si no hay stress crediticio y liquidez no se contrae rapido")
            lines.append(f"PERO: VIX {vix:.0f} activo + tasas altas -> rebotes mas cortos que en ciclos normales")
        elif s < 35:
            lines.append(f"CNN {s:.0f} = Fear -> presion vendedora, sin capitulacion clara todavia")
    if b:
        if b < 25:
            lines.append(f"BTC {b} = Extreme Fear -> niveles post-FTX (Nov 2022) precedieron +70% en 12M")
            lines.append("Caveat: $7.8B outflows ETF desde Nov = presion estructural aun presente")
    return lines

def detect_tensions(closes, fred, cnn):
    gold_d1 = d1_ret(closes['GC=F'].dropna())  if 'GC=F' in closes.columns else 0
    sp_d1   = d1_ret(closes['^GSPC'].dropna()) if '^GSPC' in closes.columns else 0
    gold_ytd= ytd_ret(closes['GC=F'].dropna()) if 'GC=F' in closes.columns else 0
    vix     = get_val(closes, '^VIX') or 20
    sp_ytd  = ytd_ret(closes['^GSPC'].dropna()) if '^GSPC' in closes.columns else 0
    hy_s    = fred.get('BAMLH0A0HYM2',{}).get('value', 3.0)
    cnn_s   = cnn.get('score', 50)
    tensions = []
    if cnn_s and cnn_s < 25 and hy_s < 4.0:
        tensions.append(f"Fear extremo (CNN {cnn_s}) pero HY spread {hy_s*100:.0f}bps OK -> no hay crisis crediticia real")
    if gold_d1 is not None and sp_d1 is not None and gold_d1 < -2 and sp_d1 < -1:
        tensions.append(f"Oro {gold_d1:+.1f}% Y equities {sp_d1:+.1f}% caen juntos -> real yields forzando liquidacion de todo")
    if vix > 25 and sp_ytd and sp_ytd > -10:
        tensions.append(f"VIX {vix:.0f} con S&P solo {sp_ytd:.1f}% YTD -> mercado teme pero no capitula. Falta limpieza.")
    if gold_ytd and gold_ytd > 10 and gold_d1 is not None and gold_d1 < -3:
        tensions.append(f"Gold +{gold_ytd:.0f}% YTD pero {gold_d1:+.1f}% hoy -> probably real yields, NO cambio de tesis")
    growth = summarize_growth(fred)
    credit = summarize_credit(fred)
    liq = summarize_liquidity(fred)
    if growth == "contraccion" and credit == "estable":
        tensions.append("Crecimiento real en contraccion pero credito estable -> desaceleracion, no crisis (aun)")
    if liq == "contrayendo" and vix < 25:
        tensions.append("Liquidez en contraccion pero volatilidad no extrema -> complacencia potencial")
    return tensions

def daily_checklist(closes, fred, cnn, btc):
    vix    = get_val(closes, '^VIX') or 20
    sp_d1  = d1_ret(closes['^GSPC'].dropna()) if '^GSPC' in closes.columns else 0
    oil_ytd= ytd_ret(closes['CL=F'].dropna()) if 'CL=F' in closes.columns else 0
    t5yie  = fred.get('T5YIE',{}).get('value', 2.5)
    t10y2y = fred.get('T10Y2Y',{}).get('value', 0.5)
    items  = [
        ('Risk-On / Risk-Off',
         'RISK-OFF' if vix > 22 or (sp_d1 is not None and sp_d1 < -0.5) else 'RISK-ON',
         vix <= 22 and (sp_d1 is None or sp_d1 >= -0.5)),
        ('Inflacion',
         f'SUBIENDO ({t5yie:.2f}% impl.)' if t5yie > 2.4 or oil_ytd > 20 else f'CONTROLADA ({t5yie:.2f}%)',
         not (t5yie > 2.4 or oil_ytd > 20)),
        ('Liquidez Fed',
         'QT — contrayendo balance',
         False),
        ('Volatilidad',
         f'ALTA (VIX {vix:.0f})' if vix > 25 else f'NORMAL (VIX {vix:.0f})',
         vix <= 25),
        ('Curva de rendimiento',
         f'INVERTIDA ({t10y2y:.2f}%)' if t10y2y < 0 else f'NORMAL (+{t10y2y:.2f}%)',
         t10y2y >= 0),
    ]
    return items

def calc_positioning(closes):
    tickers = list(HOLDINGS.keys()) + ['^GSPC', '^IXIC']
    below200, rsi_vals = 0, []
    for t in tickers:
        if t not in closes.columns: continue
        s = closes[t].dropna()
        if len(s) < 200: continue
        e200 = float(ema(s,200).iloc[-1])
        p    = float(s.iloc[-1])
        if p < e200: below200 += 1
        rsi_vals.append((t, float(calc_rsi(s).iloc[-1])))
    total = len([t for t in tickers if t in closes.columns])
    pct_b = below200 / total * 100 if total else 0
    avg_r = sum(v for _,v in rsi_vals) / len(rsi_vals) if rsi_vals else 50
    return {'pct_below_200': pct_b, 'avg_rsi': avg_r, 'rsi_vals': rsi_vals,
            'label': 'Sobrevendido' if avg_r < 35 else 'Sobrecomprado' if avg_r > 65 else 'Neutral'}

def interpret_news(title, summary):
    c = (title + ' ' + summary).lower()
    if any(w in c for w in ['iran','hormuz','strait','strike','attack','war']):
        return "Supply shock -> presion estructural en petroleo"
    if any(w in c for w in ['fed','powell','warsh','fomc','rate cut','rate hike','interest rate']):
        return "Senal de politica monetaria -> watch tasas y DXY"
    if any(w in c for w in ['cpi','inflation','price index','pce']):
        return "Dato de inflacion -> define path Fed y multiples"
    if any(w in c for w in ['gold','silver','metals','precious']):
        return "Flujos en metales -> proxy de riesgo sistémico percibido"
    if any(w in c for w in ['bitcoin','btc','crypto','digital asset']):
        return "Crypto sentiment -> risk-on/off gauge digital"
    if any(w in c for w in ['earnings','revenue','profit','guidance','results']):
        return "Salud corporativa -> define apetito de riesgo"
    if any(w in c for w in ['gdp','recession','growth','economic data']):
        return "Actividad economica -> severidad del ciclo"
    if any(w in c for w in ['tariff','trade war','china','import']):
        return "Riesgo comercial -> presion adicional valuaciones"
    if any(w in c for w in ['oil','crude','opec','energy','barrel']):
        return "Suministro energetico -> inflacion y crecimiento"
    return "Watch correlacion con holdings"

def build_tldr(regime, drivers, tensions, closes, cnn, btc, pdata, total_val, fred):
    total_inv = sum(PORTFOLIO[t]['avg']*PORTFOLIO[t]['shares'] for t in PORTFOLIO)
    pnl_p = (total_val - total_inv) / total_inv * 100
    vix   = get_val(closes, '^VIX') or 20
    cnn_s = cnn.get('score')
    lines = []
    lines.append(f"Regimen: {regime['label']} — {drivers[0]['detail'] if drivers else 'multiples drivers'}")
    if len(drivers) > 1:
        lines.append(f"{drivers[1]['label']}: {drivers[1]['detail']}")
    growth = summarize_growth(fred)
    liq = summarize_liquidity(fred)
    credit = summarize_credit(fred)
    lines.append(f"Crecimiento {growth} | Liquidez {liq} | Credito {credit}")
    if cnn_s and cnn_s < 25:
        lines.append(f"Fear extremo (CNN {cnn_s}) -> condiciones historicamente favorables para acumulacion")
    elif cnn_s and cnn_s > 60:
        lines.append(f"Greed alto (CNN {cnn_s}) -> cuidado con entradas agresivas")
    lines.append(f"Portfolio core: ${total_val:,.0f} ({pnl_p:+.1f}%) — {'resistiendo bien' if pnl_p > -5 else 'bajo presion'}")
    if tensions:
        lines.append(f"Contradiccion: {tensions[0]}")
    # Asegura 3-5 lineas para TL;DR
    if len(lines) < 3:
        lines.append(f"Drivers mixtos -> sin catalizador unico, paciencia en sizing")
    if len(lines) < 3 and btc.get('score'):
        lines.append(f"BTC F&G {btc.get('score')} -> proxy de apetito risk digital")
    return lines[:5]


def _groq_call(prompt, max_tokens=500):
    """Llama a Groq API. Retorna texto o None si falla/no hay key."""
    api_key = os.environ.get("GROQ_API_KEY", "")
    model   = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
    if not api_key:
        return None
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={"model": model, "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": max_tokens, "temperature": 0.3},
            timeout=20,
        )
        if not r.ok:
            print(f"  WARNING: Groq {r.status_code} — {r.text[:120]}")
            return None
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"  WARNING: Groq error ({e})")
        return None


def build_tldr_grok(regime, drivers, tensions, closes, cnn, btc, pdata, total_val, fred):
    """TL;DR generado por Groq. Fallback a reglas si no hay API key o falla la llamada."""
    if not os.environ.get("GROQ_API_KEY", ""):
        return build_tldr(regime, drivers, tensions, closes, cnn, btc, pdata, total_val, fred)

    vix      = get_val(closes, '^VIX') or 20
    oil_ytd  = ytd_ret(closes['CL=F'].dropna())   if 'CL=F'  in closes.columns else 0
    sp_ytd   = ytd_ret(closes['^GSPC'].dropna())  if '^GSPC' in closes.columns else 0
    gold_ytd = ytd_ret(closes['GC=F'].dropna())   if 'GC=F'  in closes.columns else 0
    cnn_s    = cnn.get('score', 'N/D')
    btc_s    = btc.get('score', 'N/D')
    total_inv = sum(PORTFOLIO[t]['avg'] * PORTFOLIO[t]['shares'] for t in PORTFOLIO)
    pnl_p    = (total_val - total_inv) / total_inv * 100 if total_inv else 0

    drivers_txt  = " | ".join(f"{d['label']}: {d['detail']}" for d in drivers[:3]) or "Sin drivers dominantes"
    tensions_txt = " | ".join(tensions[:2]) if tensions else "Sin tensiones dominantes"

    prompt = f"""Eres un analista financiero cuantitativo de largo plazo. Genera un TL;DR del dia en exactamente 4 bullets concisos en espanol.

Datos del dia {TODAY}:
- Regimen: {regime['label']} — {regime['desc']}
- Drivers: {drivers_txt}
- VIX: {vix:.1f} | Oil YTD: {oil_ytd:+.1f}% | S&P 500 YTD: {sp_ytd:+.1f}% | Gold YTD: {gold_ytd:+.1f}%
- Sentimiento: CNN Fear&Greed {cnn_s} | BTC Fear&Greed {btc_s}
- Crecimiento: {summarize_growth(fred)} | Liquidez: {summarize_liquidity(fred)} | Credito: {summarize_credit(fred)}
- Portfolio core: ${total_val:,.0f} ({pnl_p:+.1f}% vs costo)
- Tensiones: {tensions_txt}

Reglas estrictas:
- Exactamente 4 bullets, uno por linea, empezando con "- "
- Sin introduccion, sin cierre, sin titulos
- Cada bullet es una oracion directa y accionable
- Enfocado en implicaciones para un inversor de largo plazo con VT/AVUV/IAU/IBIT/AVDV
- El ultimo bullet debe ser la accion o postura sugerida para hoy"""

    try:
        text = _groq_call(prompt, max_tokens=400)
        lines = [l.lstrip("-• *").strip() for l in text.split('\n') if l.strip()] if text else []
        if lines:
            print(f"     [Groq] TL;DR generado ({len(lines)} bullets)")
            return lines[:5]
    except Exception as e:
        print(f"  WARNING: Groq TL;DR error ({e})")

    return build_tldr(regime, drivers, tensions, closes, cnn, btc, pdata, total_val, fred)


def build_alerts_meaning(closes, cnn, fred, pdata):
    alerts = []
    vix  = get_val(closes,'^VIX') or 20
    for t, info in PORTFOLIO.items():
        if t not in pdata: continue
        d = pdata[t]; e200 = d['ema200']
        if d['price'] < e200:
            if t == 'IBIT':
                alerts.append(("IBIT bajo EMA200",
                    "BTC en tendencia bajista estructural — no lidera risk assets. Acumulacion valida si tesis intacta."))
            else:
                alerts.append((f"{t} bajo EMA200",
                    f"Tendencia negativa en {t} — peso puede seguir bajando a corto plazo."))
    cnn_s = cnn.get('score')
    if cnn_s and cnn_s < 25:
        alerts.append(("CNN F&G Extreme Fear",
            f"Nivel {cnn_s}. Historicamente +8% en S&P en 3M. Contexto: VIX {vix:.0f} modera magnitud."))
    umcsent = fred.get('UMCSENT',{}).get('value',80)
    if umcsent < 60:
        alerts.append(("Confianza consumidor en nivel crisis",
            f"Michigan {umcsent} — demanda futura en riesgo. Riesgo recesivo independiente del shock de petroleo."))
    if vix > 30:
        alerts.append(("VIX > 30",
            "Mercado en modo panico activo. Opciones caras — no es momento de vender, es momento de revisar tesis."))
    return alerts

def save_narrative_log(regime, drivers, closes, cnn, btc):
    log = os.path.join(DATA_DIR, "narrative_log.csv")
    row = {'date': TODAY, 'regime': regime['code'], 'regime_label': regime['label'],
           'driver_1': drivers[0]['label'] if drivers else '',
           'vix':      round(get_val(closes,'^VIX') or 0, 1),
           'oil_ytd':  round(ytd_ret(closes['CL=F'].dropna()) if 'CL=F' in closes.columns else 0, 1),
           'gold_ytd': round(ytd_ret(closes['GC=F'].dropna()) if 'GC=F' in closes.columns else 0, 1),
           'sp500_ytd':round(ytd_ret(closes['^GSPC'].dropna()) if '^GSPC' in closes.columns else 0, 1),
           'cnn_fg':   cnn.get('score'), 'btc_fg': btc.get('score')}
    df_new = pd.DataFrame([row])
    if os.path.exists(log):
        df = pd.read_csv(log)
        df = df[df['date'] != TODAY]
        df = pd.concat([df, df_new], ignore_index=True)
    else: df = df_new
    df.to_csv(log, index=False)

def read_narrative_log(max_rows=7):
    log = os.path.join(DATA_DIR, "narrative_log.csv")
    if not os.path.exists(log): return []
    try:
        df = pd.read_csv(log)
        df = df.sort_values('date')
        tail = df.tail(max_rows)
        rows = []
        for _, r in tail.iterrows():
            rows.append((r.get('date',''), r.get('regime_label',''), r.get('driver_1','')))
        return rows
    except:
        return []


# ── PDF BUILDER ───────────────────────────────────────────────────
class PDF(FPDF):
    def normalize_text(self, text): return clean(text)

    def header(self):
        self.set_fill_color(12, 35, 75)
        self.rect(0, 0, 210, 16, 'F')
        self.set_font('Helvetica','B', 12)
        self.set_text_color(255,255,255)
        self.set_xy(7, 3)
        self.cell(130, 10, f"The Global Compounder  |  Daily Briefing")
        self.set_font('Helvetica','', 9)
        self.set_text_color(180,200,255)
        self.cell(0, 10, TODAY, align='R')
        self.set_text_color(0,0,0)
        # Más separación para evitar que el header tape el texto
        self.ln(14)

    def footer(self):
        self.set_y(-11)
        self.set_font('Helvetica','I', 6.5)
        self.set_text_color(150,150,150)
        self.cell(0, 5, f"Generado {datetime.now().strftime('%Y-%m-%d %H:%M')} | yfinance + FRED + CNN F&G + RSS | Solo uso personal | Pag {self.page_no()}", align='C')

    def sec(self, icon, title, color=(12,35,75)):
        self.ln(2)
        self.set_fill_color(*color)
        self.set_font('Helvetica','B', 9)
        self.set_text_color(255,255,255)
        self.cell(0, 6.5, f"  {icon}  {title}", ln=True, fill=True)
        self.set_text_color(0,0,0)
        self.ln(1)

    def bullet(self, text, indent=8, size=8):
        self.set_font('Helvetica','', size)
        self.set_x(indent)
        self.multi_cell(0, 5, f"- {text}")

    def kv2(self, k, v, green=None, indent=8, bold_v=False):
        self.set_font('Helvetica','', 8)
        self.set_x(indent)
        self.cell(50, 5.2, k)
        self.set_font('Helvetica','B' if bold_v else '', 8)
        if green is True:   self.set_text_color(0,140,0)
        elif green is False: self.set_text_color(180,0,0)
        self.cell(0, 5.2, str(v), ln=True)
        self.set_text_color(0,0,0)


def color_num(val):
    try:
        v = float(str(val).replace('%','').replace('$','').replace('+',''))
        return (0,140,0) if v >= 0 else (180,0,0)
    except: return (0,0,0)


def build_pdf(closes, cnn, btc, fred, news, regime, drivers, chains,
              sent_lines, tensions, checklist, positioning, tldr,
              pdata, total_val, v3=None, include_3m=True, portfolio_comment=None, wwcm_items=None,
              usdclp_comment=None):

    pdf = PDF(orientation='P', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=True, margin=13)
    pdf.set_margins(7, 20, 7)

    total_inv = sum(PORTFOLIO[t]['avg']*PORTFOLIO[t]['shares'] for t in PORTFOLIO)
    total_pnl = total_val - total_inv

    # ════════════════════════════════════════════
    # PAGINA 1 — ANALYSIS
    # ════════════════════════════════════════════
    pdf.add_page()

    # [T] TL;DR
    pdf.sec("[T]", "TL;DR DEL DIA", color=(40,40,40))
    for line in tldr:
        pdf.set_font('Helvetica','', 8.5)
        pdf.set_x(8)
        pdf.set_text_color(30,30,30)
        pdf.multi_cell(0, 5.5, clean(f"  {line}"))
    pdf.set_text_color(0,0,0)
    pdf.ln(1)

    # [R] Regimen macro
    pdf.sec("[R]", "REGIMEN MACRO ACTUAL", color=regime['color'])
    pdf.set_font('Helvetica','B', 9)
    pdf.set_x(8)
    pdf.set_text_color(*regime['color'])
    pdf.cell(0, 6, clean(regime['label']), ln=True)
    pdf.set_text_color(60,60,60)
    pdf.set_font('Helvetica','', 8)
    pdf.set_x(8)
    pdf.multi_cell(0, 5, clean(regime['desc']))
    pdf.set_text_color(0,0,0)

    # [G] Crecimiento real
    pdf.sec("[G]", "CRECIMIENTO REAL")
    for line in build_growth_real(fred):
        pdf.set_font('Helvetica','', 8); pdf.set_x(8)
        pdf.multi_cell(0, 5.2, clean(line))

    # [L] Liquidez global
    pdf.sec("[L]", "LIQUIDEZ GLOBAL")
    for line in build_liquidity(fred):
        pdf.set_font('Helvetica','', 8); pdf.set_x(8)
        pdf.multi_cell(0, 5.2, clean(line))

    # [CR] Sistema crediticio
    pdf.sec("[CR]", "SISTEMA CREDITICIO")
    for line in build_credit_system(fred):
        pdf.set_font('Helvetica','', 8); pdf.set_x(8)
        pdf.multi_cell(0, 5.2, clean(line))

    # Drivers
    pdf.sec("[D]", "DRIVERS ACTIVOS (ranking)")
    impact_label = {'HIGH':'ALTO','MED':'MEDIO','POSITIVE':'POSITIVO'}
    for d in drivers[:5]:
        pdf.set_x(8)
        pdf.set_font('Helvetica','B', 8)
        pdf.set_text_color(*d['color'])
        pdf.cell(8, 5.5, clean(d['icon']))
        pdf.set_text_color(0,0,0)
        pdf.cell(62, 5.5, clean(d['label']))
        pdf.set_font('Helvetica','I', 7.5)
        pdf.set_text_color(80,80,80)
        pdf.multi_cell(0, 5.5, clean(d['detail']))
        pdf.set_text_color(0,0,0)

    # [SCE] Escenarios
    pdf.sec("[SCE]", "ESCENARIOS (probabilidad subjetiva)")
    for p, s in build_scenarios(closes, fred, cnn):
        pdf.set_font('Helvetica','', 8)
        pdf.set_x(8)
        pdf.multi_cell(0, 5.2, clean(f"{p} -> {s}"))
    pdf.set_font('Helvetica','I', 7.5); pdf.set_text_color(90,90,90)
    pdf.set_x(8); pdf.multi_cell(0, 4.8, clean(f"Conviccion: {build_conviction(closes, fred, cnn)}"))
    pdf.set_text_color(0,0,0)

    # Causal chains
    pdf.sec("[->]", "MAPA DE RELACIONES (causalidad + loop)")
    for ch in chains:
        pdf.set_font('Helvetica','', 8)
        pdf.set_x(8)
        pdf.multi_cell(0, 5.2, clean(ch))
    fb = build_feedback_loop(closes, fred)
    pdf.set_font('Helvetica','I', 7.5); pdf.set_text_color(90,90,90)
    pdf.set_x(8); pdf.multi_cell(0, 4.8, clean(f"Loop: {fb}"))
    pdf.set_text_color(0,0,0)

    # Tensiones
    if tensions:
        pdf.sec("[!]", "TENSIONES DEL MERCADO", color=(150,80,0))
        for t in tensions:
            pdf.set_font('Helvetica','', 8)
            pdf.set_x(8)
            pdf.multi_cell(0, 5.2, clean(t))

    # [E] Expectativas
    pdf.sec("[E]", "EXPECTATIVAS DEL MERCADO (pricing)")
    for it in build_expectations(closes, fred):
        pdf.set_font('Helvetica','', 8); pdf.set_x(8)
        pdf.multi_cell(0, 5.2, clean(it))

    # [BIAS] Direccionalidad implicita
    pdf.sec("[BIAS]", "BIAS TACTICO (1-4 semanas)")
    for it in build_bias_tactico(closes, fred):
        pdf.set_font('Helvetica','', 8); pdf.set_x(8)
        pdf.multi_cell(0, 5.2, clean(it))

    # [SIG] Señal clave
    pdf.sec("[SIG]", "SENAL MAS IMPORTANTE DEL DIA")
    pdf.set_font('Helvetica','', 8); pdf.set_x(8)
    pdf.multi_cell(0, 5.2, clean(build_key_signal(closes)))

    # Sentimiento interpretado
    pdf.sec("[S]", "SENTIMIENTO — INTERPRETACION")
    cnn_s = cnn.get('score'); btc_s = btc.get('score')
    def fmt_opt(v, fmt):
        return fmt.format(v) if isinstance(v, (int, float)) else "N/A"
    def fg_rgb(s):
        if not s: return (100,100,100)
        if s < 25: return (200,0,0)
        if s < 45: return (200,120,0)
        if s < 55: return (100,100,100)
        if s < 75: return (0,150,0)
        return (0,80,200)
    pdf.set_x(8)
    pdf.set_font('Helvetica','', 8)
    pdf.cell(45, 5.5, "CNN Fear&Greed (equities):")
    pdf.set_font('Helvetica','B', 8.5)
    pdf.set_text_color(*fg_rgb(cnn_s))
    pdf.cell(35, 5.5, f"{cnn_s} - {cnn.get('rating','N/A').upper()}" if cnn_s else "N/A")
    pdf.set_font('Helvetica','I', 7.5)
    pdf.set_text_color(100,100,100)
    change_s = fmt_opt(cnn.get('change'), "{:+.1f}")
    prev_1w = cnn.get('prev_1w', 'N/A') if cnn.get('prev_1w') is not None else "N/A"
    prev_1m = cnn.get('prev_1m', 'N/A') if cnn.get('prev_1m') is not None else "N/A"
    pdf.cell(0, 5.5, f"  hoy {change_s}  |  1W: {prev_1w}  |  1M: {prev_1m}", ln=True)
    pdf.set_x(8)
    pdf.set_font('Helvetica','', 8)
    pdf.set_text_color(0,0,0)
    pdf.cell(45, 5.5, "BTC Fear&Greed (crypto):")
    pdf.set_font('Helvetica','B', 8.5)
    pdf.set_text_color(*fg_rgb(btc_s))
    pdf.cell(35, 5.5, f"{btc_s} - {btc.get('rating','N/A').upper()}" if btc_s else "N/A")
    pdf.set_font('Helvetica','I', 7.5)
    pdf.set_text_color(100,100,100)
    pdf.cell(0, 5.5, f"  ayer: {btc.get('prev','?')}", ln=True)
    pdf.set_text_color(0,0,0)
    pdf.ln(1)
    for line in sent_lines:
        pdf.set_font('Helvetica','', 8)
        pdf.set_x(8)
        pdf.multi_cell(0, 5, clean(line))

    # Checklist
    pdf.sec("[C]", "CHECKLIST DEL SISTEMA")
    for label, val, green in checklist:
        pdf.set_x(8)
        pdf.set_font('Helvetica','', 8)
        pdf.cell(50, 5.5, clean(label))
        pdf.set_font('Helvetica','B', 8)
        pdf.set_text_color(0,140,0 if green else (200,0,0)[0])
        if green: pdf.set_text_color(0,140,0)
        else:     pdf.set_text_color(180,0,0)
        pdf.cell(0, 5.5, clean(val), ln=True)
        pdf.set_text_color(0,0,0)

    # Posicionamiento
    pdf.sec("[P]", "POSICIONAMIENTO DEL MERCADO")
    pos_label_c = (180,0,0) if positioning['pct_below_200'] > 50 else (200,130,0) if positioning['pct_below_200'] > 30 else (0,140,0)
    rsi_label_c = (180,0,0) if positioning['avg_rsi'] > 65 else (0,140,0) if positioning['avg_rsi'] < 35 else (100,100,100)
    pdf.set_x(8); pdf.set_font('Helvetica','', 8)
    pdf.cell(65, 5.5, f"Tickers bajo EMA200 (proxy breadth):")
    pdf.set_font('Helvetica','B', 8); pdf.set_text_color(*pos_label_c)
    pdf.cell(0, 5.5, f"{positioning['pct_below_200']:.0f}%", ln=True)
    pdf.set_text_color(0,0,0); pdf.set_x(8); pdf.set_font('Helvetica','', 8)
    pdf.cell(65, 5.5, f"RSI promedio holdings:")
    pdf.set_font('Helvetica','B', 8); pdf.set_text_color(*rsi_label_c)
    pdf.cell(0, 5.5, f"{positioning['avg_rsi']:.1f} — {positioning['label']}", ln=True)
    pdf.set_text_color(0,0,0)

    # ════════════════════════════════════════════
    # PAGINA 2 — DATA
    # ════════════════════════════════════════════
    pdf.add_page()

    # Portfolio tabla
    pdf.sec("[O]", "PORTFOLIO")
    pnl_c = (0,140,0) if total_pnl >= 0 else (180,0,0)
    pdf.set_x(7); pdf.set_font('Helvetica','B', 9)
    pdf.set_text_color(*pnl_c)
    pdf.cell(0, 6, f"Core: ${total_val:,.2f}  |  PnL: ${total_pnl:+,.2f} ({total_pnl/total_inv*100:+.1f}%)", ln=True)
    pdf.set_text_color(0,0,0)
    for it in build_portfolio_insights(pdata, total_val, closes):
        pdf.set_x(7); pdf.set_font('Helvetica','', 7.5)
        pdf.set_text_color(70,70,70); pdf.cell(0, 4.8, clean(f"- {it}"), ln=True)
    pdf.set_text_color(0,0,0)

    # Comentario de portfolio (2da pasada)
    pdf.sec("[OP]", "COMENTARIO DE PORTFOLIO (2da pasada)")
    pdf.set_x(7); pdf.set_font('Helvetica','', 7.5)
    _pc = portfolio_comment if portfolio_comment else build_portfolio_comment(pdata, total_val)
    pdf.multi_cell(0, 5.0, clean(_pc))

    cols   = ['Ticker','Precio','1D%','YTD%','EMA200%','ATH%','PnL$','Peso%']
    widths = [17, 22, 17, 17, 20, 18, 22, 17]
    pdf.set_fill_color(190,205,230); pdf.set_font('Helvetica','B', 7.5); pdf.set_x(7)
    for c,w in zip(cols,widths): pdf.cell(w, 5.5, c, fill=True, align='C')
    pdf.ln()
    alt = False
    for t in HOLDINGS:
        if t not in pdata: continue
        d = pdata[t]; bg = (245,248,253) if alt else None; alt = not alt
        price = d['price']; e200 = d['ema200']
        vs200 = (price - e200) / e200 * 100
        w_cur = d['val'] / total_val * 100
        row = [t, f"${price:.2f}", f"{d['d1']:+.1f}%", f"{d['ytd']:+.1f}%",
               f"{vs200:+.1f}%", f"{d['ath_p']:+.1f}%", f"${d['pnl_d']:+.0f}", f"{w_cur:.1f}%"]
        pdf.set_x(7)
        if bg: pdf.set_fill_color(*bg)
        pdf.set_font('Helvetica','B', 7.5)
        pdf.cell(widths[0], 5.5, row[0], fill=bool(bg))
        for i in range(1, len(row)):
            pdf.set_font('Helvetica','', 7.5)
            if i in (2,3,4,5,6):
                try:
                    v = float(row[i].replace('%','').replace('$','').replace('+',''))
                    pdf.set_text_color(*((0,140,0) if v>=0 else (180,0,0)))
                except: pass
            pdf.cell(widths[i], 5.5, row[i], fill=bool(bg), align='C')
            pdf.set_text_color(0,0,0)
        pdf.ln()

    # Legacy
    pdf.set_font('Helvetica','I', 7.5); pdf.set_x(7); pdf.set_text_color(100,100,100)
    leg_parts, leg_total = [], 0
    for t, info in LEGACY.items():
        if t not in closes.columns: continue
        p = float(closes[t].dropna().iloc[-1]); v = p * info['shares']; leg_total += v
        leg_parts.append(f"{t}: ${p:.2f} ({(p-info['avg'])/info['avg']*100:+.1f}%)")
    pdf.cell(0, 5, f"Legacy a liquidar: {' | '.join(leg_parts)} = ${leg_total:.0f}", ln=True)
    pdf.set_text_color(0,0,0)

    # Mercado
    pdf.sec("[M]", "MERCADO")
    cols2 = ['Indicador','Valor','1D%','YTD%']; widths2 = [38,30,27,27]
    pdf.set_fill_color(190,205,230); pdf.set_font('Helvetica','B', 7.5); pdf.set_x(7)
    for c,w in zip(cols2,widths2): pdf.cell(w, 5.5, c, fill=True, align='C')
    pdf.ln(); alt = False
    for t, label in MACRO_TICKERS.items():
        if t not in closes.columns: continue
        s = closes[t].dropna(); p = float(s.iloc[-1])
        d1 = d1_ret(s); ytd = ytd_ret(s)
        bg = (245,248,253) if alt else None; alt = not alt
        if bg: pdf.set_fill_color(*bg)
        pdf.set_x(7); pdf.set_font('Helvetica','', 7.5)
        pdf.cell(widths2[0], 5.5, label, fill=bool(bg))
        pdf.cell(widths2[1], 5.5, f"{p:.2f}", fill=bool(bg), align='C')
        for val in (d1, ytd):
            pdf.set_text_color(*((0,140,0) if val and val>=0 else (180,0,0)))
            pdf.cell(widths2[2], 5.5, f"{val:+.1f}%" if val is not None else "N/A", fill=bool(bg), align='C')
            pdf.set_text_color(0,0,0)
        pdf.ln()

    # [CLP] USDCLP / Peso Chileno
    if 'USDCLP=X' in closes.columns and not closes['USDCLP=X'].dropna().empty:
        pdf.sec("[CLP]", "USDCLP / PESO CHILENO", color=(40,80,140))
        s_clp   = closes['USDCLP=X'].dropna()
        clp_val = float(s_clp.iloc[-1])
        clp_d1  = d1_ret(s_clp)
        clp_ytd = ytd_ret(s_clp)
        # Linea resumen
        pdf.set_x(8); pdf.set_font('Helvetica','B', 8.5)
        d1_col = (0,140,0) if clp_d1 and clp_d1 < 0 else (180,0,0)  # CLP se fortalece si USDCLP baja
        pdf.set_text_color(0,0,0)
        pdf.cell(32, 5.5, f"USDCLP: {clp_val:.0f}")
        pdf.set_text_color(*d1_col)
        pdf.cell(28, 5.5, f"1D: {clp_d1:+.1f}%" if clp_d1 is not None else "1D: N/A")
        pdf.set_text_color(0,0,0)
        pdf.cell(0, 5.5, f"YTD: {clp_ytd:+.1f}%" if clp_ytd is not None else "", ln=True)
        # Tabla de factores
        pdf.ln(1)
        fac_cols = ['Factor', 'Valor', 'YTD%', 'Efecto en CLP']; fac_w = [38,28,22,34]
        pdf.set_fill_color(190,205,230); pdf.set_font('Helvetica','B', 7); pdf.set_x(8)
        for c, ww in zip(fac_cols, fac_w): pdf.cell(ww, 5, c, fill=True, align='C')
        pdf.ln()
        clp_factors = []
        hg_val  = get_val(closes, 'HG=F')
        hg_ytd  = ytd_ret(closes['HG=F'].dropna()) if 'HG=F' in closes.columns else None
        dxy_val = get_val(closes, 'DX-Y.NYB')
        dxy_ytd = ytd_ret(closes['DX-Y.NYB'].dropna()) if 'DX-Y.NYB' in closes.columns else None
        oil_val = get_val(closes, 'CL=F')
        oil_ytd = ytd_ret(closes['CL=F'].dropna()) if 'CL=F' in closes.columns else None
        vix_val = get_val(closes, '^VIX') or 20
        vix_ytd = ytd_ret(closes['^VIX'].dropna()) if '^VIX' in closes.columns else None
        sp_ytd  = ytd_ret(closes['^GSPC'].dropna()) if '^GSPC' in closes.columns else None
        ff_val  = fred.get('FEDFUNDS', {}).get('value', None)
        if hg_val:  clp_factors.append(("Cobre HG=F",  f"${hg_val:.3f}/lb", f"{hg_ytd:+.1f}%" if hg_ytd is not None else "N/A", "alto = CLP +"))
        if dxy_val: clp_factors.append(("DXY",         f"{dxy_val:.1f}",   f"{dxy_ytd:+.1f}%" if dxy_ytd is not None else "N/A", "alto = CLP -"))
        if oil_val: clp_factors.append(("Oil WTI",     f"${oil_val:.1f}",  f"{oil_ytd:+.1f}%" if oil_ytd is not None else "N/A", "alto = CLP -"))
        clp_factors.append(("VIX",          f"{vix_val:.1f}",   f"{vix_ytd:+.1f}%" if vix_ytd is not None else "N/A", "alto = CLP -"))
        if sp_ytd is not None: clp_factors.append(("S&P 500",  "YTD",  f"{sp_ytd:+.1f}%", "sube = CLP +"))
        if ff_val:  clp_factors.append(("Fed Funds",   f"{ff_val:.2f}%",   "---",           "alto = CLP -"))
        alt2 = False
        for fname, fval, fytd, feff in clp_factors:
            bg = (245,248,253) if alt2 else None; alt2 = not alt2
            if bg: pdf.set_fill_color(*bg)
            pdf.set_x(8); pdf.set_font('Helvetica','', 7)
            pdf.cell(fac_w[0], 5, clean(fname), fill=bool(bg))
            pdf.cell(fac_w[1], 5, clean(fval), fill=bool(bg), align='C')
            pdf.cell(fac_w[2], 5, clean(fytd), fill=bool(bg), align='C')
            pdf.set_text_color(0,100,0) if 'CLP +' in feff else pdf.set_text_color(160,0,0)
            pdf.cell(fac_w[3], 5, clean(feff), fill=bool(bg), align='C', ln=True)
            pdf.set_text_color(0,0,0)
        # Comentario Groq
        if usdclp_comment:
            pdf.ln(1); pdf.set_x(8); pdf.set_font('Helvetica','I', 7.5)
            pdf.set_text_color(40,60,100)
            pdf.multi_cell(0, 4.8, clean(usdclp_comment))
            pdf.set_text_color(0,0,0)
        pdf.ln(1)

    # FRED
    pdf.sec("[F]", "MACRO (FRED)")
    fred_keys = [('FEDFUNDS','Fed Funds','%'),('T10Y2Y','Spread 10Y-2Y','%'),
                 ('DGS10','10Y Yield','%'),('UNRATE','Desempleo','%'),
                 ('T5YIE','Inflacion impl 5Y','%'),('BAMLH0A0HYM2','HY Spread','bps'),
                 ('UMCSENT','Conf. consumidor',''),('WALCL','Balance Fed','T')]
    items_f = [(l,fred[k]['value'],u,fred[k]['date']) for k,l,u in fred_keys if k in fred]
    for i in range(0, len(items_f), 2):
        pdf.set_x(7)
        for j in range(2):
            if i+j >= len(items_f): break
            label,val,unit,date = items_f[i+j]
            if unit=='bps':    vs = f"{val*100:.0f}bps"
            elif unit=='T':    vs = f"${val/1e6:.2f}T"
            elif unit=='%':    vs = f"{val:.2f}%"
            else:              vs = f"{val:,.0f}"
            pdf.set_font('Helvetica','',7.5); pdf.cell(35,5.2,label+':')
            pdf.set_font('Helvetica','B',7.5); pdf.cell(22,5.2,vs)
            pdf.set_font('Helvetica','I',6.5); pdf.set_text_color(130,130,130)
            pdf.cell(38,5.2,date)
            pdf.set_text_color(0,0,0)
        pdf.ln()

    # Noticias interpretadas
    pdf.sec("[N]", "NOTICIAS INTERPRETADAS")
    for n in news:
        pdf.set_x(7); pdf.set_font('Helvetica','B', 7.8)
        title_t = clean(n['title'])[:85] + ('...' if len(n['title'])>85 else '')
        pdf.multi_cell(0, 5.2, title_t)
        pdf.set_x(10); pdf.set_font('Helvetica','I', 7)
        pdf.set_text_color(80,80,80)
        interp = interpret_news(n['title'], n['summary'])
        pdf.cell(0, 4.5, f"[{n['source']}]  -> {clean(interp)}", ln=True)
        pdf.set_text_color(0,0,0)
        pdf.ln(0.5)

    # Alertas
    pdf.sec("[A]", "ALERTAS", color=(160,30,30) if tensions else (12,35,75))

    # Build alerts with meaning
    alerts_meaning = build_alerts_meaning(closes, cnn, fred, pdata)

    if alerts_meaning:
        for title_a, meaning in alerts_meaning:
            pdf.set_x(7); pdf.set_font('Helvetica','B', 8)
            pdf.set_text_color(180,0,0); pdf.cell(5,5.5,'!')
            pdf.set_text_color(0,0,0); pdf.cell(0,5.5,clean(title_a),ln=True)
            pdf.set_x(12); pdf.set_font('Helvetica','',7.5)
            pdf.set_text_color(60,60,60); pdf.multi_cell(0,4.8,clean(meaning))
            pdf.set_text_color(0,0,0)
    else:
        pdf.set_x(7); pdf.set_font('Helvetica','',8); pdf.set_text_color(0,140,0)
        pdf.cell(0,5.5,'Sin alertas criticas.',ln=True); pdf.set_text_color(0,0,0)

    # Eventos
    pdf.sec("[EV]", "EVENTOS")
    impact_color = {'HIGH':(180,0,0),'MED':(180,120,0),'LOW':(0,130,0)}
    for date_s, name, impact, note in UPCOMING_EVENTS:
        days = (datetime.strptime(date_s,'%Y-%m-%d') - datetime.today()).days
        if not 0 <= days <= 90: continue
        pdf.set_x(7); pdf.set_font('Helvetica','B',7.8)
        pdf.cell(28,5.2,f"{date_s} ({days}d)")
        pdf.set_text_color(*impact_color.get(impact,(100,100,100)))
        pdf.cell(8,5.2,f"[{impact}]")
        pdf.set_text_color(0,0,0); pdf.set_font('Helvetica','',7.8)
        pdf.cell(62,5.2,clean(name))
        pdf.set_font('Helvetica','I',7); pdf.set_text_color(100,100,100)
        pdf.cell(0,5.2,clean(note),ln=True); pdf.set_text_color(0,0,0)

    # [3M] Three month view (al final del briefing)
    if include_3m:
        v3 = v3 or build_three_month_view(closes, fred, drivers)
        pdf.sec("[3M]", "3M VIEW BASED ON CURRENT BRIEF")
        pdf.set_font('Helvetica','', 8); pdf.set_x(8)
        for line in v3['pdf_lines']:
            pdf.set_x(8)
            pdf.multi_cell(0, 5.2, clean(f"- {line}"))
        pdf.set_x(8); pdf.set_font('Helvetica','I', 7.5); pdf.set_text_color(90,90,90)
        pdf.multi_cell(0, 4.8, clean(f"Razon: {v3['reason']}"))
        if v3['changed']:
            pdf.set_x(8); pdf.multi_cell(0, 4.8, clean(f"Cambio vs previo: {v3['prev_view']} -> {v3['view']}"))
            if v3['why']: pdf.set_x(8); pdf.multi_cell(0, 4.8, clean(f"Por que: {v3['why']}"))
        else:
            pdf.set_x(8); pdf.multi_cell(0, 4.8, clean("Cambio vs previo: NO"))
        pdf.set_text_color(0,0,0)

    # [W] What would change my mind (al final, después del 3M)
    _wwcm = wwcm_items if wwcm_items else build_wwcm(closes, fred)
    pdf.sec("[W]", "WHAT WOULD CHANGE MY MIND")
    for it in _wwcm:
        pdf.set_font('Helvetica','', 8); pdf.set_x(8)
        pdf.multi_cell(0, 5.2, clean(it))

    return pdf


# ── MARKDOWN BUILDER ──────────────────────────────────────────────
def build_md(closes, cnn, btc, fred, news, regime, drivers, chains,
             sent_lines, tensions, checklist, pos, tldr, pdata, total_val, v3=None, portfolio_comment=None, wwcm_items=None,
             usdclp_comment=None):
    L = []; w = L.append
    total_inv = sum(PORTFOLIO[t]['avg']*PORTFOLIO[t]['shares'] for t in PORTFOLIO)
    total_pnl = total_val - total_inv

    w(f"# Daily Briefing — {TODAY}\n")
    w("_The Global Compounder_\n")

    w("## [T] TL;DR DEL DIA")
    for line in tldr: w(f"- {line}")
    if not tensions:
        w("- Contradiccion: no dominante")
    w("")

    w("## [R] REGIMEN MACRO ACTUAL")
    w(f"**{regime['label']}**")
    w(f"{regime['desc']}\n")

    w("## [G] CRECIMIENTO REAL")
    for line in build_growth_real(fred):
        w(f"- {line}")
    w("")

    w("## [L] LIQUIDEZ GLOBAL")
    for line in build_liquidity(fred):
        w(f"- {line}")
    w("")

    w("## [CR] SISTEMA CREDITICIO")
    for line in build_credit_system(fred):
        w(f"- {line}")
    w("")

    w("## [D] DRIVERS ACTIVOS (ranking)")
    if drivers:
        for d in drivers[:5]:
            w(f"- {d['icon']} {d['label']} - {d['detail']}")
    else:
        w("- Sin drivers dominantes detectados.")
    w("")

    w("## [SCE] ESCENARIOS (probabilidad subjetiva)")
    for p, s in build_scenarios(closes, fred, cnn):
        w(f"- {p} -> {s}")
    w(f"- Conviccion: {build_conviction(closes, fred, cnn)}")
    w("")

    w("## [->] MAPA DE RELACIONES (causalidad + loop)")
    if chains:
        for ch in chains: w(f"- {ch}")
    else:
        w("- Correlaciones mixtas, sin cadena causal dominante.")
    w(f"- Loop: {build_feedback_loop(closes, fred)}")
    w("")

    w("## [!] TENSIONES DEL MERCADO")
    if tensions:
        for t in tensions: w(f"- {t}")
    else:
        w("- Sin tensiones relevantes detectadas.")
    w("")

    w("## [E] EXPECTATIVAS DEL MERCADO (pricing)")
    for it in build_expectations(closes, fred):
        w(f"- {it}")
    w("")

    w("## [BIAS] BIAS TACTICO (1-4 semanas)")
    for it in build_bias_tactico(closes, fred):
        w(f"- {it}")
    w("")

    w("## [SIG] SENAL MAS IMPORTANTE DEL DIA")
    w(f"- {build_key_signal(closes)}")
    w("")

    w("## [S] SENTIMIENTO - INTERPRETACION")
    w(f"- CNN F&G {cnn.get('score','N/A')} -> {cnn.get('rating','N/A')} | 1W {cnn.get('prev_1w','?')} | 1M {cnn.get('prev_1m','?')}")
    w(f"- BTC F&G {btc.get('score','N/A')} -> {btc.get('rating','N/A')} | 1D {btc.get('change','?')}")
    if sent_lines:
        for line in sent_lines: w(f"- {line}")
    else:
        w("- Sin lectura fuerte de sentimiento hoy.")
    w("")

    w("## [P] POSICIONAMIENTO DEL MERCADO")
    pct_b = pos.get('pct_below_200', 0)
    pct_a = 100 - pct_b
    w(f"- Breadth: {pct_b:.0f}% bajo EMA200 / {pct_a:.0f}% sobre EMA200")
    w(f"- RSI promedio: {pos.get('avg_rsi',50):.1f} -> {pos.get('label','Neutral')}")
    w("")

    w("## [C] CHECKLIST DEL SISTEMA")
    for label, val, green in checklist:
        w(f"- {label}: {val}")
    w("")

    w("## [O] PORTFOLIO (resumen breve)")
    w(f"- Core: ${total_val:,.2f} | PnL: ${total_pnl:+,.2f} ({total_pnl/total_inv*100:+.1f}%)")
    for it in build_portfolio_insights(pdata, total_val, closes):
        w(f"- {it}")
    w("")
    w("## [OP] COMENTARIO DE PORTFOLIO (2da pasada)")
    _pc = portfolio_comment if portfolio_comment else build_portfolio_comment(pdata, total_val)
    w(f"- {_pc}")
    w("")

    w("## [M] MERCADO (tabla resumida)\n")
    w("| Indicador | Valor | 1D% | YTD% |")
    w("|-----------|-------|-----|------|")
    for t, label in MACRO_TICKERS.items():
        if t not in closes.columns: continue
        s = closes[t].dropna(); p = float(s.iloc[-1])
        w(f"| {label} | {p:.2f} | {d1_ret(s):+.1f}% | {ytd_ret(s):+.1f}% |")
    w("")
    if 'USDCLP=X' in closes.columns and not closes['USDCLP=X'].dropna().empty:
        s_clp   = closes['USDCLP=X'].dropna()
        clp_val = float(s_clp.iloc[-1])
        clp_d1  = d1_ret(s_clp)
        clp_ytd = ytd_ret(s_clp)
        w("## [CLP] USDCLP / PESO CHILENO\n")
        w(f"**USDCLP: {clp_val:.0f}** | 1D: {clp_d1:+.1f}% | YTD: {clp_ytd:+.1f}%\n")
        w("| Factor | Valor | YTD% | Efecto en CLP |")
        w("|--------|-------|------|---------------|")
        hg_val  = get_val(closes, 'HG=F')
        hg_ytd  = ytd_ret(closes['HG=F'].dropna()) if 'HG=F' in closes.columns else None
        dxy_val = get_val(closes, 'DX-Y.NYB')
        dxy_ytd = ytd_ret(closes['DX-Y.NYB'].dropna()) if 'DX-Y.NYB' in closes.columns else None
        oil_val = get_val(closes, 'CL=F')
        oil_ytd = ytd_ret(closes['CL=F'].dropna()) if 'CL=F' in closes.columns else None
        vix_val = get_val(closes, '^VIX') or 20
        vix_ytd = ytd_ret(closes['^VIX'].dropna()) if '^VIX' in closes.columns else None
        sp_ytd  = ytd_ret(closes['^GSPC'].dropna()) if '^GSPC' in closes.columns else None
        ff_val  = fred.get('FEDFUNDS', {}).get('value', None)
        if hg_val:  w(f"| Cobre HG=F | ${hg_val:.3f}/lb | {hg_ytd:+.1f}% | Cobre alto = CLP fuerte (USDCLP baja) |")
        if dxy_val: w(f"| DXY | {dxy_val:.1f} | {dxy_ytd:+.1f}% | DXY alto = CLP debil |")
        if oil_val: w(f"| Oil WTI | ${oil_val:.1f} | {oil_ytd:+.1f}% | Oil alto = CLP debil (importador) |")
        w(f"| VIX | {vix_val:.1f} | {vix_ytd:+.1f}% | VIX alto = risk-off = CLP debil |" if vix_ytd is not None else f"| VIX | {vix_val:.1f} | N/A | VIX alto = risk-off = CLP debil |")
        if sp_ytd is not None: w(f"| S&P 500 | YTD | {sp_ytd:+.1f}% | Risk-on = CLP fuerte |")
        if ff_val:  w(f"| Fed Funds | {ff_val:.2f}% | --- | Tasa alta sostiene USD = CLP debil |")
        w("")
        if usdclp_comment:
            w(f"_{usdclp_comment}_\n")

    w("## [F] MACRO (FRED)")
    fred_keys = [('FEDFUNDS','Fed Funds','%'),('T10Y2Y','Spread 10Y-2Y','%'),
                 ('DGS10','10Y Yield','%'),('UNRATE','Desempleo','%'),
                 ('T5YIE','Inflacion implicita 5Y','%'),('BAMLH0A0HYM2','HY Spread','bps'),
                 ('UMCSENT','Conf. consumidor',''),('WALCL','Balance Fed','T')]
    for k, label, unit in fred_keys:
        if k not in fred: continue
        val = fred[k]['value']; date = fred[k].get('date','')
        if unit == 'bps': vs = f"{val*100:.0f}bps"
        elif unit == 'T': vs = f"${val/1e6:.2f}T"
        elif unit == '%': vs = f"{val:.2f}%"
        else: vs = f"{val:,.0f}"
    w(f"- {label}: {vs} _{date}_")
    w("")

    w("## [N] NOTICIAS INTERPRETADAS")
    if news:
        for n in news[:5]:
            title_t = n['title'][:120] + ('...' if len(n['title']) > 120 else '')
            w(f"- {title_t} — {interpret_news(n['title'], n['summary'])} ({n['source']})")
    else:
        w("- Sin noticias relevantes filtradas.")
    w("")

    w("## [A] ALERTAS")
    alerts_meaning = build_alerts_meaning(closes, cnn, fred, pdata)
    if alerts_meaning:
        for title_a, meaning in alerts_meaning:
            w(f"- {title_a}: {meaning}")
    else:
        w("- Sin alertas criticas.")
    w("")

    w("## [EV] EVENTOS")
    for date_s, name, impact, note in UPCOMING_EVENTS:
        days = (datetime.strptime(date_s,'%Y-%m-%d') - datetime.today()).days
        if not 0 <= days <= 90: continue
        w(f"- {date_s} ({days}d) [{impact}] {name} — {note}")

    w("")
    w("## [3M] 3M VIEW BASED ON CURRENT BRIEF")
    v3 = v3 or build_three_month_view(closes, fred, drivers)
    for line in v3['lines']:
        w(f"- {line}")
    w(f"- Razon: {v3['reason']}")
    if v3['changed']:
        w(f"- Cambio vs previo: {v3['prev_view']} -> {v3['view']}")
        if v3['why']: w(f"- Por que: {v3['why']}")
    else:
        w("- Cambio vs previo: NO")
    w("")

    w("## [W] WHAT WOULD CHANGE MY MIND")
    _wwcm = wwcm_items if wwcm_items else build_wwcm(closes, fred)
    for it in _wwcm:
        w(f"- {it}")

    w(f"\n---\n_Pipeline: {datetime.now().strftime('%Y-%m-%d %H:%M')}_")
    return '\n'.join(L)


# ── MAIN ──────────────────────────────────────────────────────────
def calc_portfolio_data(closes):
    pdata, total_val = {}, 0
    for t, info in PORTFOLIO.items():
        if t not in closes.columns: continue
        s = closes[t].dropna(); price = float(s.iloc[-1]); val = price * info['shares']
        total_val += val
        pdata[t] = {'price': price, 'val': val,
                    'pnl_d': val - info['avg'] * info['shares'],
                    'pnl_p': (price - info['avg']) / info['avg'] * 100,
                    'ytd': ytd_ret(s), 'd1': d1_ret(s),
                    'ema200': float(ema(s,200).iloc[-1]),
                    'ath_p': pct_ath(s)}
    return pdata, total_val

def run():
    print(f"\n{'='*55}")
    print(f"  Daily Pipeline — {TODAY}")
    print(f"{'='*55}\n")

    print("1/6  Precios de mercado...")
    closes = get_prices()

    print("2/6  CNN Fear & Greed...")
    cnn = get_cnn_fg()
    print(f"     {cnn.get('score','ERR')} ({cnn.get('rating','N/A')})")

    print("3/6  BTC Fear & Greed...")
    btc = get_btc_fg()
    print(f"     {btc.get('score','ERR')} ({btc.get('rating','N/A')})")

    print("4/6  Noticias RSS...")
    news = get_news()
    print(f"     {len(news)} articulos")
    for n in news[:3]: print(f"     -> {n['title'][:65]}...")

    print("5/6  FRED + inteligencia...")
    fred      = get_fred()
    pdata, total_val = calc_portfolio_data(closes)
    regime    = detect_regime(closes, fred)
    drivers   = rank_drivers(closes, fred, cnn, btc)
    chains    = build_causal_chains(closes, fred)
    sent      = interpret_sentiment(cnn, btc, closes)
    tensions  = detect_tensions(closes, fred, cnn)
    checklist = daily_checklist(closes, fred, cnn, btc)
    pos       = calc_positioning(closes)
    tldr      = build_tldr_grok(regime, drivers, tensions, closes, cnn, btc, pdata, total_val, fred)
    print(f"     Regimen: {regime['label']}")

    print("6/6  Generando MD + PDF...")
    v3           = build_three_month_view_groq(closes, fred, drivers, regime=regime, tensions=tensions,
                                               cnn=cnn, btc=btc, pdata=pdata, total_val=total_val)
    port_comment = build_portfolio_comment_groq(pdata, total_val, regime=regime,
                                                closes=closes, fred=fred, tensions=tensions)
    wwcm         = build_wwcm_groq(closes, fred, regime=regime, drivers=drivers, tensions=tensions)
    usdclp_comm  = build_usdclp_comment_groq(closes, fred, regime=regime, tensions=tensions)

    md_text = build_md(closes, cnn, btc, fred, news, regime, drivers, chains,
                       sent, tensions, checklist, pos, tldr, pdata, total_val,
                       v3=v3, portfolio_comment=port_comment, wwcm_items=wwcm,
                       usdclp_comment=usdclp_comm)
    pdf_obj = build_pdf(closes, cnn, btc, fred, news, regime, drivers, chains,
                        sent, tensions, checklist, pos, tldr, pdata, total_val,
                        v3=v3, include_3m=True, portfolio_comment=port_comment, wwcm_items=wwcm,
                        usdclp_comment=usdclp_comm)

    md_path  = os.path.join(SUMM_DIR, f"{TODAY}.md")
    pdf_path = os.path.join(SUMM_DIR, f"{TODAY}.pdf")
    with open(md_path, 'w', encoding='utf-8') as f: f.write(md_text)
    pdf_obj.output(pdf_path)
    save_narrative_log(regime, drivers, closes, cnn, btc)

    print(f"\n  MD  -> {md_path}")
    print(f"  PDF -> {pdf_path}")
    print(f"\n{'─'*55}")
    print(f"  Regimen: {regime['label']}")
    print(f"  Portfolio: ${total_val:,.2f}")
    if drivers: print(f"  Driver #1: {drivers[0]['label']}")
    print(f"  CNN F&G: {cnn.get('score','N/A')} | BTC F&G: {btc.get('score','N/A')}")
    if tensions:
        print(f"\n  Tensiones detectadas:")
        for t in tensions: print(f"    ! {t}")
    print(f"{'─'*55}\n")

if __name__ == "__main__":
    run()
