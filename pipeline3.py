#!/usr/bin/env python3
"""
Pipeline 3 — The Global Compounder (distribución pública)
Arquitectura 2 etapas:
  Etapa 1: Interpretacion base (1 llamada Groq) — regimen + causa + senales + tensiones
  Etapa 2: Secciones del reporte (4 llamadas Groq usando la interpretacion base)

Datos:
  - yfinance: 12 macro tickers × 63 sesiones (W=5, M=21, Q=63)
  - FRED: tasas, spreads, empleo, crecimiento, liquidez
  - CNN F&G + BTC F&G
  - RSS news (10 articulos filtrados)
  - Calendario macro (UPCOMING_EVENTS)

Output: data/daily_summaries/YYYY-MM-DD_p3.pdf + .md
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import os, json, warnings, requests
from datetime import datetime
import pandas as pd
import yfinance as yf
import feedparser
from fpdf import FPDF

warnings.filterwarnings('ignore')


# ── env ───────────────────────────────────────────────────────────────────────
def _load_env():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if not os.path.exists(path):
        return
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip())

_load_env()


# ── config ────────────────────────────────────────────────────────────────────
BASE     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE, 'data')
SUMM_DIR = os.path.join(DATA_DIR, 'daily_summaries')
os.makedirs(SUMM_DIR, exist_ok=True)
TODAY = datetime.today().strftime('%Y-%m-%d')

try:
    yf.set_tz_cache_location(os.path.join(DATA_DIR, 'yf_cache'))
except Exception:
    pass

MACRO_TICKERS = {
    '^GSPC':     'S&P 500',
    'EEM':       'EM Equities',
    '^STOXX50E': 'Europa',
    'TLT':       'Bonos 20yr',
    'HYG':       'High Yield',
    '^VIX':      'VIX',
    'GC=F':      'Oro',
    'DX-Y.NYB':  'DXY',
    'CL=F':      'Oil WTI',
    'HG=F':      'Cobre',
    'USDJPY=X':  'USD/JPY',
    'USDCLP=X':  'USD/CLP',
}

NEWS_FEEDS = [
    # Mercados
    ('CNBC',         'https://www.cnbc.com/id/100003114/device/rss/rss.html'),
    ('MarketWatch',  'https://feeds.content.dowjones.io/public/rss/mw_marketpulse'),
    ('Reuters Biz',  'https://feeds.reuters.com/reuters/businessNews'),
    # Geopolítica
    ('Reuters World','https://feeds.reuters.com/Reuters/worldNews'),
    ('BBC World',    'https://feeds.bbci.co.uk/news/world/rss.xml'),
    ('Al Jazeera',   'https://www.aljazeera.com/xml/rss/all.xml'),
]
NEWS_KEYWORDS = [
    # Macro / Fed
    'fed', 'federal reserve', 'fomc', 'powell', 'rate', 'inflation', 'cpi', 'pce',
    'recession', 'treasury', 'yield', 'dollar', 'dxy',
    # Mercados
    'oil', 'crude', 'opec', 'gold', 'vix', "s&p", 'nasdaq', 'market', 'stocks',
    'bitcoin', 'btc', 'equity', 'metals', 'tariff', 'trade',
    # Geopolítica
    'iran', 'hormuz', 'strait', 'middle east', 'israel', 'gaza', 'hezbollah',
    'russia', 'ukraine', 'nato', 'taiwan', 'china sea', 'sanctions', 'embargo',
    'military', 'attack', 'strike', 'war', 'conflict', 'escalat', 'ceasefire',
    'opec', 'saudi', 'gulf', 'pipeline', 'energy crisis',
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


# ── helpers ───────────────────────────────────────────────────────────────────
def clean(text):
    return (str(text)
            .replace('\u2014', '-').replace('\u2013', '-').replace('\u2019', "'")
            .replace('\u201c', '"').replace('\u201d', '"').replace('\u2018', "'")
            .replace('\u2022', '*').replace('\u00b0', '')
            .encode('latin-1', errors='replace').decode('latin-1'))

def _ret(a, b):
    try:
        return f'{(float(b) / float(a) - 1) * 100:+.1f}%'
    except Exception:
        return 'N/D'

def _scalar(series, n=-1):
    try:
        return round(float(series.dropna().iloc[n]), 2)
    except Exception:
        return None

def d1_ret(s):
    return (s.iloc[-1] - s.iloc[-2]) / s.iloc[-2] * 100 if len(s) > 1 else float('nan')

def ytd_ret(s):
    try:
        p0 = s.loc[s.index >= f'{datetime.today().year}-01-01'].iloc[0]
        return (s.iloc[-1] - p0) / p0 * 100
    except Exception:
        return float('nan')


# ── data layer ────────────────────────────────────────────────────────────────
def get_series():
    all_t = list(MACRO_TICKERS.keys())
    print(f'  Descargando {len(all_t)} tickers (6 meses)...')
    try:
        raw = yf.download(all_t, period='6mo', auto_adjust=True, progress=False, threads=True)
        closes = (raw['Close'] if 'Close' in raw.columns.get_level_values(0)
                  else raw.xs('Close', axis=1, level=0))
        closes.index = pd.to_datetime(closes.index)
    except Exception as e:
        print(f'  WARNING yfinance: {e}')
        return pd.DataFrame()
    return closes.tail(63)

def get_fred():
    p = os.path.join(DATA_DIR, 'macro', 'macro_snapshot.json')
    if not os.path.exists(p):
        return {}
    with open(p, encoding='utf-8') as f:
        return json.load(f)

def get_cnn_fg():
    try:
        r = requests.get(
            'https://production.dataviz.cnn.io/index/fearandgreed/graphdata',
            headers={'User-Agent': 'Mozilla/5.0',
                     'Referer': 'https://edition.cnn.com/markets/fear-and-greed'},
            timeout=10)
        fg = r.json()['fear_and_greed']
        s = fg['score']
        return {'score': round(s, 1), 'rating': fg['rating'],
                'change': round(s - fg['previous_close'], 1)}
    except Exception:
        cache = os.path.join(DATA_DIR, 'cnn_fg_cache.json')
        if os.path.exists(cache):
            try:
                with open(cache, encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return {'score': None, 'rating': 'N/A', 'change': None}

def get_btc_fg():
    try:
        r = requests.get('https://api.alternative.me/fng/?limit=2', timeout=8)
        d = r.json()['data']
        return {'score': int(d[0]['value']), 'rating': d[0]['value_classification']}
    except Exception:
        return {'score': None, 'rating': 'N/A'}

def get_news(max_items=10):
    seen, articles = set(), []
    for source, url in NEWS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:20]:
                title = e.get('title', '').strip()
                key   = title.lower()[:60]
                if key in seen:
                    continue
                seen.add(key)
                combined = (title + ' ' + e.get('summary', '')).lower()
                if not any(kw in combined for kw in NEWS_KEYWORDS):
                    continue
                score = sum(1 for kw in NEWS_KEYWORDS if kw in combined)
                articles.append({'title': title, 'source': source,
                                  'summary': e.get('summary', '')[:200], 'score': score})
        except Exception:
            continue
    articles.sort(key=lambda x: x['score'], reverse=True)
    return articles[:max_items]


# ── macro helpers ─────────────────────────────────────────────────────────────
def summarize_growth(fred):
    ism_m = fred.get('NAPM',   {}).get('value')
    ism_s = fred.get('NMFNMI', {}).get('value')
    vals  = [v for v in (ism_m, ism_s) if v is not None]
    if not vals: return 'indeterminado'
    avg = sum(vals) / len(vals)
    if avg < 50: return 'contraccion'
    if avg < 52: return 'desaceleracion'
    return 'expansion'

def summarize_liquidity(fred):
    ch = fred.get('WALCL', {}).get('change')
    if ch is None: return 'indeterminado'
    return 'expandiendo' if ch > 0 else 'contrayendo' if ch < 0 else 'neutra'

def summarize_credit(fred):
    hy = fred.get('BAMLH0A0HYM2', {}).get('value')
    if hy is None: return 'indeterminado'
    return 'estable' if hy < 4.0 else 'tensionado'

def detect_tensions(closes, fred, cnn):
    """Detecta contradicciones y anomalias en los datos de mercado."""
    gold_d1  = d1_ret(closes['GC=F'].dropna())  if 'GC=F'  in closes.columns else 0
    sp_d1    = d1_ret(closes['^GSPC'].dropna()) if '^GSPC' in closes.columns else 0
    gold_ytd = ytd_ret(closes['GC=F'].dropna()) if 'GC=F'  in closes.columns else 0
    sp_ytd   = ytd_ret(closes['^GSPC'].dropna())if '^GSPC' in closes.columns else 0
    vix      = _scalar(closes['^VIX'].dropna()) if '^VIX'  in closes.columns else 20
    hy_s     = fred.get('BAMLH0A0HYM2', {}).get('value', 3.0)
    cnn_s    = cnn.get('score', 50)

    tensions = []
    if cnn_s and cnn_s < 25 and hy_s < 4.0:
        tensions.append(
            f'Fear extremo (CNN {cnn_s}) pero HY spread {hy_s*100:.0f}bps OK '
            f'-> no hay crisis crediticia real')
    if gold_d1 is not None and sp_d1 is not None and gold_d1 < -2 and sp_d1 < -1:
        tensions.append(
            f'Oro {gold_d1:+.1f}% Y equities {sp_d1:+.1f}% caen juntos '
            f'-> real yields forzando liquidacion de todo')
    if vix and vix > 25 and sp_ytd and sp_ytd > -10:
        tensions.append(
            f'VIX {vix:.0f} con S&P solo {sp_ytd:.1f}% YTD '
            f'-> mercado teme pero no capitula. Falta limpieza.')
    if gold_ytd and gold_ytd > 10 and gold_d1 is not None and gold_d1 < -3:
        tensions.append(
            f'Gold +{gold_ytd:.0f}% YTD pero {gold_d1:+.1f}% hoy '
            f'-> probably real yields, NO cambio de tesis')
    growth = summarize_growth(fred)
    credit = summarize_credit(fred)
    liq    = summarize_liquidity(fred)
    if growth == 'contraccion' and credit == 'estable':
        tensions.append('Crecimiento en contraccion pero credito estable -> desaceleracion, no crisis (aun)')
    if liq == 'contrayendo' and vix and vix < 25:
        tensions.append('Liquidez en contraccion pero volatilidad no extrema -> complacencia potencial')
    return tensions


# ── format helpers ────────────────────────────────────────────────────────────
def format_macro_summary(closes):
    lines = []
    for t, label in MACRO_TICKERS.items():
        if t not in closes.columns:
            continue
        s = closes[t].dropna()
        if len(s) < 2:
            continue
        price = _scalar(s)
        r_w = _ret(_scalar(s, -5),  price) if len(s) >= 5  else 'N/D'
        r_m = _ret(_scalar(s, -21), price) if len(s) >= 21 else 'N/D'
        r_q = _ret(_scalar(s, 0),   price)
        lines.append(f'{label:15s} {price:>9.2f}  W {r_w:>7}  M {r_m:>7}  Q {r_q:>7}')
    return '\n'.join(lines)

def format_fred_summary(fred):
    def v(key): return fred.get(key, {}).get('value', 'N/D')
    return (
        f"Fed Funds {v('FEDFUNDS')}%  |  10Y {v('DGS10')}%  |  2Y {v('DGS2')}%  |  "
        f"Spread 10Y-2Y {v('T10Y2Y')}%  |  Inflacion impl 5Y {v('T5YIE')}%\n"
        f"HY spread {v('BAMLH0A0HYM2')}  |  IG spread {v('BAMLC0A0CM')}  |  "
        f"Desempleo {v('UNRATE')}%  |  Confianza consumidor {v('UMCSENT')}"
    )

def upcoming_next(n=4):
    """Retorna los proximos N eventos futuros desde hoy."""
    return [(d, name, pri, desc)
            for d, name, pri, desc in UPCOMING_EVENTS if d >= TODAY][:n]


# ── groq ─────────────────────────────────────────────────────────────────────
def _groq_call(prompt, max_tokens=800):
    api_key = os.environ.get('GROQ_API_KEY', '')
    model   = os.environ.get('GROQ_MODEL', 'llama-3.3-70b-versatile')
    if not api_key:
        return None
    try:
        r = requests.post(
            'https://api.groq.com/openai/v1/chat/completions',
            headers={'Authorization': f'Bearer {api_key}',
                     'Content-Type': 'application/json'},
            json={'model': model,
                  'messages': [{'role': 'user', 'content': prompt}],
                  'max_tokens': max_tokens, 'temperature': 0.3},
            timeout=30,
        )
        if not r.ok:
            print(f'  WARNING Groq {r.status_code}: {r.text[:120]}')
            return None
        return r.json()['choices'][0]['message']['content'].strip()
    except Exception as e:
        print(f'  WARNING Groq: {e}')
        return None


# ── ETAPA 1: Interpretacion base ──────────────────────────────────────────────
def build_interpretation(closes, fred, cnn, btc, news, tensions):
    macro_txt = format_macro_summary(closes)
    fred_txt  = format_fred_summary(fred)
    news_txt  = '\n'.join(f'- [{a["source"]}] {a["title"]}' for a in news[:8])
    tens_txt  = '\n'.join(f'! {t}' for t in tensions) if tensions else 'Sin tensiones detectadas.'
    cnn_s  = cnn.get('score', 'N/D')
    cnn_r  = cnn.get('rating', '')
    cnn_ch = cnn.get('change', None)
    btc_s  = btc.get('score', 'N/D')
    btc_r  = btc.get('rating', '')

    prompt = f"""Eres un analista macro senior. Tu tarea es INTERPRETAR los datos del mercado de hoy \
y producir una lectura base que sera usada para generar todas las secciones de un reporte financiero \
de distribucion publica. Es critico que la causa raiz este anclada en datos y noticias concretas.

DATOS DE MERCADO — {TODAY}

[ACTIVOS] Precio | W (5d) | M (21d) | Q (63d):
{macro_txt}

[FRED MACRO]:
{fred_txt}
Crecimiento: {summarize_growth(fred)} | Liquidez: {summarize_liquidity(fred)} | Credito: {summarize_credit(fred)}

[SENTIMIENTO]:
CNN Fear & Greed: {cnn_s} ({cnn_r}){f', cambio vs ayer: {cnn_ch:+.1f}' if cnn_ch else ''}
BTC Fear & Greed: {btc_s} ({btc_r})

[TENSIONES DETECTADAS]:
{tens_txt}

[NOTICIAS DEL DIA]:
{news_txt if news_txt else 'Sin noticias disponibles.'}

Produce la interpretacion con EXACTAMENTE este formato (sin texto antes ni despues):

REGIMEN: [etiqueta corta, ej: "Risk-Off / Inflation Shock"]

CAUSA_RAIZ: [1-2 oraciones: QUE esta pasando Y POR QUE, citando la noticia o dato concreto]

SENALES:
- [activo] [W/M/Q]: [observacion con numero concreto]
- [activo] [W/M/Q]: [observacion con numero concreto]
- [activo] [W/M/Q]: [observacion con numero concreto]

DIVERGENCIAS: [max 2 divergencias notables con numeros, o "Sin divergencias relevantes"]"""

    return _groq_call(prompt, max_tokens=600)


# ── ETAPA 2: Secciones ────────────────────────────────────────────────────────
def build_tldr(interp, cnn, btc, closes, fred):
    vix   = _scalar(closes['^VIX'].dropna())   if '^VIX'  in closes.columns else 'N/D'
    dgs10 = fred.get('DGS10', {}).get('value', 'N/D')
    sp_q  = _ret(_scalar(closes['^GSPC'].dropna(), 0), _scalar(closes['^GSPC'].dropna())) \
            if '^GSPC' in closes.columns else 'N/D'

    prompt = f"""Usando esta interpretacion del mercado como base:

{interp}

Genera un TL;DR de EXACTAMENTE 4 bullets en espanol, comenzando cada uno con "- ".
Deben cubrir: (1) regimen actual con causa, (2) movimiento mas relevante del dia con numero, \
(3) tension o divergencia mas importante, (4) que vigilar esta semana.
Datos adicionales: VIX {vix} | 10Y {dgs10}% | S&P Q {sp_q} | CNN F&G {cnn.get('score','N/D')} | BTC F&G {btc.get('score','N/D')}
Sin titulos, sin introduccion, solo los 4 bullets."""

    return _groq_call(prompt, max_tokens=400)


def build_3m_view(interp, closes, fred):
    oil_m  = _ret(_scalar(closes['CL=F'].dropna(),   -21), _scalar(closes['CL=F'].dropna()))  \
             if 'CL=F'  in closes.columns else 'N/D'
    sp_q   = _ret(_scalar(closes['^GSPC'].dropna(),  0),   _scalar(closes['^GSPC'].dropna())) \
             if '^GSPC' in closes.columns else 'N/D'
    hy     = fred.get('BAMLH0A0HYM2', {}).get('value', 'N/D')
    dgs10  = fred.get('DGS10',        {}).get('value', 'N/D')
    t5yie  = fred.get('T5YIE',        {}).get('value', 'N/D')

    prompt = f"""Usando esta interpretacion del mercado como base:

{interp}

Genera un 3M VIEW (perspectiva proximos 3 meses) con EXACTAMENTE 5 bullets en espanol, \
comenzando cada uno con "- ".
Datos adicionales: Oil M {oil_m} | S&P Q {sp_q} | HY spread {hy} | 10Y {dgs10}% | Inflacion impl 5Y {t5yie}%

1. Base case (~60%): escenario mas probable con cifras concretas (niveles, porcentajes)
2. Bear case (~20%): que podria salir peor, con causa especifica y cifra de referencia
3. Bull case (~15%): sorpresa positiva y condicion necesaria concreta
4. Claves a monitorear: 2-3 indicadores con umbrales numericos exactos
5. Implicancias para activos: equities / bonos / oro / crypto / commodities

Sin titulos, sin introduccion, solo los 5 bullets."""

    return _groq_call(prompt, max_tokens=700)


def build_wwcm(interp, tensions):
    tens_txt = '\n'.join(f'! {t}' for t in tensions) if tensions else 'Sin tensiones detectadas.'

    prompt = f"""Usando esta interpretacion del mercado como base:

{interp}

Tensiones detectadas automaticamente:
{tens_txt}

Genera "WHAT WOULD CHANGE MY MIND" con 4-5 condiciones concretas que cambiarian el regimen actual.
Formato: bullets en espanol comenzando con "- ".
Cada bullet: condicion especifica + umbral numerico + que implicaria para los mercados.
Sin titulos, sin introduccion, solo los bullets."""

    return _groq_call(prompt, max_tokens=500)


def build_usdclp_comment(interp, closes):
    clp = closes['USDCLP=X'].dropna() if 'USDCLP=X' in closes.columns else None
    cu  = closes['HG=F'].dropna()     if 'HG=F'     in closes.columns else None
    dxy = closes['DX-Y.NYB'].dropna() if 'DX-Y.NYB' in closes.columns else None

    clp_now = _scalar(clp)                                                        if clp is not None else 'N/D'
    clp_w   = _ret(_scalar(clp, -5),  _scalar(clp)) if clp is not None and len(clp) >= 5  else 'N/D'
    clp_m   = _ret(_scalar(clp, -21), _scalar(clp)) if clp is not None and len(clp) >= 21 else 'N/D'
    clp_q   = _ret(_scalar(clp, 0),   _scalar(clp)) if clp is not None                    else 'N/D'
    cu_q    = _ret(_scalar(cu,  0),   _scalar(cu))  if cu  is not None                    else 'N/D'
    dxy_q   = _ret(_scalar(dxy, 0),   _scalar(dxy)) if dxy is not None                    else 'N/D'

    prompt = f"""Usando esta interpretacion del mercado como base:

{interp}

Genera un comentario sobre el USDCLP. 2-3 oraciones.
Datos: USDCLP {clp_now} | W {clp_w} | M {clp_m} | Q {clp_q} | Cobre Q {cu_q} | DXY Q {dxy_q}
Incluye: nivel actual en contexto, presiones dominantes (cobre/DXY/EM), direccion esperada proximas semanas.
En espanol, directo al punto, sin titulos."""

    return _groq_call(prompt, max_tokens=300)


# ── PDF ───────────────────────────────────────────────────────────────────────
class PDF(FPDF):
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=12)
        self.set_margins(8, 10, 8)

    def header(self):
        self.set_font('Helvetica', 'B', 7)
        self.set_text_color(120, 120, 120)
        self.cell(0, 5, clean(f'THE GLOBAL COMPOUNDER — MACRO BRIEF | {TODAY}'), align='R')
        self.ln(4)

    def footer(self):
        self.set_y(-10)
        self.set_font('Helvetica', '', 6)
        self.set_text_color(150, 150, 150)
        now = datetime.now().strftime('%Y-%m-%d %H:%M')
        self.cell(0, 4, clean(
            f'Generado: {now}  |  Groq llama-3.3-70b-versatile  |  '
            f'yfinance + FRED + RSS  |  Pipeline 3'), align='C')

    def section(self, title, color=(30, 60, 120)):
        self.set_fill_color(*color)
        self.set_text_color(255, 255, 255)
        self.set_font('Helvetica', 'B', 8)
        self.cell(0, 6, clean(f'  {title}'), fill=True, ln=True)
        self.set_text_color(0, 0, 0)
        self.ln(1)

    def body(self, text, size=8):
        self.set_font('Helvetica', '', size)
        self.set_x(10)
        self.multi_cell(0, 5, clean(text))

    def bullet(self, text, size=8):
        text = text.strip().lstrip('-* ')
        if not text:
            return
        self.set_font('Helvetica', '', size)
        self.set_x(8)
        self.cell(5, 5.2, clean('-'))
        self.multi_cell(0, 5.2, clean(text))


def build_pdf(closes, fred, cnn, btc, news, tensions,
              interp, tldr, v3, wwcm, usdclp_comment):
    pdf = PDF()
    pdf.add_page()

    # ── TL;DR ─────────────────────────────────────────────────────────────────
    pdf.section('[TL;DR] RESUMEN EJECUTIVO', color=(20, 20, 60))
    if tldr:
        for line in tldr.split('\n'):
            if line.strip():
                pdf.bullet(line)
    pdf.ln(3)

    # ── Sentimiento ───────────────────────────────────────────────────────────
    pdf.section('[S] SENTIMIENTO', color=(80, 40, 100))
    cnn_ch = cnn.get('change', None)
    ch_txt = f'  (cambio: {cnn_ch:+.1f})' if cnn_ch else ''
    pdf.body(
        f"CNN Fear & Greed: {cnn.get('score','N/D')} — {cnn.get('rating','')}{ch_txt}"
        f"   |   BTC Fear & Greed: {btc.get('score','N/D')} — {btc.get('rating','')}", size=8)
    pdf.ln(2)

    # ── Resumen de activos ────────────────────────────────────────────────────
    pdf.section('[M] RESUMEN DE ACTIVOS', color=(40, 80, 160))
    pdf.set_font('Helvetica', 'B', 7)
    pdf.set_fill_color(230, 235, 245)
    pdf.cell(38, 5.5, 'Activo',  fill=True)
    pdf.cell(22, 5.5, 'Precio',  fill=True, align='R')
    pdf.cell(18, 5.5, 'W (5d)',  fill=True, align='R')
    pdf.cell(18, 5.5, 'M (21d)', fill=True, align='R')
    pdf.cell(18, 5.5, 'Q (63d)', fill=True, align='R')
    pdf.ln()

    def ret_color(r):
        try:
            v = float(r.replace('%', '').replace('+', ''))
            return (180, 0, 0) if v < 0 else (0, 130, 0)
        except Exception:
            return (80, 80, 80)

    for t, label in MACRO_TICKERS.items():
        if t not in closes.columns:
            continue
        s = closes[t].dropna()
        if len(s) < 2:
            continue
        price = _scalar(s)
        r_w = _ret(_scalar(s, -5),  price) if len(s) >= 5  else 'N/D'
        r_m = _ret(_scalar(s, -21), price) if len(s) >= 21 else 'N/D'
        r_q = _ret(_scalar(s, 0),   price)
        pdf.set_font('Helvetica', '', 7)
        pdf.set_fill_color(248, 248, 252)
        pdf.cell(38, 5, clean(label), fill=True)
        pdf.cell(22, 5, f'{price:.2f}', fill=True, align='R')
        for r in [r_w, r_m, r_q]:
            pdf.set_text_color(*ret_color(r))
            pdf.cell(18, 5, r, fill=True, align='R')
            pdf.set_text_color(0, 0, 0)
        pdf.ln()
    pdf.ln(2)

    # ── Tensiones ─────────────────────────────────────────────────────────────
    if tensions:
        pdf.section('[!] TENSIONES DETECTADAS', color=(160, 50, 0))
        for t in tensions:
            pdf.bullet(t, size=7.5)
        pdf.ln(2)

    # ── Noticias ──────────────────────────────────────────────────────────────
    pdf.section('[N] NOTICIAS', color=(100, 60, 20))
    for a in news[:8]:
        pdf.set_font('Helvetica', 'B', 6.5)
        pdf.set_x(10)
        pdf.multi_cell(0, 4.5, clean(f'[{a["source"]}] {a["title"]}'))
        if a.get('summary'):
            pdf.set_font('Helvetica', '', 6)
            pdf.set_x(14)
            pdf.multi_cell(0, 4, clean(a['summary'][:160]))
        pdf.ln(1)
    pdf.ln(1)

    # ── Interpretacion base ───────────────────────────────────────────────────
    pdf.section('[I] INTERPRETACION BASE', color=(80, 40, 120))
    if interp:
        for line in interp.split('\n'):
            line = line.strip()
            if not line:
                pdf.ln(1)
            elif line.startswith('-'):
                pdf.bullet(line)
            elif ':' in line and line.split(':')[0].replace('_', ' ').isupper():
                parts = line.split(':', 1)
                pdf.set_font('Helvetica', 'B', 7.5)
                pdf.set_x(10)
                pdf.set_text_color(60, 60, 60)
                pdf.multi_cell(0, 5, clean(parts[0] + ':'))
                pdf.set_text_color(0, 0, 0)
                if len(parts) > 1 and parts[1].strip():
                    pdf.set_font('Helvetica', '', 7.5)
                    pdf.set_x(14)
                    pdf.multi_cell(0, 5, clean(parts[1].strip()))
            else:
                pdf.body(line, size=7.5)
    pdf.ln(2)

    # ── USDCLP ────────────────────────────────────────────────────────────────
    clp = closes['USDCLP=X'].dropna() if 'USDCLP=X' in closes.columns else None
    cu  = closes['HG=F'].dropna()     if 'HG=F'     in closes.columns else None
    pdf.section('[CLP] USD/CLP', color=(20, 100, 130))
    pdf.set_font('Helvetica', 'B', 7)
    pdf.set_fill_color(220, 240, 245)
    pdf.cell(38, 5.5, 'Activo',  fill=True)
    pdf.cell(22, 5.5, 'Precio',  fill=True, align='R')
    pdf.cell(18, 5.5, 'W (5d)',  fill=True, align='R')
    pdf.cell(18, 5.5, 'M (21d)', fill=True, align='R')
    pdf.cell(18, 5.5, 'Q (63d)', fill=True, align='R')
    pdf.ln()
    for label, s in [('USD/CLP', clp), ('Cobre (HG=F)', cu)]:
        if s is not None and len(s) >= 2:
            price = _scalar(s)
            r_w = _ret(_scalar(s, -5),  price) if len(s) >= 5  else 'N/D'
            r_m = _ret(_scalar(s, -21), price) if len(s) >= 21 else 'N/D'
            r_q = _ret(_scalar(s, 0),   price)
            pdf.set_font('Helvetica', '', 7)
            pdf.set_fill_color(245, 250, 252)
            pdf.cell(38, 5, clean(label), fill=True)
            pdf.cell(22, 5, f'{price:.2f}', fill=True, align='R')
            for r in [r_w, r_m, r_q]:
                pdf.set_text_color(*ret_color(r))
                pdf.cell(18, 5, r, fill=True, align='R')
                pdf.set_text_color(0, 0, 0)
            pdf.ln()
    pdf.ln(2)
    if usdclp_comment:
        pdf.body(usdclp_comment, size=7.5)
    pdf.ln(3)

    # ── Upcoming events ───────────────────────────────────────────────────────
    events = upcoming_next(5)
    if events:
        pdf.section('[CAL] CALENDARIO MACRO', color=(40, 100, 80))
        pdf.set_font('Helvetica', 'B', 7)
        pdf.set_fill_color(230, 245, 238)
        pdf.cell(25, 5.5, 'Fecha',       fill=True)
        pdf.cell(10, 5.5, 'Prior.',      fill=True, align='C')
        pdf.cell(75, 5.5, 'Evento',      fill=True)
        pdf.cell(0,  5.5, 'Relevancia',  fill=True)
        pdf.ln()
        PRIORITY_COLOR = {'HIGH': (200, 0, 0), 'MED': (180, 120, 0), 'LOW': (80, 80, 80)}
        for date, name, pri, desc in events:
            pc = PRIORITY_COLOR.get(pri, (80, 80, 80))
            pdf.set_font('Helvetica', 'B', 7)
            pdf.set_x(10)
            pdf.set_text_color(*pc)
            pdf.cell(14, 5, clean(date))
            pdf.cell(10, 5, f'[{pri}]')
            pdf.set_text_color(0, 0, 0)
            pdf.set_font('Helvetica', 'B', 7)
            pdf.cell(0, 5, clean(name), ln=True)
            pdf.set_font('Helvetica', '', 6.5)
            pdf.set_x(34)
            pdf.set_text_color(80, 80, 80)
            pdf.multi_cell(0, 4.5, clean(desc))
            pdf.set_text_color(0, 0, 0)
        pdf.ln(2)

    # ── WWCM ──────────────────────────────────────────────────────────────────
    pdf.section('[W] WHAT WOULD CHANGE MY MIND', color=(130, 70, 20))
    if wwcm:
        for line in wwcm.split('\n'):
            if line.strip():
                pdf.bullet(line)
    pdf.ln(3)

    # ── 3M View ───────────────────────────────────────────────────────────────
    pdf.section('[3M] 3M VIEW — BASED ON CURRENT BRIEF', color=(60, 30, 100))
    if v3:
        for line in v3.split('\n'):
            if line.strip():
                pdf.bullet(line)
    pdf.ln(3)

    return pdf


# ── Markdown ──────────────────────────────────────────────────────────────────
def build_md(closes, news, tensions, interp, tldr, v3, wwcm, usdclp_comment):
    now = datetime.now()
    L = []

    L += [f'# THE GLOBAL COMPOUNDER — MACRO BRIEF | {TODAY}',
          f'*Pipeline 3  |  {now.strftime("%Y-%m-%d %H:%M")}  |  llama-3.3-70b-versatile*',
          '']

    L += ['## [TL;DR]', '']
    if tldr:
        for l in tldr.split('\n'):
            if l.strip(): L.append(l.strip())
    L += ['', '---', '']

    L += ['## [M] RESUMEN DE ACTIVOS', '',
          '| Activo | Precio | W | M | Q |',
          '|---|---|---|---|---|']
    for t, label in MACRO_TICKERS.items():
        if t not in closes.columns: continue
        s = closes[t].dropna()
        if len(s) < 2: continue
        price = _scalar(s)
        r_w = _ret(_scalar(s, -5),  price) if len(s) >= 5  else 'N/D'
        r_m = _ret(_scalar(s, -21), price) if len(s) >= 21 else 'N/D'
        r_q = _ret(_scalar(s, 0),   price)
        L.append(f'| {label} | {price:.2f} | {r_w} | {r_m} | {r_q} |')
    L += ['', '---', '']

    if tensions:
        L += ['## [!] TENSIONES DETECTADAS', '']
        for t in tensions:
            L.append(f'! {t}')
        L += ['', '---', '']

    L += ['## [N] NOTICIAS', '']
    for a in news[:8]:
        L.append(f'**[{a["source"]}]** {a["title"]}')
        if a.get('summary'): L.append(f'> {a["summary"][:160]}')
        L.append('')
    L += ['---', '']

    L += ['## [I] INTERPRETACION BASE', '']
    if interp: L.append(interp)
    L += ['', '---', '']

    clp = closes['USDCLP=X'].dropna() if 'USDCLP=X' in closes.columns else None
    cu  = closes['HG=F'].dropna()     if 'HG=F'     in closes.columns else None
    L += ['## [CLP] USD/CLP', '']
    if clp is not None and len(clp) >= 2:
        p = _scalar(clp)
        L.append(f'USDCLP: {p:.2f} | W {_ret(_scalar(clp,-5),p) if len(clp)>=5 else "N/D"} '
                 f'| M {_ret(_scalar(clp,-21),p) if len(clp)>=21 else "N/D"} '
                 f'| Q {_ret(_scalar(clp,0),p)}')
    if cu is not None and len(cu) >= 2:
        p = _scalar(cu)
        L.append(f'Cobre:  {p:.2f} | Q {_ret(_scalar(cu,0),p)}')
    if usdclp_comment: L += ['', usdclp_comment]
    L += ['', '---', '']

    events = upcoming_next(5)
    if events:
        L += ['## [CAL] CALENDARIO MACRO', '',
              '| Fecha | Prioridad | Evento | Relevancia |',
              '|---|---|---|---|']
        for date, name, pri, desc in events:
            L.append(f'| {date} | {pri} | {name} | {desc} |')
        L += ['', '---', '']

    L += ['## [W] WHAT WOULD CHANGE MY MIND', '']
    if wwcm:
        for l in wwcm.split('\n'):
            if l.strip(): L.append(l.strip())
    L += ['', '---', '']

    L += ['## [3M] 3M VIEW', '']
    if v3:
        for l in v3.split('\n'):
            if l.strip(): L.append(l.strip())
    L += ['', '---', '']

    return '\n'.join(L)


# ── run ───────────────────────────────────────────────────────────────────────
def run():
    print('=' * 55)
    print(f'  PIPELINE 3 — Macro Brief | {TODAY}')
    print('=' * 55)

    # 1. Datos
    print('\n[1/3] Recopilando datos...')
    closes   = get_series()
    fred     = get_fred()
    cnn      = get_cnn_fg()
    btc      = get_btc_fg()
    news     = get_news(max_items=10)
    tensions = detect_tensions(closes, fred, cnn)

    ok_tickers = len([c for c in closes.columns if not closes[c].dropna().empty]) if not closes.empty else 0
    print(f'  Series:    {ok_tickers}/{len(MACRO_TICKERS)} tickers')
    print(f'  FRED:      {len(fred)} series')
    print(f'  CNN F&G:   {cnn.get("score","N/D")} ({cnn.get("rating","N/A")})')
    print(f'  BTC F&G:   {btc.get("score","N/D")} ({btc.get("rating","N/A")})')
    print(f'  Noticias:  {len(news)} articulos')
    if tensions:
        for t in tensions:
            print(f'  ! {t}')

    # 2. Etapa 1 — Interpretacion base
    print('\n[2/3] Groq — Etapa 1: Interpretacion base...')
    interp = build_interpretation(closes, fred, cnn, btc, news, tensions)
    if interp:
        print('  OK\n' + '-' * 55)
        print(interp)
        print('-' * 55)
        regime_line = next((l for l in interp.split('\n') if l.startswith('REGIMEN:')), '')
    else:
        print('  WARN: sin respuesta')
        interp = 'Interpretacion no disponible.'
        regime_line = ''

    # 3. Etapa 2 — Secciones
    print('\n[3/3] Groq — Etapa 2: Secciones...')
    print('  TL;DR...')
    tldr           = build_tldr(interp, cnn, btc, closes, fred)
    print('  3M View...')
    v3             = build_3m_view(interp, closes, fred)
    print('  WWCM...')
    wwcm           = build_wwcm(interp, tensions)
    print('  USDCLP...')
    usdclp_comment = build_usdclp_comment(interp, closes)
    print('  OK — 4 secciones generadas')

    # 4. Output
    stem     = f'{TODAY}_p3'
    md_path  = os.path.join(SUMM_DIR, f'{stem}.md')
    pdf_path = os.path.join(SUMM_DIR, f'{stem}.pdf')

    md = build_md(closes, news, tensions, interp, tldr, v3, wwcm, usdclp_comment)
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(md)

    try:
        pdf = build_pdf(closes, fred, cnn, btc, news, tensions,
                        interp, tldr, v3, wwcm, usdclp_comment)
        pdf.output(pdf_path)
        print(f'\n  PDF: data/daily_summaries/{stem}.pdf')
    except Exception as e:
        print(f'\n  ERROR PDF: {e}')
        import traceback; traceback.print_exc()

    print(f'  MD:  data/daily_summaries/{stem}.md')
    print(f'\n{"=" * 55}')
    print(f'  {regime_line}')
    print(f'  CNN F&G: {cnn.get("score","N/D")} | BTC F&G: {btc.get("score","N/D")}')
    if tensions:
        print(f'  Tensiones: {len(tensions)}')
    print(f'{"=" * 55}')


if __name__ == '__main__':
    run()
