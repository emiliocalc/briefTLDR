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

def _fg_rating(score):
    if score is None: return 'N/A'
    if score < 25:  return 'extreme fear'
    if score < 45:  return 'fear'
    if score < 55:  return 'neutral'
    if score < 75:  return 'greed'
    return 'extreme greed'

def get_cnn_fg():
    # Fuente principal: finhacker.cz (no bloqueada por GitHub Actions)
    try:
        r = requests.get(
            'https://www.finhacker.cz/wp-content/data/fng-live.json',
            timeout=10)
        d = r.json()
        s = round(float(d['score']), 1)
        result = {'score': s, 'rating': _fg_rating(s), 'change': None}
        # Guardar en cache
        cache = os.path.join(DATA_DIR, 'cnn_fg_cache.json')
        with open(cache, 'w', encoding='utf-8') as f:
            json.dump(result, f)
        return result
    except Exception:
        pass
    # Fallback: CNN directo
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
        pass
    # Fallback: cache local
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
                                  'summary': e.get('summary', '')[:200], 'score': score,
                                  'url': e.get('link', '')})
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
        r_1d = _ret(_scalar(s, -2),  price) if len(s) >= 2  else 'N/D'
        r_w  = _ret(_scalar(s, -5),  price) if len(s) >= 5  else 'N/D'
        r_m  = _ret(_scalar(s, -21), price) if len(s) >= 21 else 'N/D'
        r_q  = _ret(_scalar(s, 0),   price)
        lines.append(f'{label:15s} {price:>9.2f}  1D {r_1d:>7}  W {r_w:>7}  M {r_m:>7}  Q {r_q:>7}')
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


REGLAS_GLOBALES = """
REGLAS DE CONSISTENCIA (OBLIGATORIAS):

1. NIVELES Y PROYECCIONES:
   - Toda proyeccion de precio DEBE ser consistente con el nivel actual provisto en los datos.
   - Ejemplo: si S&P = 6500, una caida de -10% implica ~5850. Nunca uses niveles como 3800.
   - PROHIBIDO usar niveles historicos o default del modelo de entrenamiento.

2. DIRECCION LOGICA:
   - Si dices "sube X%", el nivel futuro DEBE ser mayor al actual.
   - Si dices "cae X%", el nivel futuro DEBE ser menor al actual.
   - Si hay contradiccion → NO escribir ese escenario.

3. COHERENCIA INTERNA:
   - No puedes contradecir datos actuales.
   - Ejemplo: si Oil = 98, no puedes decir "sube a 80" (80 < 98 = caida, no subida).
   - Si un umbral ya fue superado → NO usarlo como condicion futura.

4. ESCALA CORRECTA:
   - Usa SIEMPRE los niveles provistos en los datos.
   - PROHIBIDO mezclar escalas (ej: oro en 4500 vs 1800).

5. SI NO ESTAS SEGURO:
   - Reduce precision en vez de inventar.
   - Prefiere rangos ("caida adicional de 5-10%") en vez de niveles incorrectos.

6. PROHIBIDO NARRATIVE BIAS:
   - No fuerces una historia unica para explicar todo.
   - Si hay multiples drivers, mencionalos o reduce la certeza.
   - Usa lenguaje probabilistico cuando no hay evidencia clara: "probablemente", "consistente con", "sugiere".
"""

CHECK_FINAL = """
CHECK FINAL (OBLIGATORIO ANTES DE RESPONDER):
- Algún nivel es imposible dado el precio actual? → corrige.
- Alguna direccion esta invertida? → corrige.
- Algun escenario contradice otro? → corrige.
- Algun numero parece de otro regimen historico? → elimina y recalcula.
"""

# ── groq ─────────────────────────────────────────────────────────────────────
def _groq_call(prompt, max_tokens=800):
    api_key = os.environ.get('GROQ_API_KEY', '')
    model   = os.environ.get('GROQ_MODEL', 'deepseek-r1-distill-llama-70b')
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
{REGLAS_GLOBALES}
REGLA OBLIGATORIA: cada vez que menciones una variacion porcentual, DEBES especificar el horizonte \
temporal correspondiente entre parentesis: (1D), (W=5d), (M=21d) o (Q=63d). Ejemplo correcto: \
"Oil +47.1% (M=21d)". Ejemplo incorrecto: "Oil subio 47%".
EXCEPCION: el Fear & Greed (CNN F&G, BTC F&G) NO tiene horizonte temporal — NUNCA uses (1D), (W), (M) ni (Q) junto al F&G.

REGLA DE CAUSALIDAD:
- La CAUSA_RAIZ debe estar respaldada por: (a) al menos 1 dato de mercado Y (b) al menos 1 noticia o indicador macro.
- Debes mencionar al menos 2 drivers distintos si existen en los datos (geopolitica, tasas/inflacion, dolar/liquidez, credit stress). Ejemplo correcto: "tensiones geopoliticas + presion inflacionaria via petroleo". Si solo hay 1 dominante → justificar explicitamente por que domina.
- PROHIBIDO reducir todo a geopolitica si los datos muestran multiples presiones simultaneas.
- Si no hay evidencia suficiente → usar lenguaje probabilistico: "probablemente", "consistente con", "sugiere".
- PROHIBIDO atribuir TODO a una sola causa sin evidencia clara.
- PROHIBIDO usar narrativa geopolitica si los activos no lo reflejan claramente.

DATOS DE MERCADO — {TODAY}

[ACTIVOS] Precio | W (5d) | M (21d) | Q (63d):
{macro_txt}

[FRED MACRO]:
{fred_txt}
Crecimiento: {summarize_growth(fred)} | Liquidez: {summarize_liquidity(fred)} | Credito: {summarize_credit(fred)}

[SENTIMIENTO] (escala 0-100: 0=Panico total, 50=Neutro, 100=Euforia maxima — subir = menos miedo, bajar = mas miedo):
CNN Fear & Greed: {cnn_s}/100 ({cnn_r}) — ZONA: {"panico" if (cnn_s or 50) < 25 else "miedo" if (cnn_s or 50) < 45 else "neutro" if (cnn_s or 50) < 55 else "codicia"}{f', cambio vs ayer: {cnn_ch:+.1f}' if cnn_ch else ''}
BTC Fear & Greed: {btc_s}/100 ({btc_r})

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

DIVERGENCIAS: [max 2 divergencias notables con numeros. Usa lenguaje preciso y no binario. Ejemplo correcto: "credito no confirma el estres de equities (HY 320bps vs VIX 27)". Ejemplo incorrecto: "HY 320bps → no hay crisis". O escribe "Sin divergencias relevantes"]"""

    return _groq_call(prompt, max_tokens=600)


# ── ETAPA 2: Secciones ────────────────────────────────────────────────────────
def build_tldr(interp, cnn, btc, closes, fred):
    def r1d(t):
        s = closes[t].dropna() if t in closes.columns else None
        return _ret(_scalar(s, -2), _scalar(s)) if s is not None and len(s) >= 2 else 'N/D'

    dgs10 = fred.get('DGS10', {}).get('value', 'N/D')
    sp_q  = _ret(_scalar(closes['^GSPC'].dropna(), 0), _scalar(closes['^GSPC'].dropna())) \
            if '^GSPC' in closes.columns else 'N/D'

    retornos_1d = (
        f"S&P 500 (1D): {r1d('^GSPC')} | VIX (1D): {r1d('^VIX')} | "
        f"Oil WTI (1D): {r1d('CL=F')} | Oro (1D): {r1d('GC=F')} | "
        f"DXY (1D): {r1d('DX-Y.NYB')} | USDCLP (1D): {r1d('USDCLP=X')}"
    )

    prompt = f"""Usando esta interpretacion del mercado como base:

{interp}
{REGLAS_GLOBALES}
RETORNOS 1D EXACTOS DE HOY (usa estos numeros exactos — NO los cambies):
{retornos_1d}

REGLA OBLIGATORIA: cada variacion porcentual debe incluir su horizonte: (1D), (W=5d), (M=21d) o (Q=63d).
CONTEXTO F&G: escala 0-100 donde 0=Panico total, 100=Euforia maxima. Subir = menos miedo. Bajar = mas miedo. EXCEPCION: el Fear & Greed NO tiene horizonte temporal.

REGLA DE PRIORIDAD: el movimiento mas relevante debe ser el de MAYOR magnitud relativa en (1D) O el mas informativo macro (el que mejor explica el regimen). Elige uno y justifica brevemente por que es el mas relevante. No elegir arbitrariamente.

Genera un TL;DR de EXACTAMENTE 4 bullets en espanol, comenzando cada uno con "- ".
Deben cubrir: (1) regimen actual con causa, (2) movimiento mas relevante HOY usando SOLO retorno (1D) exacto, (3) tension o divergencia mas importante, (4) que vigilar esta semana.
Datos adicionales: 10Y {dgs10}% | S&P Q {sp_q} | CNN F&G {cnn.get('score','N/D')}/100 | BTC F&G {btc.get('score','N/D')}/100
Sin titulos, sin introduccion, solo los 4 bullets."""

    return _groq_call(prompt, max_tokens=400)


def build_3m_view(interp, closes, fred):
    oil_m  = _ret(_scalar(closes['CL=F'].dropna(),   -21), _scalar(closes['CL=F'].dropna()))  \
             if 'CL=F'  in closes.columns else 'N/D'
    sp_now = _scalar(closes['^GSPC'].dropna()) if '^GSPC' in closes.columns else 'N/D'
    sp_q   = _ret(_scalar(closes['^GSPC'].dropna(),  0),   _scalar(closes['^GSPC'].dropna())) \
             if '^GSPC' in closes.columns else 'N/D'
    oil_now = _scalar(closes['CL=F'].dropna()) if 'CL=F' in closes.columns else 'N/D'
    hy     = fred.get('BAMLH0A0HYM2', {}).get('value', 'N/D')
    dgs10  = fred.get('DGS10',        {}).get('value', 'N/D')
    t5yie  = fred.get('T5YIE',        {}).get('value', 'N/D')

    prompt = f"""Usando esta interpretacion del mercado como base:

{interp}
{REGLAS_GLOBALES}
PRECIOS ACTUALES EXACTOS (ancla para todos tus calculos):
S&P 500: {sp_now} | Oil WTI: {oil_now} | HY spread: {hy}% | 10Y: {dgs10}% | Inflacion impl 5Y: {t5yie}%

REGLA CRITICA — ANCLA NUMERICA PARA S&P 500:
- Nivel actual S&P 500: {sp_now}
- Una caida de 5% → {f'{sp_now * 0.95:.0f}' if isinstance(sp_now, float) else 'nivel_actual * 0.95'}
- Una caida de 10% → {f'{sp_now * 0.90:.0f}' if isinstance(sp_now, float) else 'nivel_actual * 0.90'}
- Una subida de 10% → {f'{sp_now * 1.10:.0f}' if isinstance(sp_now, float) else 'nivel_actual * 1.10'}
- PROHIBIDO usar niveles recordados o historicos. Si hay duda → usa SOLO % sin nivel absoluto.

REGLA para datos historicos: cada variacion porcentual pasada debe incluir su horizonte: (1D), (W=5d), (M=21d) o (Q=63d).
REGLA para proyecciones: NO uses etiquetas de horizonte. Usa lenguaje temporal: "en 3 meses", "hacia junio".
REGLA de niveles: usa PORCENTAJES principalmente; niveles absolutos SOLO si son consistentes con el precio actual calculado.

REGLAS ADICIONALES PARA ESCENARIOS:
- Cada escenario DEBE partir desde los PRECIOS ACTUALES EXACTOS provistos arriba.
- Antes de escribir un nivel: calcula nivel_actual * (1 + % cambio). Si el resultado no tiene sentido → usa solo %.
- PROHIBIDO: mezclar niveles de otro regimen historico, targets sin base matematica en precios actuales.

Genera un 3M VIEW con EXACTAMENTE 5 bullets en espanol, comenzando cada uno con "- ".
1. Base case (~60%): escenario mas probable con % concretos
2. Bear case (~20%): que podria salir peor, causa especifica y % de referencia
3. Bull case (~15%): sorpresa positiva y condicion necesaria concreta
4. Claves a monitorear: 2-3 indicadores con thresholds binarios claros. Formato: "indicador >X = estres / <Y = normalizacion". Evita rangos vagos como "entre 25-30". Ejemplo correcto: "VIX >32 = capitulacion, VIX <22 = normalizacion".
5. Implicancias para activos: equities / bonos / oro / crypto / commodities
{CHECK_FINAL}
Sin titulos, sin introduccion, solo los 5 bullets."""

    return _groq_call(prompt, max_tokens=700)


def build_wwcm(interp, tensions, closes, fred):
    tens_txt = '\n'.join(f'! {t}' for t in tensions) if tensions else 'Sin tensiones detectadas.'

    sp  = _scalar(closes['^GSPC'].dropna())  if '^GSPC'    in closes.columns else 'N/D'
    vix = _scalar(closes['^VIX'].dropna())   if '^VIX'     in closes.columns else 'N/D'
    oil = _scalar(closes['CL=F'].dropna())   if 'CL=F'     in closes.columns else 'N/D'
    hy  = fred.get('BAMLH0A0HYM2', {}).get('value', 'N/D')
    dgs = fred.get('DGS10',        {}).get('value', 'N/D')

    prompt = f"""Usando esta interpretacion del mercado como base:

{interp}
{REGLAS_GLOBALES}
PRECIOS ACTUALES (referencia exacta — NO uses otros niveles):
S&P 500: {sp} | VIX: {vix} | Oil WTI: {oil} | HY spread: {hy}% | 10Y Treasury: {dgs}%

REGLA OBLIGATORIA: cada variacion porcentual debe incluir su horizonte: (1D), (W=5d), (M=21d) o (Q=63d).
CONTEXTO F&G: escala 0-100 donde 0=Panico total, 100=Euforia maxima. Subir = menos miedo. EXCEPCION: el Fear & Greed NO tiene horizonte temporal.

REGLAS PARA UMBRALES:
- Cada umbral debe ser REALISTA respecto al nivel actual. No puedes usar niveles ya alcanzados como condicion futura.
- Ejemplo: si Oil = 98, no puedes decir "riesgo si supera 85" (ya fue superado).
- Usa buffers logicos: cerca = ±5%, significativo = ±10-15%.
- Cada condicion debe implicar un CAMBIO REAL de regimen, no algo trivial o ya ocurrido.

Tensiones detectadas: {tens_txt}

Genera "WHAT WOULD CHANGE MY MIND" con 4-5 condiciones que cambiarian el regimen actual.
Bullets en espanol comenzando con "- ". Umbral numerico realista + implicancia para mercados.
{CHECK_FINAL}
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
{REGLAS_GLOBALES}
REGLA OBLIGATORIA: cada variacion porcentual debe incluir su horizonte: (1D), (W=5d), (M=21d) o (Q=63d).

REGLA MULTI-FACTOR:
- Debes considerar al menos 2 factores (DXY + cobre, o riesgo global + cobre).
- Si los factores son contradictorios → mencionar la tension explicitamente.
- PROHIBIDO relaciones lineales simplistas tipo "S&P baja → CLP sube" sin matices.

REGLA TEMPORAL: usa SOLO lenguaje natural para proyecciones ("proximas semanas", "en el corto plazo", "en un mes"). PROHIBIDO usar etiquetas tecnicas (W=5d), (M=21d), (Q=63d) en proyecciones futuras — esas etiquetas son SOLO para retornos historicos pasados.

Genera un comentario sobre el USDCLP. 2-3 oraciones.
Datos: USDCLP {clp_now} | Retorno W: {clp_w} | M: {clp_m} | Q: {clp_q} | Cobre Q: {cu_q} | DXY Q: {dxy_q}
Incluye: nivel actual en contexto, presiones dominantes con al menos 2 factores, direccion esperada en las proximas semanas.
En espanol, directo al punto, sin titulos."""

    return _groq_call(prompt, max_tokens=300)


# ── PDF ───────────────────────────────────────────────────────────────────────
class PDF(FPDF):
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=14)
        self.set_margins(8, 10, 8)
        self._page_h = 297  # A4 mm

    def header(self):
        pass

    def footer(self):
        self.set_y(-14)
        self.set_font('Helvetica', '', 8)
        self.set_text_color(150, 150, 150)
        now = datetime.now().strftime('%Y-%m-%d %H:%M')
        self.cell(0, 4, clean(f'Vista Macro  |  Generado: {now}  |  yfinance + FRED + RSS'), align='C', ln=True)
        self.cell(0, 4, clean('(*) = interpretacion de Groq en base a los datos descargados'), align='C')

    def section(self, title, min_space=55, color=None):
        # Si no queda suficiente espacio, saltar a página nueva
        if self.get_y() + min_space > self._page_h - 14:
            self.add_page()
        self.set_fill_color(70, 70, 78)   # gris oscuro uniforme
        self.set_text_color(255, 255, 255)
        self.set_font('Helvetica', 'B', 8)
        self.cell(0, 6, clean(f'  {title}'), fill=True, ln=True)
        self.set_text_color(0, 0, 0)
        self.ln(1)

    def body(self, text, size=8, indent=8):
        self.set_font('Helvetica', '', size)
        old_lm = self.l_margin
        self.set_left_margin(indent)
        self.set_x(indent)
        self.multi_cell(0, 5, clean(text), align='J')
        self.set_left_margin(old_lm)

    def bullet(self, text, size=8):
        text = text.strip().lstrip('-* ')
        if not text:
            return
        self.set_font('Helvetica', '', size)
        old_lm = self.l_margin
        self.set_left_margin(15)
        self.set_x(8)
        self.cell(7, 5.2, clean('-'))
        self.multi_cell(0, 5.2, clean(text), align='L')
        self.set_left_margin(old_lm)


def build_pdf(closes, fred, cnn, btc, news, tensions,
              interp, tldr, v3, wwcm, usdclp_comment):
    pdf = PDF()
    pdf.add_page()

    # ── Título ────────────────────────────────────────────────────────────────
    date_fmt = datetime.strptime(TODAY, '%Y-%m-%d').strftime('%d/%m/%Y')
    pdf.set_font('Helvetica', 'B', 16)
    pdf.set_text_color(30, 30, 40)
    pdf.cell(0, 10, clean(f'Vista Macro  {date_fmt}'), ln=True, align='L')
    pdf.set_font('Helvetica', '', 8)
    pdf.set_text_color(130, 130, 140)
    pdf.cell(0, 5, clean('Macro Brief  |  yfinance + FRED + RSS'), ln=True)
    pdf.ln(4)
    pdf.set_draw_color(200, 200, 210)
    pdf.set_line_width(0.3)
    pdf.line(8, pdf.get_y(), 202, pdf.get_y())
    pdf.ln(4)
    pdf.set_text_color(0, 0, 0)

    # ── Disclaimer ────────────────────────────────────────────────────────────
    pdf.set_fill_color(245, 245, 230)
    pdf.set_draw_color(200, 190, 130)
    pdf.set_line_width(0.3)
    pdf.set_font('Helvetica', 'I', 8)
    pdf.set_text_color(100, 90, 40)
    yesterday = (datetime.strptime(TODAY, '%Y-%m-%d')
                 .replace(hour=0, minute=0, second=0))
    import calendar as _cal
    # retroceder al último día hábil
    wd = yesterday.weekday()  # 0=lun … 6=dom
    # si hoy es lunes el cierre es el viernes anterior
    delta = 3 if wd == 0 else 1
    from datetime import timedelta
    close_date = (yesterday - timedelta(days=delta)).strftime('%d/%m/%Y')
    disclaimer_text = (
        f'Todos los precios y retornos corresponden al cierre de mercado del {close_date}. '
        f'La interpretacion de los datos es generada automaticamente por un modelo de inteligencia artificial (Groq llama-3.3-70b) que puede cometer errores. '
        f'Este reporte es de caracter informativo y educativo. '
        f'No constituye asesoramiento financiero ni una recomendacion de inversion. '
        f'The Global Compounder no se responsabiliza por decisiones tomadas en base a este contenido.'
    )
    x0 = pdf.get_x()
    pdf.set_x(8)
    pdf.multi_cell(0, 4.5, clean(disclaimer_text), border=1, fill=True)
    pdf.set_text_color(0, 0, 0)
    pdf.set_draw_color(0, 0, 0)
    pdf.ln(3)

    # ── TL;DR ─────────────────────────────────────────────────────────────────
    pdf.section('[TL;DR] RESUMEN EJECUTIVO (*)')
    if tldr:
        for line in tldr.split('\n'):
            line = line.strip().lstrip('-* ')
            if line:
                pdf.body(line, size=8)
                pdf.ln(1)
    pdf.ln(2)

    # ── Sentimiento ───────────────────────────────────────────────────────────
    pdf.section('[S] SENTIMIENTO')
    cnn_ch = cnn.get('change', None)
    ch_txt = f'  (cambio: {cnn_ch:+.1f})' if cnn_ch else ''
    pdf.body(
        f"CNN Fear & Greed: {cnn.get('score','N/D')} — {cnn.get('rating','')}{ch_txt}"
        f"   |   BTC Fear & Greed: {btc.get('score','N/D')} — {btc.get('rating','')}", size=8)
    pdf.ln(2)

    # ── Resumen de activos ────────────────────────────────────────────────────
    pdf.section('[M] RESUMEN DE ACTIVOS')
    pdf.set_font('Helvetica', 'B', 8)
    pdf.set_fill_color(230, 235, 245)
    pdf.cell(36, 5.5, 'Activo',  fill=True)
    pdf.cell(20, 5.5, 'Precio',  fill=True, align='R')
    pdf.cell(16, 5.5, '1D',      fill=True, align='R')
    pdf.cell(16, 5.5, 'W (5d)',  fill=True, align='R')
    pdf.cell(16, 5.5, 'M (21d)', fill=True, align='R')
    pdf.cell(16, 5.5, 'Q (63d)', fill=True, align='R')
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
        r_1d = _ret(_scalar(s, -2),  price) if len(s) >= 2  else 'N/D'
        r_w  = _ret(_scalar(s, -5),  price) if len(s) >= 5  else 'N/D'
        r_m  = _ret(_scalar(s, -21), price) if len(s) >= 21 else 'N/D'
        r_q  = _ret(_scalar(s, 0),   price)
        pdf.set_font('Helvetica', '', 8)
        pdf.set_fill_color(248, 248, 252)
        yf_url = f'https://finance.yahoo.com/quote/{t.replace("^", "%5E").replace("=", "%3D")}'
        pdf.set_text_color(20, 80, 160)
        pdf.cell(36, 5, clean(label), fill=True, link=yf_url)
        pdf.set_text_color(0, 0, 0)
        pdf.cell(20, 5, f'{price:.2f}', fill=True, align='R')
        for r in [r_1d, r_w, r_m, r_q]:
            pdf.set_text_color(*ret_color(r))
            pdf.cell(16, 5, r, fill=True, align='R')
            pdf.set_text_color(0, 0, 0)
        pdf.ln()
    pdf.ln(2)

    # ── Tensiones ─────────────────────────────────────────────────────────────
    if tensions:
        pdf.section('[!] TENSIONES DETECTADAS')
        for t in tensions:
            pdf.body(t, size=7.5)
            pdf.ln(1)
        pdf.ln(1)

    # ── Noticias ──────────────────────────────────────────────────────────────
    pdf.section('[N] NOTICIAS')
    for a in news[:8]:
        url = a.get('url', '')
        pdf.set_left_margin(8)
        pdf.set_x(8)
        pdf.set_font('Helvetica', 'B', 8)
        if url:
            pdf.set_text_color(20, 80, 160)
        pdf.multi_cell(0, 4.5, clean(f'[{a["source"]}] {a["title"]}'), align='L', link=url)
        pdf.set_text_color(0, 0, 0)
        if a.get('summary'):
            pdf.set_font('Helvetica', '', 8)
            pdf.set_left_margin(8)
            pdf.set_x(8)
            pdf.multi_cell(0, 4, clean(a['summary'][:160]), align='J')
        pdf.ln(1)
    pdf.ln(1)

    # ── Interpretacion base ───────────────────────────────────────────────────
    _LABEL_MAP = {
        'REGIMEN': 'Régimen', 'CAUSA_RAIZ': 'Causa Raíz',
        'SENALES': 'Señales', 'DIVERGENCIAS': 'Divergencias',
    }
    pdf.section('[I] INTERPRETACION BASE (*)', min_space=90)
    if interp:
        for line in interp.split('\n'):
            line = line.strip()
            if not line:
                pdf.ln(1)
            elif ':' in line and line.split(':')[0].strip().upper().replace(' ', '_') in _LABEL_MAP:
                parts = line.split(':', 1)
                raw_label = parts[0].strip().upper().replace(' ', '_')
                label = _LABEL_MAP.get(raw_label, parts[0].strip().title())
                content = parts[1].strip() if len(parts) > 1 else ''
                old_lm = pdf.l_margin
                pdf.set_left_margin(8)
                pdf.set_x(8)
                pdf.set_font('Helvetica', 'B', 8)
                pdf.set_text_color(50, 50, 50)
                pdf.cell(0, 5, clean(label + ':'), ln=True)
                pdf.set_text_color(0, 0, 0)
                if content:
                    pdf.set_font('Helvetica', '', 8)
                    pdf.set_x(8)
                    pdf.multi_cell(0, 5, clean(content), align='J')
                pdf.set_left_margin(old_lm)
            elif line.startswith('-'):
                pdf.body(line.lstrip('-* '), size=8, indent=8)
            else:
                pdf.body(line, size=8, indent=8)
    pdf.ln(2)

    # ── USDCLP ────────────────────────────────────────────────────────────────
    clp = closes['USDCLP=X'].dropna() if 'USDCLP=X' in closes.columns else None
    cu  = closes['HG=F'].dropna()     if 'HG=F'     in closes.columns else None
    dxy = closes['DX-Y.NYB'].dropna() if 'DX-Y.NYB' in closes.columns else None
    oil = closes['CL=F'].dropna()     if 'CL=F'     in closes.columns else None
    vix = closes['^VIX'].dropna()     if '^VIX'     in closes.columns else None
    sp  = closes['^GSPC'].dropna()    if '^GSPC'    in closes.columns else None

    pdf.section('[CLP] USDCLP / PESO CHILENO (*)')

    # Headline: precio + 1D + YTD
    if clp is not None and len(clp) >= 2:
        clp_val = _scalar(clp)
        clp_1d  = _ret(_scalar(clp, -2), clp_val) if len(clp) >= 2 else 'N/D'
        clp_ytd = _ret(next((float(clp.loc[clp.index >= f'{datetime.today().year}-01-01'].iloc[0])
                              for _ in [1] if len(clp.loc[clp.index >= f'{datetime.today().year}-01-01']) > 0),
                             float(clp.iloc[0])), clp_val)
        pdf.set_font('Helvetica', 'B', 8)
        pdf.set_x(8)
        pdf.cell(30, 6, clean(f'USDCLP: {clp_val:.0f}'))
        pdf.set_text_color(*ret_color(clp_1d))
        pdf.cell(28, 6, clean(f'1D: {clp_1d}'))
        pdf.set_text_color(0, 0, 0)
        pdf.cell(28, 6, clean(f'YTD: {clp_ytd}'))
        pdf.ln(7)

    # Tabla de factores
    fac_w = [38, 26, 22, 36]
    pdf.set_font('Helvetica', 'B', 8)
    pdf.set_fill_color(220, 230, 245)
    for h, w in zip(['Factor', 'Valor', 'YTD%', 'Efecto en CLP'], fac_w):
        pdf.cell(w, 5.5, h, fill=True, align='C' if h != 'Factor' else 'L')
    pdf.ln()

    def ytd(s):
        try:
            p0 = float(s.loc[s.index >= f'{datetime.today().year}-01-01'].iloc[0])
            return f'{(float(s.iloc[-1]) / p0 - 1) * 100:+.1f}%'
        except Exception:
            return 'N/D'

    factors = []
    if cu  is not None: factors.append(('Cobre HG=F', f'${_scalar(cu):.3f}/lb', ytd(cu),  ('alto = CLP +', True)))
    if dxy is not None: factors.append(('DXY',        f'{_scalar(dxy):.1f}',    ytd(dxy), ('alto = CLP -', False)))
    if oil is not None: factors.append(('Oil WTI',    f'${_scalar(oil):.1f}',   ytd(oil), ('alto = CLP -', False)))
    if vix is not None: factors.append(('VIX',        f'{_scalar(vix):.1f}',    ytd(vix), ('alto = CLP -', False)))
    if sp  is not None: factors.append(('S&P 500',    'YTD',                    ytd(sp),  ('sube = CLP +', True)))
    ff = fred.get('FEDFUNDS', {}).get('value')
    if ff: factors.append(('Fed Funds', f'{ff:.2f}%', '---', ('alto = CLP -', False)))

    for name, val, ytd_v, (eff, positive) in factors:
        pdf.set_font('Helvetica', '', 8)
        pdf.set_fill_color(248, 250, 254)
        pdf.cell(fac_w[0], 5, clean(name), fill=True)
        pdf.cell(fac_w[1], 5, clean(val),  fill=True, align='R')
        pdf.set_text_color(*ret_color(ytd_v))
        pdf.cell(fac_w[2], 5, clean(ytd_v), fill=True, align='R')
        pdf.set_text_color(0, 130, 0) if positive else pdf.set_text_color(160, 0, 0)
        pdf.cell(fac_w[3], 5, clean(eff), fill=True, align='C')
        pdf.set_text_color(0, 0, 0)
        pdf.ln()
    pdf.ln(3)

    if usdclp_comment:
        pdf.set_font('Helvetica', 'I', 8)
        pdf.set_left_margin(8)
        pdf.set_x(8)
        pdf.multi_cell(0, 5, clean(usdclp_comment), align='J')
        pdf.set_left_margin(8)
    pdf.ln(3)

    # ── Upcoming events ───────────────────────────────────────────────────────
    events = upcoming_next(5)
    if events:
        pdf.section('[CAL] CALENDARIO MACRO')
        PRIORITY_COLOR = {'HIGH': (200, 0, 0), 'MED': (180, 120, 0), 'LOW': (80, 80, 80)}
        for date, name, pri, desc in events:
            pc = PRIORITY_COLOR.get(pri, (80, 80, 80))
            pdf.set_x(8)
            pdf.set_font('Helvetica', 'B', 8)
            pdf.set_text_color(*pc)
            pdf.cell(28, 5, clean(f'{date}  [{pri}]'))
            pdf.set_text_color(0, 0, 0)
            pdf.set_font('Helvetica', '', 8)
            pdf.cell(0, 5, clean(name), ln=True)
        pdf.ln(2)

    # ── WWCM ──────────────────────────────────────────────────────────────────
    pdf.section('[W] WHAT WOULD CHANGE MY MIND (*)', min_space=70)
    if wwcm:
        for line in wwcm.split('\n'):
            line = line.strip().lstrip('-* ')
            if line:
                pdf.body(line, size=8)
                pdf.ln(1)
    pdf.ln(2)

    # ── 3M View ───────────────────────────────────────────────────────────────
    pdf.section('[3M] 3M VIEW — BASED ON CURRENT BRIEF (*)', min_space=70)
    if v3:
        for line in v3.split('\n'):
            line = line.strip().lstrip('-* ')
            if line:
                pdf.body(line, size=8)
                pdf.ln(1)
    pdf.ln(2)

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
    wwcm           = build_wwcm(interp, tensions, closes, fred)
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
