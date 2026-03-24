#!/usr/bin/env python3
"""
Pipeline 3 — Vista Macro (distribución pública)
Arquitectura 2 etapas:
  Etapa 1: Interpretacion base (1 llamada Gemini) — regimen + causa + senales + tensiones
  Etapa 2: Secciones del reporte (4 llamadas Gemini usando la interpretacion base)

Datos:
  - yfinance: 12 macro tickers × 63 sesiones (W=5, M=21, Q=63)
  - FRED: tasas, spreads, empleo, crecimiento, liquidez
  - CNN F&G + BTC F&G
  - RSS news (10 articulos filtrados)
  - Calendario macro (UPCOMING_EVENTS)

Output: data/daily_summaries/YYYY-MM-DD_p3.pdf + .md
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

import os, re, json, time, warnings, requests
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


REGLAS_CONSISTENCIA = """
REGLAS DE CONSISTENCIA NUMERICA (OBLIGATORIAS):

1. NIVELES Y PROYECCIONES
   Toda proyeccion debe ser consistente con el spot actual:
   nivel_futuro = nivel_actual x (1 + % cambio).
   PROHIBIDO usar niveles historicos, escalas equivocadas o referencias de otro regimen.

2. DIRECCION LOGICA
   "Sube X%" implica nivel futuro MAYOR al actual.
   "Baja X%" implica nivel futuro MENOR al actual.
   Si hay contradiccion entre direccion y nivel, NO escribir ese escenario.

3. COHERENCIA INTERNA
   Si Oil=98, "sube a 80" es una caida.
   Si un umbral ya fue alcanzado o superado, NO usarlo como trigger futuro.
   No usar thresholds imposibles o redundantes dado el estado actual.

4. MAGNITUD REALISTA
   En horizonte de 3 meses, movimientos tipicos en equities estan en torno a ±5-15%.
   Un movimiento mayor requiere una causa explicita y proporcional.
   Si no hay base suficiente para un nivel preciso, usar rango.

5. CONSISTENCIA CROSS-ASSET
   Los escenarios deben conversar entre activos.
   Oil fuerte y persistente debe implicar algo en inflacion esperada, VIX o crecimiento.
   VIX cayendo no es consistente con proyeccion de caida fuerte en equities salvo explicacion explicita.
   Stress en credito debe tener algun eco en riesgo, spreads o equities.

6. CONSISTENCIA ENTRE ESCENARIOS
   Bear_nivel < Base_nivel < Bull_nivel.
   Cada escenario debe tener trigger, path y resultado distinguibles.
   PROHIBIDO construir tres versiones de intensidad del mismo relato sin diferenciacion real.

7. TERMINOS PRECISOS
   No usar "capitulacion", "stress", "normalizacion", "alivio" o "dislocacion" sin respaldo numerico
   o condicion observable. Si se usa un termino fuerte, debe estar anclado a un nivel o trigger concreto.
"""

REGLAS_EDITORIALES = """
REGLAS EDITORIALES (OBLIGATORIAS):

1. PROHIBIDO LENGUAJE GENERICO DE MARKET COMMENTARY
   No usar "confirma", "refleja", "indica", "sugiere", "se observa", "presion alcista", "presion bajista"
   salvo que se especifique: que hipotesis esta en juego, que dato la respalda, y que matiz existe.

2. DRIVER MATERIAL
   Un driver solo califica si tiene evidencia observable: movimiento >2% en horizonte util,
   O cambio claro en spread/yield, O noticia con efecto directo visible en precio.
   Un activo estable NO es driver activo. Puede ser divergencia o no-confirmacion, pero no driver.

3. PARSIMONIA
   Si los datos apuntan a una lectura simple, el analisis debe permitirse ser simple.
   Si solo hay 1 driver material, declararlo explicitamente.
   Si no hay divergencias relevantes, escribir "Sin divergencias relevantes".
   No forzar multiples drivers, contrapesos o tensiones para sonar sofisticado.

4. CONTRAPESO REAL
   Solo mencionar factor contrario si existe evidencia observable.
   Si todos los factores materiales apuntan igual, declararlo como "presion unidireccional".
   PROHIBIDO inventar equilibrio o balance cuando los datos no lo muestran.

5. DENSIDAD
   Cada oracion debe agregar informacion nueva.
   Si una oracion puede eliminarse sin perdida de insight, debe eliminarse.
   PROHIBIDO rellenar con reformulaciones de la misma tesis.

6. INCERTIDUMBRE HONESTA
   Si la evidencia es mixta, mostrar la tension.
   PROHIBIDO cerrar una historia limpia cuando los activos no lo permiten.
   Si falta confirmacion, decirlo; no rellenarlo con causalidad elegante.

7. TONO
   Escribir como market note para lector informado.
   No explicar VIX, spreads, DXY, HY, F&G ni conceptos basicos.
   Evitar tono escolar, tono de consultor y tono de comentarista generico.

8. NO DRAMATIZACION SIN EVIDENCIA
   No usar lenguaje epico o sentencioso sin respaldo observable.
   Evitar "falta limpieza", "no capitula", "mercado nervioso", "modo panico" salvo soporte cuantitativo.

9. DISTINCION FUNCIONAL ENTRE BLOQUES
   Cada bloque hace una tarea distinta:
   Interpretation = marco analitico. TL;DR = priorizacion. 3M View = proyeccion.
   WWCM = falsacion. USDCLP = traduccion al FX local.
   PROHIBIDO reformular la misma tesis con sinonimos entre secciones.

10. FILTRO FINAL DE CALIDAD
    Antes de responder, eliminar mentalmente cualquier frase que un analista exigente tacharia por:
    obvia, generica, intercambiable con cualquier otro brief, o propia de un LLM disciplinado.
"""

EJEMPLOS_EDITORIALES = """
EJEMPLOS CRITICOS — bueno vs malo:

MALO: "lo que sugiere que los inversores estan preocupados por la situacion actual"
BUENO: eliminar — no agrega dato ni hipotesis accionable

MALO: "podria continuar con un sesgo a la depreciacion en el corto plazo"
BUENO: "DXY +1.7% (Q) y cobre -3.8% (Q) apuntan en la misma direccion: presion unidireccional a CLP debil"

MALO: "cierta cantidad de inversores que no estan completamente convencidos"
BUENO: "HY spread estable en 320bps — credito no convalida el panic-buying de puts en VIX"

MALO (en 3M Implicancias): "activo que mas se beneficia: oro (refugio seguro)" cuando oro cae -16% (M)
BUENO: verificar precios primero — un activo que baja NO puede ser el mas beneficiado del regimen actual

MALO (WWCM): "VIX sube a 40: el stress se profundiza" — confirma, no falsa
BUENO (WWCM): "VIX cae < 18 en 5D: demanda por proteccion se agota, regimen pierde su senal dominante"

MALO: "en el contexto actual de incertidumbre"
BUENO: eliminar completamente o reemplazar con el dato especifico que genera esa incertidumbre
"""

CHECK_FINAL = """
CHECK FINAL (OBLIGATORIO):
- Algun nivel imposible dado el spot actual? → corregir.
- Alguna direccion invertida? → corregir.
- Bear_nivel < Base_nivel < Bull_nivel? → verificar.
- Algun escenario contradice otro? → corregir.
- Algun bullet repite una idea ya expresada en este mismo bloque? → eliminar o profundizar.
- Algun trigger de WWCM hace que la tesis quede MAS justificada? → no es falsacion; reemplazar.
- Algun contrapeso fue inventado solo para balancear el texto? → eliminar.
- Algun termino fuerte quedo sin ancla numerica u observable? → corregir.
"""

# ── helpers de parsing ───────────────────────────────────────────────────────
def _parse_interp(interp):
    """Extrae campos estructurados del texto de interpretacion."""
    fields = {'REGIMEN': '', 'LECTURA_CENTRAL': '', 'SENALES': '', 'DIVERGENCIAS': ''}
    if not interp:
        return fields
    current = None
    buf = []
    for line in interp.split('\n'):
        s = line.strip()
        matched = False
        for key in fields:
            if s.startswith(key + ':'):
                if current:
                    fields[current] = '\n'.join(buf).strip()
                current = key
                buf = [s[len(key) + 1:].strip()]
                matched = True
                break
        if not matched and current:
            buf.append(s)
    if current:
        fields[current] = '\n'.join(buf).strip()
    return fields


# ── llm call (Gemini primario, Groq como fallback) ───────────────────────────
def _groq_call(prompt, max_tokens=800):
    gemini_key = os.environ.get('GEMINI_API_KEY', '')
    if gemini_key:
        return _gemini_call(prompt, gemini_key, max_tokens)
    return _groq_call_impl(prompt, max_tokens)

def _gemini_call(prompt, api_key, max_tokens=800, model=None):
    model = model or os.environ.get('GEMINI_MODEL', 'gemini-2.5-flash')
    url   = f'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}'
    for attempt in range(4):
        try:
            r = requests.post(
                url,
                headers={'Content-Type': 'application/json'},
                json={
                    'contents': [{'role': 'user', 'parts': [{'text': prompt}]}],
                    'generationConfig': {
                        'maxOutputTokens': 8192,
                        'temperature':     0.3,
                        'thinkingConfig':  {'thinkingBudget': 0},
                    },
                },
                timeout=120,
            )
            if r.status_code == 429:
                wait = 35 * (attempt + 1)
                print(f'  429 rate limit — esperando {wait}s (intento {attempt+1}/4)...')
                time.sleep(wait)
                continue
            if not r.ok:
                print(f'  WARNING Gemini {r.status_code}: {r.text[:200]}')
                return None
            candidates = r.json().get('candidates', [])
            if not candidates:
                print(f'  WARNING Gemini: no candidates in response')
                return None
            parts = candidates[0]['content']['parts']
            text  = ''.join(p['text'] for p in parts if not p.get('thought', False))
            return text.strip() or None
        except Exception as e:
            print(f'  WARNING Gemini: {e}')
            return None
    print('  WARNING Gemini: se agotaron los reintentos (429 persistente)')
    return None

def _groq_call_impl(prompt, max_tokens=800):
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

    prompt = f"""Eres un analista macro senior y editor de market note.
{REGLAS_CONSISTENCIA}
{REGLAS_EDITORIALES}
{EJEMPLOS_EDITORIALES}
Objetivo: construir el marco analitico del dia. Identificar el regimen dominante, el o los drivers \
materiales y la principal divergencia interna del tape. No resumir noticias; sintetizar mercado.

Reglas especificas:
- Ordena drivers por impacto observable en precio, no por narrativa.
- Un driver requiere evidencia concreta. Un activo estable NO califica como driver.
- Si solo hay 1 driver material, declararlo y explicar brevemente por que los demas no califican.
- Buscar activamente el dato que menos encaja con la lectura central.
- Si no hay divergencia relevante, escribir exactamente: "Sin divergencias relevantes".
- No usar "causa raiz". No usar verbos genericos salvo con hipotesis + dato + matiz.

REGLA OBLIGATORIA: toda variacion porcentual incluye horizonte: (1D), (W=5d), (M=21d) o (Q=63d).
Ejemplo correcto: "Oil +47.1% (M=21d)". Ejemplo incorrecto: "Oil subio 47%".
EXCEPCION: CNN F&G y BTC F&G NO tienen horizonte temporal.

DATOS DE MERCADO — {TODAY}

[ACTIVOS] Precio | W (5d) | M (21d) | Q (63d):
{macro_txt}

[FRED MACRO]:
{fred_txt}
Crecimiento: {summarize_growth(fred)} | Liquidez: {summarize_liquidity(fred)} | Credito: {summarize_credit(fred)}

[SENTIMIENTO] (0=Panico total, 50=Neutro, 100=Euforia maxima — subir = menos miedo, bajar = mas miedo):
CNN Fear & Greed: {cnn_s}/100 ({cnn_r}) — ZONA: {"panico" if (cnn_s or 50) < 25 else "miedo" if (cnn_s or 50) < 45 else "neutro" if (cnn_s or 50) < 55 else "codicia"}{f', cambio vs ayer: {cnn_ch:+.1f}' if cnn_ch else ''}
BTC Fear & Greed: {btc_s}/100 ({btc_r})

[TENSIONES DETECTADAS]:
{tens_txt}

[NOTICIAS DEL DIA]:
{news_txt if news_txt else 'Sin noticias disponibles.'}

Produce la interpretacion con EXACTAMENTE este formato (sin texto antes ni despues):

REGIMEN: [max 5 palabras. Formato: postura_de_riesgo / driver_principal.
La postura es observable en equities+VIX. El driver es el de mayor impacto concreto en precio.
Ej correcto: "Risk-Off / Oil-Driven Stress". Ej incorrecto: "Mercado en modo defensivo".]

LECTURA_CENTRAL: [1-2 oraciones con al menos un dato o numero relevante. Debe decir que esta
dominando el tape Y que NO esta confirmando del todo, si aplica. Sin "se observa", "refleja", "causa raiz".]

SENALES:
- Señal 1 — EQUITIES o VOLATILIDAD: [activo] [(horizonte)]: [dato concreto] — [mecanismo: por que ESTE activo es la senal, no otro]
- Señal 2 — TASAS, CREDITO o COMMODITIES: [activo] [(horizonte)]: [dato concreto] — [mecanismo: que revela del regimen que la senal 1 no revela]
- Señal 3 — SENTIMIENTO, DIVISA o ACTIVO QUE DIVERGE: [activo] [(horizonte)]: [dato concreto] — [mecanismo: como esto CONTRADICE o MATIZA las senales 1 y 2]

REGLA CRITICA para [mecanismo]:
  Responde una de estas preguntas concretas:
  (a) que umbral o condicion activa este dato?
  (b) por que este activo especificamente y no otro?
  (c) que contradiccion interna revela?
  PROHIBIDO: "muestra alivio", "sugiere preocupacion", "indica riesgo", "implica cautela", "refleja incertidumbre".

DIVERGENCIAS: [el dato que menos encaja con la lectura central, con numero. O "Sin divergencias relevantes".]

Regla critica: las 3 senales deben aportar informacion distinta entre si.
PROHIBIDO tres senales que digan lo mismo desde tres activos distintos."""

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

    parsed = _parse_interp(interp)
    regimen        = parsed['REGIMEN']
    lectura        = parsed['LECTURA_CENTRAL']

    prompt = f"""Eres un editor de market note.
{REGLAS_CONSISTENCIA}
{REGLAS_EDITORIALES}
{EJEMPLOS_EDITORIALES}
Objetivo: entregar en 4 bullets lo unico que el lector debe retener hoy.
No resumir todo el analisis. Seleccionar y priorizar.

MARCO DEL DIA (referencia — NO reformular, priorizar):
REGIMEN: {regimen}
LECTURA_CENTRAL: {lectura}

RETORNOS 1D EXACTOS (usar estos numeros exactos — NO cambiarlos):
{retornos_1d}

REGLA OBLIGATORIA: toda variacion porcentual incluye horizonte: (1D), (W=5d), (M=21d) o (Q=63d).
EXCEPCION: Fear & Greed NO tiene horizonte temporal.
CONTEXTO F&G: 0=Panico total, 100=Euforia maxima. Subir = menos miedo.

Reglas especificas:
- Cada bullet debe introducir una idea distinta.
- No reformular literalmente la LECTURA_CENTRAL.
- Bullet 2: movimiento mas informativo macro del dia, no necesariamente el de mayor magnitud.
  "Mas informativo" = el movimiento que revela algo sobre el regimen que los otros no revelan.
  PROHIBIDO "confirma", "refleja", "indica", "sugiere" salvo con hipotesis + evidencia + matiz.
  Ejemplo correcto: "VIX +11.3% (1D) con S&P -0.8%: la demanda por proteccion sube mas rapido que
  el deterioro del equity — stress latente"
- Bullet 4: NO es watchlist generica. Es la incertidumbre concreta que el dia dejo abierta.
  Ejemplo correcto: "Credito no se movio hoy; si HY supera 360bps esta semana, el stress de
  equities deja de ser aislado."

Genera TL;DR de EXACTAMENTE 4 bullets en espanol, comenzando cada uno con "- ":
1. Diagnostico del regimen + driver principal, con dato.
2. Movimiento mas informativo del dia (retorno 1D exacto) + implicancia macro.
3. Tension o divergencia relevante, con numero. DEBE ser una dimension distinta a bullet 2.
4. Incertidumbre concreta que hoy quedo abierta. DEBE ser distinta a la tension de bullet 3.

REGLA ANTI-SOLAPAMIENTO (critica):
Antes de escribir bullet N, verificar que su idea central no aparece ya en bullets anteriores.
Si dos bullets dicen variantes de "el mercado no normalizo completamente", eliminar uno y
reemplazarlo con una dimension diferente del dia (cross-asset, credito, FX, datos macro).
Cuatro bullets = cuatro insights distintos. Si no hay cuatro insights reales, el cuarto puede
ser el mas debil, pero no puede repetir la misma lectura con otras palabras.

Datos adicionales: 10Y {dgs10}% | S&P Q {sp_q} | CNN F&G {cnn.get('score','N/D')}/100 | BTC F&G {btc.get('score','N/D')}/100
Sin titulos, sin introduccion, solo los 4 bullets.

Regla critica: TL;DR prioriza. No explica el marco, no proyecta 3 meses y no falsa la tesis."""

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

    parsed      = _parse_interp(interp)
    regimen     = parsed['REGIMEN']
    lectura     = parsed['LECTURA_CENTRAL']
    divergencia = parsed['DIVERGENCIAS']

    prompt = f"""Eres un analista macro senior.
{REGLAS_CONSISTENCIA}
{REGLAS_EDITORIALES}
{EJEMPLOS_EDITORIALES}
Objetivo: proyectar la evolucion mas probable del regimen en horizonte de 3 meses.
Distinguir continuidad, deterioro o alivio/reversion parcial.
No repetir la tesis del dia; proyectarla.

MARCO ACTUAL (proyectar su evolucion — NO describir de nuevo, sino proyectar):
REGIMEN: {regimen}
LECTURA_CENTRAL: {lectura}
DIVERGENCIAS: {divergencia}

PRECIOS ACTUALES EXACTOS (ancla para todos los calculos):
S&P 500: {sp_now} | Oil WTI: {oil_now} | HY spread: {hy}% | 10Y: {dgs10}% | Inflacion impl 5Y: {t5yie}%

ANCLA NUMERICA S&P 500 (PROHIBIDO usar otros niveles):
- Nivel actual: {sp_now}
- Caida 5%  → {f'{sp_now * 0.95:.0f}' if isinstance(sp_now, float) else 'nivel_actual x 0.95'}
- Caida 10% → {f'{sp_now * 0.90:.0f}' if isinstance(sp_now, float) else 'nivel_actual x 0.90'}
- Subida 10% → {f'{sp_now * 1.10:.0f}' if isinstance(sp_now, float) else 'nivel_actual x 1.10'}

ESTRUCTURA DE PROBABILIDADES (definir ANTES de escribir los bullets):
- Base + Bear + Bull = exactamente 100%.
- Rangos orientativos: Base 50-65%, Bear 15-30%, Bull 10-25%.
- Elige los tres valores ahora y usalos consistentemente.

REGLA para retornos historicos: incluir horizonte (1D), (W=5d), (M=21d) o (Q=63d).
REGLA para proyecciones: lenguaje temporal natural ("en 3 meses", "hacia junio") — PROHIBIDO labels backward.
REGLA de niveles: usar % principalmente; niveles absolutos SOLO si son matematicamente consistentes con spot.

Reglas especificas:
- Cada escenario: trigger distinto, path distinto, nivel o rango distinto.
- PROHIBIDO tres versiones de intensidad del mismo relato.
- Si el escenario base incluye alivio o rebote, decir explicitamente si es rebote tecnico,
  normalizacion parcial o reversion del regimen.
- Claves a monitorear = thresholds que discriminan entre los 3 escenarios definidos.
- Implicancias por activo derivadas del escenario BASE.
  PROHIBIDO cliches automaticos tipo "tech gana / oro neutral / energia pierde" sin mecanismo explicito.
- La etiqueta del escenario y el path deben conversar entre si.

Genera 3M VIEW con EXACTAMENTE 5 bullets en espanol, comenzando cada uno con "- ":
1. Base case (XX%): trigger especifico + path + nivel/rango consistente con spot.
2. Bear case (XX%): trigger distinto al del base + transmision cross-asset + % o nivel desde spot.
3. Bull case (XX%): condicion concreta + path distinto + nivel/rango. Suma Base+Bear+Bull = 100%.
   Verificar: Bear_nivel < Base_nivel < Bull_nivel.
4. Claves: 2-3 thresholds binarios que discriminen entre los 3 escenarios.
   Formato: "indicador >X = [implicancia concreta] / <Y = [implicancia concreta]".
5. Implicancias: activo que mas se beneficia (y por que especifico del escenario base) /
   neutral / mas vulnerable.
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

    parsed   = _parse_interp(interp)
    regimen  = parsed['REGIMEN']
    lectura  = parsed['LECTURA_CENTRAL']
    senales  = parsed['SENALES']

    prompt = f"""Eres un analista macro senior.
{REGLAS_CONSISTENCIA}
{REGLAS_EDITORIALES}
{EJEMPLOS_EDITORIALES}
Objetivo: definir que evidencia concreta invalidaria o debilitaria la lectura central.
No listar variables importantes. No proyectar. No confirmar la tesis. Solo falsarla.

TESIS Y SENALES VIGENTES (identificar que evidencia las invalidaria — NO reformularlas):
REGIMEN: {regimen}
LECTURA_CENTRAL: {lectura}
SENALES:
{senales}

PRECIOS ACTUALES (referencia exacta):
S&P 500: {sp} | VIX: {vix} | Oil WTI: {oil} | HY spread: {hy}% | 10Y Treasury: {dgs}%

REGLA OBLIGATORIA: toda variacion porcentual incluye horizonte (1D), (W=5d), (M=21d) o (Q=63d).
EXCEPCION: Fear & Greed NO tiene horizonte temporal.

DEFINICION OPERATIVA DE FALSACION (critica):
Un bullet INVALIDA si describe evidencia INCONSISTENTE con el regimen actual.
Un bullet NO invalida si solo describe una version mas intensa del mismo regimen.

Ejemplo — Regimen: Risk-Off / Oil-Driven Stress:
  INVALIDA: "Oil cae >8% sin escalada geopolitica: la causalidad central pierde soporte"
  NO INVALIDA: "VIX sube a 40: el stress se profundiza" → eso confirma, no falsa.

Test obligatorio para cada bullet: si el trigger se cumple y el regimen queda MAS justificado,
NO es falsacion. Reemplazar por algo que realmente ponga en duda la tesis.

Reglas especificas:
- La mayoria de bullets debe invalidar o debilitar la tesis.
- PROHIBIDO triggers que solo intensifican el regimen actual.
- Cada bullet ataca una DIMENSION DISTINTA de la tesis:
    * Precio del driver principal (ej: oil revierte sin razon geopolitica)
    * Causalidad (ej: el activo clave se mueve pero por driver diferente al asumido)
    * Cross-asset inconsistente (ej: credito no acompana el rebote de equities)
    * Sentimiento / posicionamiento (ej: CNN F&G sube rapido sin catalizador claro)
  PROHIBIDO dos bullets que ataquen la misma dimension con distintos umbrales.
- Si tensiones pre-computadas contradicen LECTURA_CENTRAL, priorizar LECTURA_CENTRAL.
  Tensiones pre-computadas (dato adicional, no marco): {tens_txt}
- Cada bullet explica por que ese trigger obliga a revisar la lectura.
- PROHIBIDO bullets redundantes entre si.
- PROHIBIDO umbrales ya alcanzados como trigger futuro.

Genera WHAT WOULD CHANGE MY MIND con 4-5 bullets en espanol comenzando con "- ".
Estructura: [variable] + [umbral numerico] + [por que invalida o debilita la tesis].
{CHECK_FINAL}
Sin titulos, sin introduccion, solo los bullets.

Regla critica: WWCM es falsacion real. No mezclar con "what would reinforce the view"."""

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

    parsed  = _parse_interp(interp)
    regimen = parsed['REGIMEN']

    prompt = f"""Eres un analista de FX con enfoque macro.
{REGLAS_CONSISTENCIA}
{REGLAS_EDITORIALES}
{EJEMPLOS_EDITORIALES}
Objetivo: traducir el marco global al USDCLP con foco en drivers observables.
Priorizar DXY y cobre. Usar riesgo global solo si su efecto es visible en los datos.

CONTEXTO DE RIESGO GLOBAL: {regimen}

REGLA OBLIGATORIA: toda variacion porcentual historica incluye horizonte (1D), (W=5d), (M=21d) o (Q=63d).
REGLA TEMPORAL: proyecciones en lenguaje natural ("proximas semanas", "en el corto plazo").
PROHIBIDO etiquetas backward en proyecciones futuras.

Reglas especificas:
- Analizar primero DXY y cobre.
- Riesgo global entra solo si agrega informacion observable sobre USDCLP.
- Mencionar factor contrario SOLO si existe evidencia en los datos provistos.
- Si DXY y cobre apuntan en la misma direccion para el par, declararlo como presion unidireccional.
  Ejemplo: "Presion unidireccional: DXY firme y cobre debil apuntan ambos a depreciacion del CLP."
- PROHIBIDO inventar contrapesos para balancear el texto.
- La conclusion debe tener sesgo claro: apreciacion / depreciacion / lateral con sesgo.
- PROHIBIDO terminar en ambiguedad tipo "dependera" sin razon explicita.
- No usar "presion alcista/bajista" sin nombrar que factor la genera.
- Si no existe factor contrario observable, decirlo explicitamente.

Genera comentario USDCLP de 2-3 oraciones en espanol:
1. Nivel actual en contexto, con referencia historica concreta o comparativa util.
2. Driver dominante + factor contrario (si existe en los datos) + balance real.
3. Sesgo de direccion en lenguaje probabilistico pero claro.

Datos: USDCLP {clp_now} | W: {clp_w} | M: {clp_m} | Q: {clp_q} | Cobre Q: {cu_q} | DXY Q: {dxy_q}
Directo al punto, sin titulos."""

    return _groq_call(prompt, max_tokens=300)


# ── Pase editorial ────────────────────────────────────────────────────────────
def editorial_pass(interp, tldr, v3, wwcm, clp_comment, closes):
    """Segunda llamada a Gemini como editor: elimina lenguaje generico y corrige contradicciones cross-seccion."""
    api_key = os.environ.get('GEMINI_API_KEY', '')
    if not api_key:
        return interp, tldr, v3, wwcm, clp_comment

    macro_txt = format_macro_summary(closes)

    sections_txt = f"""===INTERPRETACION===
{interp or ''}

===TLDR===
{tldr or ''}

===3M_VIEW===
{v3 or ''}

===WWCM===
{wwcm or ''}

===CLP===
{clp_comment or ''}"""

    prompt = f"""Eres un editor senior de market notes. Tu tarea: revisar y corregir estas secciones.
NO cambies estructura, formato ni numeros. Solo mejora el texto donde viola las reglas.

DATOS DE MERCADO (usa para verificar hechos):
{macro_txt}

{REGLAS_EDITORIALES}
{EJEMPLOS_EDITORIALES}

TAREAS ESPECIFICAS:
1. LENGUAJE GENERICO: Reescribir o eliminar frases que puedan aparecer en cualquier brief de cualquier dia.
   Ejemplos prohibidos: "lo que sugiere que los inversores estan preocupados", "en el contexto actual",
   "cierta cantidad de", "podria continuar con un sesgo", "es probable que".
   Reemplazar por dato + mecanismo concreto, o eliminar si no aporta.

2. CONTRADICCIONES CROSS-SECCION: Un activo con caida pronunciada en precios NO puede ser el "mas
   beneficiado" en 3M Implicancias. Verificar consistencia entre todas las secciones.

3. WWCM FALSACION REAL: Todo bullet de WWCM debe hacer la tesis MENOS justificada, no mas intensa.
   Si un bullet confirma el regimen en vez de falsarlo, reescribirlo.

4. CLP CONCLUSION CONCRETA: La conclusion del CLP debe tener sesgo claro con mecanismo.
   PROHIBIDO terminar con "dependera de" o "sesgo a la depreciacion en el corto plazo" sin ancla.

SECCIONES A EDITAR:
{sections_txt}

Responde EXACTAMENTE en este formato (sin texto antes ni despues de los delimitadores):
===INTERPRETACION===
[interpretacion corregida]
===TLDR===
[tldr corregido]
===3M_VIEW===
[3m view corregido]
===WWCM===
[wwcm corregido]
===CLP===
[clp corregido]
===FIN==="""

    result = _gemini_call(prompt, api_key, max_tokens=4000, model='gemini-2.5-flash')
    if not result:
        return interp, tldr, v3, wwcm, clp_comment

    def _extract(tag, text):
        m = re.search(rf'==={tag}===\s*(.*?)(?=(?:===\w|===FIN===))', text, re.DOTALL)
        return m.group(1).strip() if m else None

    return (
        _extract('INTERPRETACION', result) or interp,
        _extract('TLDR',           result) or tldr,
        _extract('3M_VIEW',        result) or v3,
        _extract('WWCM',           result) or wwcm,
        _extract('CLP',            result) or clp_comment,
    )


# ── ETAPA 2: Resumen de noticias ──────────────────────────────────────────────
def build_news_summary(news):
    if not news:
        return None
    articles_txt = '\n'.join(
        f'[{i+1}] [{a["source"]}] {a["title"]} — {a.get("summary","")[:200]}'
        for i, a in enumerate(news[:10])
    )
    prompt = f"""Eres un editor de noticias financieras. Sintetiza los articulos en 3 a 5 oraciones en espanol.

JERARQUIA OBLIGATORIA (orden de aparicion):
1. Primero: lo que movio precios de activos (equities, petroleo, tasas, FX) — con el dato concreto.
2. Segundo: catalizador geopolitico o macro que explica ese movimiento.
3. Tercero: contexto adicional relevante, solo si aporta algo nuevo.

Formato de salida OBLIGATORIO:
- Una oracion por linea.
- Cada oracion termina con el NUMERO del articulo fuente entre parentesis: (1), (3), etc.
- Si varios articulos cubren el mismo hecho, elige el mas relevante — EXACTAMENTE un numero por oracion, nunca (5, 10).
- Agrupa articulos del mismo tema en UNA sola oracion — no repitas el mismo hecho en dos lineas.
- No inventes datos. Tono directo. Sin frases genericas. Sin titulo ni introduccion.

Ejemplo correcto:
El petroleo cayo -10% tras la pausa de Trump en ataques a Iran (3).
El CEO de Chevron advirtio que el mercado no ha procesado completamente el shock de oferta (1).

ARTICULOS:
{articles_txt}"""

    return _groq_call(prompt, max_tokens=400)


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
        utc_now      = datetime.utcnow()
        # Santiago: UTC-3 en verano (oct-mar), UTC-4 en invierno (abr-sep)
        stgo_offset  = -3 if utc_now.month in (10, 11, 12, 1, 2, 3) else -4
        # New York: UTC-4 en verano (mar-nov), UTC-5 en invierno (nov-mar)
        ny_offset    = -4 if 3 <= utc_now.month <= 11 else -5
        from datetime import timedelta
        stgo_time    = (utc_now + timedelta(hours=stgo_offset)).strftime('%H:%M')
        ny_time      = (utc_now + timedelta(hours=ny_offset)).strftime('%H:%M')
        stgo_utc_lbl = f'UTC{stgo_offset:+d}'
        ny_utc_lbl   = f'UTC{ny_offset:+d}'
        date_str     = (utc_now + timedelta(hours=stgo_offset)).strftime('%d/%m/%Y')
        footer1 = f'Generado el {date_str} a las {stgo_time} (Santiago {stgo_utc_lbl}) / {ny_time} (New York {ny_utc_lbl})  |  yfinance + FRED + RSS'
        self.cell(0, 4, clean(footer1), align='C', ln=True)
        self.cell(0, 4, clean('(*) = interpretacion de Gemini en base a los datos descargados'), align='C')

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
              interp, tldr, v3, wwcm, usdclp_comment, news_summary=None):
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
        f'La interpretacion de los datos es generada automaticamente por un modelo de inteligencia artificial (Gemini 2.5 Pro) que puede cometer errores. '
        f'Este reporte es de caracter informativo y educativo. '
        f'No constituye asesoramiento financiero ni una recomendacion de inversion. '
        f'Vista Macro no se responsabiliza por decisiones tomadas en base a este contenido.'
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
    pdf.section('[N] NOTICIAS (*)')
    if news_summary:
        # Build index -> (source_name, url) map  (1-based)
        idx_map = {i+1: (a['source'], a.get('url', '')) for i, a in enumerate(news[:10])}

        pdf.set_font('Helvetica', '', 8)
        pdf.set_left_margin(8)
        for line in news_summary.split('\n'):
            line = line.strip()
            if not line:
                continue
            m = re.search(r'\((\d+)(?:[^)]*)\)[.,]?\s*$', line)
            if m:
                idx         = int(m.group(1))
                text_before = line[:m.start()].rstrip()
                src_name, url = idx_map.get(idx, ('?', ''))
                pdf.set_x(8)
                pdf.set_text_color(0, 0, 0)
                pdf.write(5.2, clean(text_before + ' ('))
                pdf.set_text_color(20, 80, 160)
                pdf.write(5.2, clean(src_name), link=url)
                pdf.set_text_color(0, 0, 0)
                pdf.write(5.2, clean(')'))
                pdf.ln(6)
            else:
                pdf.set_x(8)
                pdf.set_text_color(0, 0, 0)
                pdf.write(5.2, clean(line))
                pdf.ln(6)
        pdf.set_left_margin(8)
    else:
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
    pdf.ln(2)

    # ── Interpretacion base ───────────────────────────────────────────────────
    _LABEL_MAP = {
        'REGIMEN': 'Régimen', 'CAUSA_RAIZ': 'Lectura Central',
        'LECTURA_CENTRAL': 'Lectura Central',
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
def build_md(closes, news, tensions, interp, tldr, v3, wwcm, usdclp_comment, news_summary=None):
    now = datetime.now()
    L = []

    L += [f'# VISTA MACRO — MACRO BRIEF | {TODAY}',
          f'*Pipeline 3  |  {now.strftime("%Y-%m-%d %H:%M")}  |  Gemini 2.5 Flash*',
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
    if news_summary:
        idx_map = {i+1: (a['source'], a.get('url', '')) for i, a in enumerate(news[:10])}
        for line in news_summary.split('\n'):
            line = line.strip()
            if not line:
                continue
            def _replace_idx(m, _idx_map=idx_map):
                idx = int(m.group(1))
                src_name, url = _idx_map.get(idx, ('?', ''))
                return f'([{src_name}]({url}))' if url else f'({src_name})'
            line_md = re.sub(r'\((\d+)(?:[^)]*)\)([.,]?\s*)$', lambda m2: _replace_idx(m2) + m2.group(2), line)
            L.append(line_md)
    else:
        for a in news[:8]:
            L.append(f'**[{a["source"]}]** {a["title"]}')
            if a.get('summary'): L.append(f'> {a["summary"][:160]}')
            L.append('')
    L += ['', '---', '']

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
    print('\n[2/3] Gemini — Etapa 1: Interpretacion base...')
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

    time.sleep(15)
    # 3. Etapa 2 — Secciones
    print('\n[3/3] Gemini — Etapa 2: Secciones...')
    print('  TL;DR...')
    tldr           = build_tldr(interp, cnn, btc, closes, fred)
    time.sleep(15)
    print('  3M View...')
    v3             = build_3m_view(interp, closes, fred)
    time.sleep(15)
    print('  WWCM...')
    wwcm           = build_wwcm(interp, tensions, closes, fred)
    time.sleep(15)
    print('  USDCLP...')
    usdclp_comment = build_usdclp_comment(interp, closes)
    time.sleep(15)
    print('  Noticias...')
    news_summary   = build_news_summary(news)
    time.sleep(15)
    print('  Pase editorial...')
    interp, tldr, v3, wwcm, usdclp_comment = editorial_pass(
        interp, tldr, v3, wwcm, usdclp_comment, closes
    )
    print('  OK — 5 secciones generadas + pase editorial')

    # 4. Output
    stem     = f'{TODAY}_p3'
    md_path  = os.path.join(SUMM_DIR, f'{stem}.md')
    pdf_path = os.path.join(SUMM_DIR, f'{stem}.pdf')

    md = build_md(closes, news, tensions, interp, tldr, v3, wwcm, usdclp_comment, news_summary=news_summary)
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(md)

    try:
        pdf = build_pdf(closes, fred, cnn, btc, news, tensions,
                        interp, tldr, v3, wwcm, usdclp_comment, news_summary=news_summary)
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
