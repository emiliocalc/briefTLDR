#!/usr/bin/env python3
"""
Pipeline 2 — Análisis macro con series temporales multi-resolución vía Groq.
12 activos globales x 63 sesiones diarias (W=5, M=21, Q=63)
Mejoras v2: descripciones de ETFs, precios exactos con fechas, PDF output.
"""

import os, warnings, requests
from datetime import datetime
import pandas as pd
import yfinance as yf
from fpdf import FPDF
import feedparser

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
TICKERS = {
    '^GSPC':     'S&P 500',
    'EEM':       'EM Equities',
    '^STOXX50E': 'Europa',
    'TLT':       'Bonos 20yr US',
    'HYG':       'High Yield',
    '^VIX':      'VIX',
    'GC=F':      'Oro',
    'DX-Y.NYB':  'DXY',
    'CL=F':      'Oil WTI',
    'HG=F':      'Cobre',
    'USDJPY=X':  'USD/JPY',
    'USDCLP=X':  'USD/CLP',
}

# Descripciones detalladas para el prompt (mejora #1)
TICKER_DESC = {
    '^GSPC':     'Indice S&P 500 — renta variable EE.UU., proxy del mercado global dominante',
    'EEM':       'iShares MSCI EM ETF — renta variable mercados emergentes (China, India, Brasil, etc.)',
    '^STOXX50E': 'EURO STOXX 50 — renta variable Europa (50 blue chips eurozona)',
    'TLT':       'iShares 20yr Treasury ETF — bonos soberanos EEUU largo plazo, proxy de tasas',
    'HYG':       'iShares High Yield ETF — credito corporativo high yield, proxy de apetito de riesgo',
    '^VIX':      'CBOE VIX — volatilidad implicita S&P 500, indice de miedo del mercado',
    'GC=F':      'Oro futuros (COMEX) — safe haven, hedge inflacion y debilidad USD',
    'DX-Y.NYB':  'US Dollar Index (DXY) — fuerza del USD vs cesta de 6 monedas desarrolladas',
    'CL=F':      'WTI Crude Oil futuros — precio petroleo, proxy inflacion y demanda global',
    'HG=F':      'Cobre futuros (COMEX) — mejor proxy de crecimiento global e industrial; clave para Chile',
    'USDJPY=X':  'USD/JPY — yen japones; apreciacion del yen = desarmado de carry trade = risk-off global',
    'USDCLP=X':  'USD/CLP — tipo de cambio peso chileno; afectado por cobre, DXY y riesgo EM',
}

# Descripciones del portfolio para el prompt (mejora #1)
PORTFOLIO_DESC = {
    'VT   40%': 'Vanguard Total World — exposicion global a renta variable desarrollada + emergente',
    'AVUV 20%': 'Avantis US Small Cap Value — factor tilt small cap value EE.UU.',
    'IAU  15%': 'iShares Gold Trust — oro fisico, cobertura inflacion/riesgo sistémico',
    'IBIT 15%': 'iShares Bitcoin Trust — Bitcoin spot ETF, activo de alto riesgo/retorno',
    'AVDV 10%': 'Avantis Intl Small Cap Value — small cap value mercados desarrollados ex-EE.UU.',
}

NEWS_FEEDS = [
    ("Reuters Business", "https://feeds.reuters.com/reuters/businessNews"),
    ("Reuters Markets",  "https://feeds.reuters.com/reuters/UKmarkets"),
    ("CNBC",             "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("MarketWatch",      "https://feeds.content.dowjones.io/public/rss/mw_realtimeheadlines"),
    ("Yahoo Finance",    "https://finance.yahoo.com/news/rssindex"),
]

NEWS_KEYWORDS = [
    'fed', 'federal reserve', 'fomc', 'powell', 'warsh', 'rate', 'inflation',
    'cpi', 'pce', 'gdp', 'recession', 'employment', 'nfp',
    'iran', 'hormuz', 'oil', 'crude', 'opec',
    'bitcoin', 'btc', 'crypto', 'gold', 'treasury', 'yield',
    'vix', 's&p', 'nasdaq', 'market', 'stocks',
    'tariff', 'trade', 'china', 'dollar', 'dxy', 'equity', 'metals', 'copper',
    'emerging markets', 'yen', 'japan',
]

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── helpers ───────────────────────────────────────────────────────────────────
def _ret(a, b):
    return f"{(b / a - 1) * 100:+.1f}%"

def clean(text):
    """Fuerza latin-1 para FPDF."""
    return (str(text)
            .replace('\u2014', '-').replace('\u2013', '-').replace('\u2019', "'")
            .replace('\u201c', '"').replace('\u201d', '"').replace('\u2018', "'")
            .replace('\u2022', '*').replace('\u00b0', 'deg').replace('\u2192', '->')
            .replace('\u2191', 'up').replace('\u2193', 'down')
            .encode('latin-1', errors='replace').decode('latin-1'))

# ── news ──────────────────────────────────────────────────────────────────────
def get_news(max_items=10):
    """Descarga y filtra noticias relevantes de los RSS feeds."""
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
                articles.append({
                    'title':   title,
                    'summary': e.get('summary', '')[:220],
                    'source':  source,
                    'score':   score,
                })
        except Exception as e:
            print(f"  WARNING: feed {source} error ({e})")
            continue
    articles.sort(key=lambda x: x['score'], reverse=True)
    return articles[:max_items]

# ── data ──────────────────────────────────────────────────────────────────────
def get_series(n_days=63):
    """Descarga cierres y retorna ultimas n_days sesiones por ticker."""
    print(f"  Descargando {len(TICKERS)} tickers (6 meses)...")
    raw = yf.download(list(TICKERS.keys()), period='6mo', auto_adjust=True,
                      progress=False, threads=True)
    closes = (raw['Close'] if 'Close' in raw.columns.get_level_values(0)
              else raw.xs('Close', axis=1, level=0))
    closes.index = pd.to_datetime(closes.index)

    result = {}
    for t in TICKERS:
        if t in closes.columns:
            s = closes[t].dropna().tail(n_days)
            if len(s) >= 5:
                result[t] = s
            else:
                print(f"  WARNING: {t} — solo {len(s)} sesiones disponibles")
        else:
            print(f"  WARNING: {t} — no encontrado en yfinance")
    return result

# ── prompt ────────────────────────────────────────────────────────────────────
def build_prompt(series_dict, news=None):
    """
    Construye el prompt con:
    - Descripciones de cada activo
    - Precios exactos con fechas en puntos clave W/M/Q
    - Serie completa Q para deteccion de patrones
    - Headlines de noticias para contexto causal
    """
    # Bloque de descripciones de activos
    desc_lines = []
    for ticker, label in TICKERS.items():
        desc = TICKER_DESC.get(ticker, '')
        desc_lines.append(f"  {label} ({ticker}): {desc}")
    desc_block = "\n".join(desc_lines)

    # Bloque de descripciones del portfolio
    port_lines = []
    for pos, desc in PORTFOLIO_DESC.items():
        port_lines.append(f"  {pos}: {desc}")
    port_block = "\n".join(port_lines)

    # Bloque de datos con precios exactos + fechas (mejora #2)
    data_lines = []
    for ticker, label in TICKERS.items():
        if ticker not in series_dict:
            data_lines.append(f"{label} ({ticker}): sin datos\n")
            continue

        s    = series_dict[ticker]
        vals = [round(float(v), 2) for v in s.values]
        idx  = s.index
        n    = len(vals)

        # Precios exactos con fecha en puntos clave
        p_q_date  = idx[0].strftime('%Y-%m-%d')
        p_m_date  = idx[-21].strftime('%Y-%m-%d') if n >= 21 else idx[0].strftime('%Y-%m-%d')
        p_w_date  = idx[-5].strftime('%Y-%m-%d')  if n >= 5  else idx[0].strftime('%Y-%m-%d')
        p_now_date = idx[-1].strftime('%Y-%m-%d')

        p_q  = vals[0]
        p_m  = vals[-21] if n >= 21 else vals[0]
        p_w  = vals[-5]  if n >= 5  else vals[0]
        p_now = vals[-1]

        r_w = _ret(p_w, p_now)
        r_m = _ret(p_m, p_now)
        r_q = _ret(p_q, p_now)

        # Serie completa en una linea
        serie_str = ", ".join(str(v) for v in vals)

        data_lines.append(
            f"{label} ({ticker})\n"
            f"  Precios: Q-inicio({p_q_date})={p_q}  M-inicio({p_m_date})={p_m}"
            f"  W-inicio({p_w_date})={p_w}  Hoy({p_now_date})={p_now}\n"
            f"  Retornos: W={r_w}  M={r_m}  Q={r_q}\n"
            f"  Serie Q completa ({n}d, mas antiguo->hoy): {serie_str}\n"
        )

    data_block = "\n".join(data_lines)

    # Bloque de noticias
    if news:
        news_lines = []
        for i, a in enumerate(news, 1):
            news_lines.append(f"{i}. [{a['source']}] {a['title']}")
            if a['summary']:
                news_lines.append(f"   {a['summary'][:180]}")
        news_block = "\n".join(news_lines)
    else:
        news_block = "Sin noticias disponibles."

    prompt = f"""Eres un analista macro senior con vision global. \
Analiza las series de cierres diarios para 12 activos clave junto con los titulares de noticias recientes. \
Cada serie tiene hasta 63 sesiones (Q=trimestre). \
Los ultimos 21 valores corresponden al horizonte M (mes), los ultimos 5 al W (semana).

DESCRIPCION DE LOS ACTIVOS ANALIZADOS:
{desc_block}

DESCRIPCION DEL PORTFOLIO DEL INVERSOR:
{port_block}

CONTEXTO:
- Inversor chileno de largo plazo
- El USDCLP es relevante para sus retornos en pesos
- El cobre (HG=F) tiene correlacion directa con la economia chilena y el CLP

NOTICIAS RECIENTES (ordenadas por relevancia):
{news_block}

DATOS CON PRECIOS EXACTOS Y FECHAS:
{data_block}

INSTRUCCIONES:
- Usa UNICAMENTE los datos y noticias provistos. No inventes cifras.
- Usa las noticias para explicar el POR QUE de los movimientos de precio, no solo el que.
- Al citar una observacion, incluye el precio exacto y la fecha.
- Detecta patrones en la serie completa: tendencias, aceleraciones, reversiones, soportes rotos.
- Responde en espanol, conciso y directo.

Responde con EXACTAMENTE esta estructura (sin secciones adicionales):

REGIMEN ACTUAL:
[1-2 lineas. Risk-on / risk-off / transicion / lateral. Justifica con precios y fechas exactos. Si hay una noticia que explica el regimen, citala.]

SENALES CLAVE (top 3):
1. [Activo] [W/M/Q]: [precio exacto y fecha] — CAUSA: [noticia relevante que explica el movimiento, o "sin noticia clara"]
2. [Activo] [W/M/Q]: [precio exacto y fecha] — CAUSA: [noticia relevante que explica el movimiento, o "sin noticia clara"]
3. [Activo] [W/M/Q]: [precio exacto y fecha] — CAUSA: [noticia relevante que explica el movimiento, o "sin noticia clara"]

DIVERGENCIAS:
[Maximo 2 divergencias con precios concretos y la noticia que podria explicarlas. Si no hay ninguna, escribe exactamente: "Sin divergencias relevantes."]

USDCLP OUTLOOK:
[2-3 lineas sobre direccion esperada. Cita los niveles actuales de cobre, DXY y EM. Menciona cualquier noticia geopolitica o macro que afecte la direccion.]"""

    return prompt

# ── groq ──────────────────────────────────────────────────────────────────────
def _groq_call(prompt, max_tokens=1500):
    api_key = os.environ.get("GROQ_API_KEY", "")
    model   = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
    if not api_key:
        print("  WARNING: GROQ_API_KEY no configurado en .env")
        return None
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"},
            json={"model": model,
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": max_tokens,
                  "temperature": 0.3},
            timeout=35,
        )
        if not r.ok:
            print(f"  WARNING: Groq {r.status_code} — {r.text[:200]}")
            return None
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"  WARNING: Groq error ({e})")
        return None

# ── 3M view ───────────────────────────────────────────────────────────────────
def build_3m_view(series_dict, news=None, macro_analysis=None):
    """3M VIEW basado en series + noticias. Segunda llamada a Groq."""
    sp   = series_dict.get('^GSPC')
    vix  = series_dict.get('^VIX')
    oil  = series_dict.get('CL=F')
    gold = series_dict.get('GC=F')
    dxy  = series_dict.get('DX-Y.NYB')
    cu   = series_dict.get('HG=F')
    eem  = series_dict.get('EEM')
    tlt  = series_dict.get('TLT')
    hyg  = series_dict.get('HYG')

    def last(s):  return round(float(s.iloc[-1]), 2) if s is not None else 'N/D'
    def retq(s):  return _ret(float(s.iloc[0]),  float(s.iloc[-1])) if s is not None and len(s) > 1 else 'N/D'
    def retm(s):  return _ret(float(s.iloc[-21]), float(s.iloc[-1])) if s is not None and len(s) >= 21 else 'N/D'

    news_txt = ''
    if news:
        news_txt = '\n'.join(f'- [{a["source"]}] {a["title"]}' for a in news[:8])

    regime_txt = ''
    if macro_analysis:
        for line in macro_analysis.split('\n'):
            if line.strip():
                regime_txt = line.strip()
                break

    prompt = f"""Eres un analista macro de largo plazo. Genera un 3M VIEW (perspectiva proximos 3 meses) \
basado en los datos y noticias del dia.

DATOS CLAVE (precio actual | retorno M | retorno Q):
- S&P 500:    {last(sp)} | {retm(sp)} | {retq(sp)}
- VIX:        {last(vix)} | {retm(vix)} | {retq(vix)}
- Oil WTI:    {last(oil)} | {retm(oil)} | {retq(oil)}
- Oro:        {last(gold)} | {retm(gold)} | {retq(gold)}
- DXY:        {last(dxy)} | {retm(dxy)} | {retq(dxy)}
- Cobre:      {last(cu)} | {retm(cu)} | {retq(cu)}
- EM (EEM):   {last(eem)} | {retm(eem)} | {retq(eem)}
- TLT:        {last(tlt)} | {retm(tlt)} | {retq(tlt)}
- HYG:        {last(hyg)} | {retm(hyg)} | {retq(hyg)}

REGIMEN DETECTADO: {regime_txt}

NOTICIAS RELEVANTES:
{news_txt if news_txt else 'Sin noticias disponibles.'}

PORTFOLIO DEL INVERSOR: VT 40% | AVUV 20% | IAU 15% | IBIT 15% | AVDV 10%
Inversor chileno de largo plazo.

Genera exactamente 5 bullets en espanol, uno por linea, empezando con "- ":
1. Base case (~60%): escenario mas probable en 3 meses con cifras concretas
2. Bear case (~20%): que podria salir peor, causado por que noticia o dato
3. Bull case (~15%): sorpresa positiva y condicion necesaria para que ocurra
4. Claves a monitorear: 2-3 indicadores con umbrales concretos
5. Postura portfolio: accion concreta sugerida para VT/AVUV/IAU/IBIT/AVDV

Sin introduccion, sin titulos, solo los 5 bullets."""

    return _groq_call(prompt, max_tokens=700)

# ── PDF ───────────────────────────────────────────────────────────────────────
class PDF(FPDF):
    def header(self):
        self.set_fill_color(20, 20, 20)
        self.rect(0, 0, 210, 14, 'F')
        self.set_text_color(255, 255, 255)
        self.set_font('Helvetica', 'B', 11)
        self.set_xy(8, 3)
        self.cell(130, 8, 'Macro Brief  |  Pipeline 2  |  Series Temporales')
        self.set_font('Helvetica', '', 8)
        self.set_xy(0, 3)
        self.cell(200, 8, datetime.now().strftime('%Y-%m-%d %H:%M'), align='R')
        self.set_text_color(0, 0, 0)
        self.ln(14)

    def footer(self):
        self.set_y(-10)
        self.set_font('Helvetica', 'I', 6.5)
        self.set_text_color(120, 120, 120)
        self.cell(0, 5,
            clean(f'Generado {datetime.now().strftime("%Y-%m-%d %H:%M")} | '
                  f'yfinance daily closes | llama-3.3-70b-versatile | '
                  f'W=5 M=21 Q=63 sesiones | Pag {self.page_no()}'),
            align='C')
        self.set_text_color(0, 0, 0)

    def section(self, title, color=(40, 80, 160)):
        self.set_fill_color(*color)
        self.set_text_color(255, 255, 255)
        self.set_font('Helvetica', 'B', 8.5)
        self.cell(0, 6, clean(f'  {title}'), ln=True, fill=True)
        self.set_text_color(0, 0, 0)
        self.ln(1)

    def body(self, text, size=8):
        self.set_font('Helvetica', '', size)
        self.set_x(8)
        self.multi_cell(0, 5, clean(text))

    def kv(self, k, v, bold_v=False):
        self.set_font('Helvetica', '', 8)
        self.set_x(8)
        self.cell(48, 5.2, clean(k))
        self.set_font('Helvetica', 'B' if bold_v else '', 8)
        self.cell(0, 5.2, clean(str(v)), ln=True)


def _color_ret(val_str):
    """Retorna color RGB segun si el retorno es positivo, negativo o neutro."""
    s = val_str.strip()
    if s.startswith('+'):
        return (0, 130, 60)
    elif s.startswith('-'):
        return (180, 30, 30)
    return (60, 60, 60)


def build_pdf(analysis, series_dict, news=None, v3=None):
    pdf = PDF()
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()

    # ── Tabla resumen de activos ──────────────────────────────────────────────
    pdf.section('RESUMEN DE ACTIVOS  (W=1 semana  M=1 mes  Q=1 trimestre)')

    # Cabecera tabla
    pdf.set_font('Helvetica', 'B', 7.5)
    pdf.set_fill_color(230, 235, 245)
    pdf.set_x(8)
    pdf.cell(50, 5.5, 'Activo',       fill=True)
    pdf.cell(28, 5.5, 'Precio',       fill=True, align='R')
    pdf.cell(28, 5.5, 'W  (5d)',      fill=True, align='R')
    pdf.cell(28, 5.5, 'M  (21d)',     fill=True, align='R')
    pdf.cell(28, 5.5, 'Q  (63d)',     fill=True, align='R')
    pdf.cell(0,  5.5, 'Fecha actual', fill=True, align='R')
    pdf.ln(5.5)

    alt = False
    for ticker, label in TICKERS.items():
        if ticker not in series_dict:
            continue
        s     = series_dict[ticker]
        vals  = [float(v) for v in s.values]
        n     = len(vals)
        price = vals[-1]
        date  = s.index[-1].strftime('%Y-%m-%d')
        r_w   = _ret(vals[-5],  vals[-1]) if n >= 5  else '--'
        r_m   = _ret(vals[-21], vals[-1]) if n >= 21 else '--'
        r_q   = _ret(vals[0],   vals[-1])

        if alt:
            pdf.set_fill_color(248, 249, 252)
        else:
            pdf.set_fill_color(255, 255, 255)
        alt = not alt

        pdf.set_font('Helvetica', '', 7.5)
        pdf.set_x(8)
        pdf.cell(50, 5.2, clean(label), fill=True)
        pdf.set_font('Helvetica', 'B', 7.5)
        pdf.cell(28, 5.2, f'{price:,.2f}', fill=True, align='R')

        for r in [r_w, r_m, r_q]:
            cr = _color_ret(r)
            pdf.set_text_color(*cr)
            pdf.set_font('Helvetica', 'B', 7.5)
            pdf.cell(28, 5.2, clean(r), fill=True, align='R')
            pdf.set_text_color(0, 0, 0)

        pdf.set_font('Helvetica', '', 7)
        pdf.set_text_color(100, 100, 100)
        pdf.cell(0, 5.2, date, fill=True, align='R')
        pdf.set_text_color(0, 0, 0)
        pdf.ln(5.2)

    pdf.ln(4)

    # ── Noticias relevantes ───────────────────────────────────────────────────
    if news:
        pdf.section('NOTICIAS RELEVANTES', color=(80, 80, 80))
        for i, a in enumerate(news, 1):
            pdf.set_font('Helvetica', 'B', 7.5)
            pdf.set_x(8)
            pdf.cell(12, 5, clean(f'{i}.'), )
            pdf.set_text_color(60, 90, 140)
            pdf.cell(30, 5, clean(f'[{a["source"]}]'))
            pdf.set_text_color(0, 0, 0)
            pdf.set_font('Helvetica', '', 7.5)
            pdf.multi_cell(0, 5, clean(a['title']))
            if a.get('summary'):
                pdf.set_font('Helvetica', 'I', 7)
                pdf.set_text_color(90, 90, 90)
                pdf.set_x(20)
                pdf.multi_cell(0, 4.5, clean(a['summary'][:180]))
                pdf.set_text_color(0, 0, 0)
            pdf.ln(1)
        pdf.ln(3)

    # ── Análisis Groq ─────────────────────────────────────────────────────────
    if not analysis:
        pdf.section('ANALISIS GROQ — no disponible', color=(150, 50, 50))
        pdf.body('Verificar GROQ_API_KEY en .env')
        return pdf

    # Parsear las secciones del análisis
    sections = {
        'REGIMEN ACTUAL':  ('REGIMEN ACTUAL',       (40,  80, 160)),
        'SENALES CLAVE':   ('SENALES CLAVE (top 3)', (60, 100,  50)),
        'DIVERGENCIAS':    ('DIVERGENCIAS',           (130, 70,  20)),
        'USDCLP OUTLOOK':  ('USDCLP OUTLOOK',        (20, 100, 130)),
    }

    # Dividir el texto en bloques por sección
    raw_text = analysis
    parsed   = {}
    order    = list(sections.keys())

    for i, key in enumerate(order):
        # Buscar el inicio del bloque
        start_idx = raw_text.find(key)
        if start_idx == -1:
            # Intentar variantes
            alt_key = key.replace('SENALES CLAVE', 'SE').split()[0]
            for line in raw_text.split('\n'):
                if key.split()[0] in line.upper():
                    start_idx = raw_text.find(line)
                    break
        if start_idx == -1:
            parsed[key] = ''
            continue

        # Encontrar el fin del bloque (inicio del siguiente)
        end_idx = len(raw_text)
        for next_key in order[i+1:]:
            ni = raw_text.find(next_key, start_idx + 1)
            if ni != -1:
                end_idx = ni
                break

        block = raw_text[start_idx:end_idx].strip()
        # Remover el encabezado de la sección del bloque
        first_newline = block.find('\n')
        if first_newline != -1:
            block = block[first_newline:].strip()
        parsed[key] = block

    # Renderizar cada sección
    for key, (title, color) in sections.items():
        content = parsed.get(key, '')
        pdf.section(title, color=color)
        if content:
            for line in content.split('\n'):
                line = line.strip()
                if not line:
                    pdf.ln(1)
                    continue
                pdf.set_font('Helvetica', '', 8)
                pdf.set_x(8)
                pdf.multi_cell(0, 5.2, clean(line))
        else:
            pdf.body('[sin datos]')
        pdf.ln(3)

    # ── 3M View ───────────────────────────────────────────────────────────────
    if v3:
        pdf.ln(2)
        pdf.section('[3M] 3M VIEW — BASED ON CURRENT BRIEF', color=(60, 30, 100))
        for line in v3.split('\n'):
            line = line.strip().lstrip('-* ')
            if not line:
                pdf.ln(1)
                continue
            pdf.set_font('Helvetica', '', 8)
            pdf.set_x(8)
            pdf.cell(5, 5.2, clean('-'))
            pdf.multi_cell(0, 5.2, clean(line))
        pdf.ln(3)

    # ── Descripción de activos del portfolio ──────────────────────────────────
    pdf.add_page()
    pdf.section('COMPOSICION DEL PORTFOLIO', color=(60, 60, 60))
    for pos, desc in PORTFOLIO_DESC.items():
        pdf.set_font('Helvetica', 'B', 8)
        pdf.set_x(8)
        pdf.cell(28, 5.2, clean(pos))
        pdf.set_font('Helvetica', '', 8)
        pdf.multi_cell(0, 5.2, clean(desc))

    pdf.ln(4)
    pdf.section('ACTIVOS MACRO ANALIZADOS', color=(60, 60, 60))
    for ticker, label in TICKERS.items():
        desc = TICKER_DESC.get(ticker, '')
        pdf.set_font('Helvetica', 'B', 8)
        pdf.set_x(8)
        pdf.cell(36, 5.2, clean(f'{label} ({ticker})'))
        pdf.set_font('Helvetica', '', 7.5)
        pdf.multi_cell(0, 5.2, clean(desc))

    return pdf

# ── MD ────────────────────────────────────────────────────────────────────────
def build_md(analysis, series_dict):
    now   = datetime.now()
    lines = []
    w     = lines.append

    w('# MACRO BRIEF — PIPELINE 2')
    w(f'*{now.strftime("%Y-%m-%d %H:%M")} | llama-3.3-70b-versatile | W=5 M=21 Q=63 sesiones*')
    w('')
    w('## Resumen de activos')
    w('')
    w('| Activo | Precio | Fecha | W | M | Q |')
    w('|---|---:|---|---:|---:|---:|')
    for ticker, label in TICKERS.items():
        if ticker not in series_dict:
            w(f'| {label} | — | — | — | — | — |')
            continue
        s     = series_dict[ticker]
        vals  = [float(v) for v in s.values]
        n     = len(vals)
        price = vals[-1]
        date  = s.index[-1].strftime('%Y-%m-%d')
        r_w   = _ret(vals[-5],  vals[-1]) if n >= 5  else '--'
        r_m   = _ret(vals[-21], vals[-1]) if n >= 21 else '--'
        r_q   = _ret(vals[0],   vals[-1])
        w(f'| {label} | {price:,.2f} | {date} | {r_w} | {r_m} | {r_q} |')

    w('')
    w('---')
    w('')
    w('## Analisis macro — Groq')
    w('')
    if analysis:
        w(analysis)
    else:
        w('*Analisis no disponible — verificar GROQ_API_KEY en .env*')

    w('')
    w('---')
    w('*Pipeline 2 — yfinance daily closes, auto_adjust=True*')

    return '\n'.join(lines)

# ── run ───────────────────────────────────────────────────────────────────────
def run():
    print('=' * 55)
    print('  PIPELINE 2 v2 — Macro Series Analysis')
    print('=' * 55)

    print('\n[1/4] Descargando series temporales...')
    series = get_series(n_days=63)
    print(f'  OK: {len(series)}/{len(TICKERS)} tickers con datos')

    print('\n[2/4] Descargando noticias RSS...')
    news = get_news(max_items=10)
    print(f'  OK: {len(news)} articulos relevantes')
    for a in news:
        print(f'       [{a["source"]}] {a["title"][:80]}')

    print('\n[3/4] Enviando a Groq...')
    prompt   = build_prompt(series, news=news)
    analysis = _groq_call(prompt, max_tokens=1500)
    if analysis:
        print('  OK: analisis macro recibido')
        print('\n' + '-' * 55)
        print(analysis)
        print('-' * 55)
    else:
        print('  WARN: sin respuesta de Groq (analisis macro)')

    print('\n     Generando 3M view...')
    v3 = build_3m_view(series, news=news, macro_analysis=analysis)
    if v3:
        print('  OK: 3M view recibido')
        print('\n' + '-' * 55)
        print(v3)
        print('-' * 55)
    else:
        print('  WARN: sin respuesta de Groq (3M view)')

    print('\n[4/4] Generando outputs...')
    stem = f"brief2_{datetime.now().strftime('%Y%m%d_%H%M')}"

    # MD
    md      = build_md(analysis, series)
    md_path = os.path.join(OUTPUT_DIR, f'{stem}.md')
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(md)
    print(f'  MD:  output/{stem}.md')

    # PDF
    try:
        pdf     = build_pdf(analysis, series, news=news, v3=v3)
        pdf_path = os.path.join(OUTPUT_DIR, f'{stem}.pdf')
        pdf.output(pdf_path)
        print(f'  PDF: output/{stem}.pdf')
    except Exception as e:
        print(f'  WARNING: PDF error — {e}')

    print('\nDONE')

if __name__ == '__main__':
    run()
