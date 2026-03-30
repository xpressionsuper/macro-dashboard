"""
매크로 스트레스 대시보드 v2 - 개별 차트 9장 전송판
"""

import os
import requests
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN",   "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
FRED_API_KEY     = os.environ.get("FRED_API_KEY",     "")

PERIOD     = "1y"
INTERVAL   = "1wk"
CHART_DIR  = "/tmp/macro_charts"

C = dict(
    bg="#0d1117", panel="#161b22", border="#30363d",
    gold="#d4af37", silver="#c0c0c0", red="#f85149",
    green="#3fb950", blue="#58a6ff", orange="#e3b341",
    purple="#bc8cff", cyan="#39d0d8", pink="#ff7b9c",
    text="#e6edf3", subtext="#8b949e",
)

TICKERS = {
    "DXY":   "DX-Y.NYB",
    "10Y":   "^TNX",
    "2Y":    "^IRX",
    "Gold":  "GLD",
    "Silver":"SLV",
    "WTI":   "CL=F",
    "Brent": "BZ=F",
    "Copper":"HG=F",
    "VIX":   "^VIX",
    "OVX":   "^OVX",
    "GVZ":   "^GVZ",
    "EMB":   "EMB",
    "HYG":   "HYG",
    "LQD":   "LQD",
    "TLT":   "TLT",
    "KRW":   "KRW=X",
    "TRY":   "TRY=X",
    "ZAR":   "ZAR=X",
    "BRL":   "BRL=X",
    "INR":   "INR=X",
    "ILS":   "ILS=X",
    "KSA":   "KSA",
    "BDRY":  "BDRY",
    "SPY":   "SPY",
    "EEM":   "EEM",
}

FRED_SERIES = {
    "RRP":       "RRPONTSYD",
    "TED":       "TEDRATE",
    "IG_SPREAD": "BAMLC0A0CM",
    "HY_SPREAD": "BAMLH0A0HYM2",
    "BEI_5Y":    "T5YIE",
    "BEI_10Y":   "T10YIE",
    "ICSA":      "ICSA",
}

# ─────────────────────────────────────────────────────
# 1. 데이터 수집
# ─────────────────────────────────────────────────────

def fetch_market(tickers):
    print("Market data collecting...")
    data = {}
    for label, ticker in tickers.items():
        try:
            df = yf.download(ticker, period=PERIOD, interval=INTERVAL,
                             progress=False, auto_adjust=True)
            if df.empty:
                print(f"  [SKIP] {label}")
                continue
            s = df["Close"]
            if isinstance(s, pd.DataFrame):
                s = s.iloc[:, 0]
            s = s.squeeze()
            if not isinstance(s, pd.Series):
                s = pd.Series([float(s)], name=label)
            data[label] = s.dropna()
            print(f"  [OK] {label}")
        except Exception as e:
            print(f"  [ERR] {label} - {e}")
    return data


def fetch_fred(series_id, label):
    if not FRED_API_KEY:
        return pd.Series(dtype=float, name=label)
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_API_KEY}"
           f"&file_type=json&observation_start="
           f"{(datetime.today()-timedelta(days=400)).strftime('%Y-%m-%d')}")
    try:
        obs = requests.get(url, timeout=10).json().get("observations", [])
        s = pd.Series(
            {o["date"]: float(o["value"]) for o in obs if o["value"] != "."},
            name=label)
        s.index = pd.to_datetime(s.index)
        print(f"  [OK] FRED:{label}")
        return s.dropna()
    except Exception as e:
        print(f"  [ERR] FRED:{label} - {e}")
        return pd.Series(dtype=float, name=label)


def fetch_all_fred():
    print("FRED data collecting...")
    return {k: fetch_fred(v, k) for k, v in FRED_SERIES.items()}


# ─────────────────────────────────────────────────────
# 2. 파생 지표
# ─────────────────────────────────────────────────────

def safe_s(mkt, key):
    s = mkt.get(key, pd.Series(dtype=float))
    return s.dropna() if isinstance(s, pd.Series) else pd.Series(dtype=float)


def derive(mkt, fred):
    d = {}
    try:
        if "10Y" in mkt and "2Y" in mkt:
            d["TERM_SPREAD"] = safe_s(mkt,"10Y") - safe_s(mkt,"2Y")
        if "Gold" in mkt and "Silver" in mkt:
            d["GOLD_SILVER"] = safe_s(mkt,"Gold") / safe_s(mkt,"Silver")
        if "Copper" in mkt and "Gold" in mkt:
            d["COPPER_GOLD"] = safe_s(mkt,"Copper") / safe_s(mkt,"Gold")
        if "Copper" in mkt and "WTI" in mkt:
            d["COPPER_WTI"]  = safe_s(mkt,"Copper") / safe_s(mkt,"WTI")
        if "Brent" in mkt and "WTI" in mkt:
            d["BRENT_WTI"]   = safe_s(mkt,"Brent") - safe_s(mkt,"WTI")
        bei = fred.get("BEI_10Y", pd.Series(dtype=float))
        if "10Y" in mkt and not bei.empty:
            t10 = safe_s(mkt, "10Y")
            d["REAL_RATE"] = t10 - bei.reindex(t10.index, method="ffill")
    except Exception as e:
        print(f"  [WARN] derive: {e}")
    return d


# ─────────────────────────────────────────────────────
# 3. 시그널
# ─────────────────────────────────────────────────────

def latest(s):
    if not isinstance(s, pd.Series): return None
    s = s.dropna()
    return float(s.iloc[-1]) if len(s) else None

def pct_chg(s, periods=4):
    if not isinstance(s, pd.Series): return None
    s = s.dropna()
    if len(s) < periods + 1: return None
    return (s.iloc[-1] / s.iloc[-periods-1] - 1) * 100

def sig(val, thr, direction="above"):
    if val is None: return "⚪"
    return "🔴" if (val > thr if direction == "above" else val < thr) else "🟢"

def fmt(v, unit="", decimals=1):
    return f"{v:.{decimals}f}{unit}" if v is not None else "N/A"

def build_signals(mkt, fred, derived):
    m  = lambda k: mkt.get(k, pd.Series(dtype=float))
    f  = lambda k: fred.get(k, pd.Series(dtype=float))
    dv = lambda k: derived.get(k, pd.Series(dtype=float))

    dxy=latest(m("DXY")); vix=latest(m("VIX")); ovx=latest(m("OVX"))
    gvz=latest(m("GVZ"))
    krw=latest(m("KRW")); try_r=latest(m("TRY")); zar=latest(m("ZAR"))
    brl=latest(m("BRL")); inr=latest(m("INR")); ils=latest(m("ILS"))
    emb_chg=pct_chg(m("EMB"),4); hyg=latest(m("HYG")); wti=latest(m("WTI"))
    ksa_chg=pct_chg(m("KSA"),4); bdry_chg=pct_chg(m("BDRY"),4)
    term_sp=latest(dv("TERM_SPREAD")); gs_ratio=latest(dv("GOLD_SILVER"))
    cu_gold=latest(dv("COPPER_GOLD")); cu_wti=latest(dv("COPPER_WTI"))
    brent_wti=latest(dv("BRENT_WTI")); real_rate=latest(dv("REAL_RATE"))
    rrp_chg=pct_chg(f("RRP"),8); ted=latest(f("TED"))
    ig_sp=latest(f("IG_SPREAD")); hy_sp=latest(f("HY_SPREAD"))
    bei_5y=latest(f("BEI_5Y")); bei_10y=latest(f("BEI_10Y")); icsa=latest(f("ICSA"))

    return {
        "🔴 Liquidity & Dollar": [
            ("DXY Dollar Index",    dxy,      fmt(dxy),            sig(dxy,104)),
            ("TED Spread (bp)",     ted,      fmt(ted,"",2),       sig(ted,0.5)),
            ("Fed RRP 8wk chg",     rrp_chg,  fmt(rrp_chg,"%"),    sig(rrp_chg,-30,"below")),
        ],
        "🟠 Credit & Bond Stress": [
            ("Term Spread (10-2Y)", term_sp,  fmt(term_sp,"%p",2), sig(term_sp,0,"below")),
            ("IG Credit Spread",    ig_sp,    fmt(ig_sp,"%",2),    sig(ig_sp,1.5)),
            ("HY Credit Spread",    hy_sp,    fmt(hy_sp,"%",2),    sig(hy_sp,4.5)),
            ("EMB 4wk Return",      emb_chg,  fmt(emb_chg,"%"),    sig(emb_chg,-3,"below")),
            ("HYG Hi-Yield ETF",    hyg,      fmt(hyg),            sig(hyg,75,"below")),
        ],
        "🟡 Volatility & Sentiment": [
            ("VIX (S&P500 Vol)",    vix,      fmt(vix),            sig(vix,25)),
            ("OVX (Oil Vol)",       ovx,      fmt(ovx),            sig(ovx,40)),
            ("GVZ (Gold Vol)",      gvz,      fmt(gvz),            sig(gvz,20)),
        ],
        "🟢 Real Economy Leading": [
            ("Copper/Gold Ratio",   cu_gold,  fmt(cu_gold,"",4),   sig(cu_gold,0.0018,"below")),
            ("Copper/WTI Ratio",    cu_wti,   fmt(cu_wti,"",3),    sig(cu_wti,0.05,"below")),
            ("BDRY Shipping 4wk",   bdry_chg, fmt(bdry_chg,"%"),   sig(bdry_chg,-10,"below")),
            ("Initial Jobless",     icsa,     f"{icsa/1000:.0f}K" if icsa else "N/A", sig(icsa,250000)),
        ],
        "🔵 Inflation & Real Rate": [
            ("5Y Breakeven Infl",   bei_5y,   fmt(bei_5y,"%",2),   sig(bei_5y,3.0)),
            ("10Y Breakeven Infl",  bei_10y,  fmt(bei_10y,"%",2),  sig(bei_10y,2.8)),
            ("Real Rate (10Y-BEI)", real_rate,fmt(real_rate,"%",2),sig(real_rate,0,"below")),
            ("Gold/Silver Ratio",   gs_ratio, fmt(gs_ratio),       sig(gs_ratio,80)),
        ],
        "⚫ Geopolitical (Hormuz)": [
            ("Brent-WTI Spread",    brent_wti,f"${brent_wti:.2f}" if brent_wti else "N/A", sig(brent_wti,5)),
            ("WTI Crude",           wti,      f"${wti:.1f}" if wti else "N/A", sig(wti,90)),
            ("USD/ILS (Israel)",    ils,      fmt(ils,"",3),       sig(ils,3.8)),
            ("KSA Saudi ETF 4wk",   ksa_chg,  fmt(ksa_chg,"%"),    sig(ksa_chg,-5,"below")),
        ],
        "🌏 EM Currencies": [
            ("USD/KRW (Korea)",     krw,      fmt(krw,"",0),       sig(krw,1380)),
            ("USD/TRY (Turkey)",    try_r,    fmt(try_r),          sig(try_r,35)),
            ("USD/ZAR (S.Africa)",  zar,      fmt(zar),            sig(zar,19)),
            ("USD/BRL (Brazil)",    brl,      fmt(brl,"",2),       sig(brl,5.5)),
            ("USD/INR (India)",     inr,      fmt(inr),            sig(inr,85)),
        ],
    }


def count_alerts(signals):
    danger, total = 0, 0
    for items in signals.values():
        for row in items:
            total += 1
            if row[3] == "🔴": danger += 1
    return danger, total


def overall_status(danger, total):
    r = danger / total if total else 0
    if r >= 0.6:    return "🚨 CRITICAL"
    elif r >= 0.4:  return "🔴 DANGER"
    elif r >= 0.25: return "⚠️ WARNING"
    elif r >= 0.1:  return "🟡 WATCH"
    else:           return "✅ STABLE"


# ─────────────────────────────────────────────────────
# 4. 개별 차트 생성 헬퍼
# ─────────────────────────────────────────────────────

def base_fig(title, desc):
    """공통 레이아웃 기반 figure 생성"""
    fig = go.Figure()
    fig.update_layout(
        paper_bgcolor=C["bg"],
        plot_bgcolor=C["panel"],
        font=dict(family="Arial, sans-serif", color=C["text"], size=11),
        height=420, width=820,
        margin=dict(l=60, r=200, t=70, b=50),  # 우측 여백을 넓혀 레전드 배치
        title=dict(
            text=f"<b>{title}</b><br><sup style='color:{C['subtext']}'>{desc}</sup>",
            font=dict(size=13, color=C["text"]),
            x=0.02, xanchor="left",
        ),
        legend=dict(
            bgcolor="rgba(22,27,34,0.9)",
            bordercolor=C["border"],
            borderwidth=1,
            font=dict(size=10),
            x=1.02,           # 차트 오른쪽 바깥
            y=1.0,
            xanchor="left",
            yanchor="top",
            orientation="v",  # 세로 나열
        ),
        xaxis=dict(gridcolor=C["border"], zeroline=False,
                   tickfont=dict(size=9)),
        yaxis=dict(gridcolor=C["border"], zeroline=False,
                   tickfont=dict(size=9)),
    )
    return fig


def add_line(fig, s, name, color, dash="solid"):
    if not isinstance(s, pd.Series): return
    s = s.dropna()
    if s.empty: return
    fig.add_trace(go.Scatter(
        x=s.index, y=s.values, name=name,
        line=dict(color=color, width=2, dash=dash),
        hovertemplate=f"<b>{name}</b><br>%{{x|%Y-%m-%d}}<br>%{{y:.3f}}<extra></extra>",
    ))


def add_bar(fig, s, name):
    if not isinstance(s, pd.Series): return
    s = s.dropna()
    if s.empty: return
    colors = [C["green"] if v >= 0 else C["red"] for v in s.values]
    fig.add_trace(go.Bar(
        x=s.index, y=s.values, name=name,
        marker_color=colors,
        hovertemplate="<b>Spread</b><br>%{x|%Y-%m-%d}<br>%{y:.2f}%p<extra></extra>",
    ))


def hline(fig, y, color, label=""):
    fig.add_hline(
        y=y, line_dash="dash", line_color=color, line_width=1,
        annotation_text=label,
        annotation_font_size=9,
        annotation_font_color=color,
        annotation_position="top right",
    )


def normalize(s):
    if not isinstance(s, pd.Series): return pd.Series(dtype=float)
    s = s.dropna()
    return (s / s.iloc[0] * 100) if len(s) else s


def save(fig, filename):
    path = f"{CHART_DIR}/{filename}"
    fig.write_image(path, scale=2)
    return path


# ─────────────────────────────────────────────────────
# 5. 차트 9장 개별 생성
# ─────────────────────────────────────────────────────

def make_all_charts(mkt, derived, fred):
    os.makedirs(CHART_DIR, exist_ok=True)
    m  = lambda k: mkt.get(k, pd.Series(dtype=float))
    dv = lambda k: derived.get(k, pd.Series(dtype=float))
    f  = lambda k: fred.get(k, pd.Series(dtype=float))
    paths = []

    # ① DXY & VIX
    fig = base_fig("1. DXY & VIX",
                   "DXY above 104 = dollar stress  |  VIX above 25 = fear  |  VIX above 30 = panic")
    add_line(fig, m("DXY"), "DXY", C["blue"])
    add_line(fig, m("VIX"), "VIX", C["red"], "dot")
    hline(fig, 104, C["orange"], "DXY 104")
    hline(fig, 25,  C["red"],    "VIX 25")
    hline(fig, 30,  C["red"],    "VIX 30")
    paths.append(save(fig, "01_dxy_vix.png"))

    # ② Gold vs Silver
    fig = base_fig("2. Gold vs Silver (normalized to 100)",
                   "G/S Ratio above 80 = silver undervalued  |  divergence = stress signal")
    add_line(fig, normalize(m("Gold")),   "Gold (norm)",    C["gold"])
    add_line(fig, normalize(m("Silver")), "Silver (norm)",  C["silver"], "dot")
    add_line(fig, dv("GOLD_SILVER"),      "Gold/Silver Ratio", C["orange"], "dash")
    hline(fig, 80, C["red"], "Ratio 80")
    paths.append(save(fig, "02_gold_silver.png"))

    # ③ Credit Spreads
    fig = base_fig("3. Credit Spreads",
                   "IG above 1.5% = investment grade stress  |  HY above 4.5% = high yield stress")
    add_line(fig, f("IG_SPREAD"), "IG Spread (%)", C["blue"])
    add_line(fig, f("HY_SPREAD"), "HY Spread (%)", C["red"], "dot")
    hline(fig, 1.5, C["orange"], "IG 1.5%")
    hline(fig, 4.5, C["red"],    "HY 4.5%")
    paths.append(save(fig, "03_credit_spreads.png"))

    # ④ EM Currencies
    fig = base_fig("4. EM Currencies vs USD (normalized to 100)",
                   "Rising = EM currency weakening  |  3+ simultaneous = Fed pivot signal")
    for key, lbl, clr in [
        ("KRW","KRW (Korea)",   C["blue"]),
        ("TRY","TRY (Turkey)",  C["red"]),
        ("ZAR","ZAR (S.Africa)",C["orange"]),
        ("BRL","BRL (Brazil)",  C["green"]),
        ("INR","INR (India)",   C["purple"]),
    ]:
        add_line(fig, normalize(m(key)), lbl, clr)
    paths.append(save(fig, "04_em_currencies.png"))

    # ⑤ Copper Ratios
    fig = base_fig("5. Copper/Gold & Copper/WTI Ratio",
                   "Cu/Gold falling = recession leading  |  Cu/WTI falling = stagflation signal")
    add_line(fig, dv("COPPER_GOLD"), "Copper/Gold", C["cyan"])
    add_line(fig, dv("COPPER_WTI"),  "Copper/WTI",  C["pink"], "dot")
    paths.append(save(fig, "05_copper_ratios.png"))

    # ⑥ Oil & OVX — 이중 Y축 (가격 vs 변동성 스케일 분리)
    fig = go.Figure()
    fig.update_layout(
        paper_bgcolor=C["bg"],
        plot_bgcolor=C["panel"],
        font=dict(family="Arial, sans-serif", color=C["text"], size=11),
        height=420, width=820,
        margin=dict(l=60, r=200, t=70, b=50),
        title=dict(
            text="<b>6. Oil Price (WTI / Brent) & OVX Volatility</b>",
            font=dict(size=13, color=C["text"]),
            x=0.02, xanchor="left",
        ),
        legend=dict(
            bgcolor="rgba(22,27,34,0.9)",
            bordercolor=C["border"],
            borderwidth=1,
            font=dict(size=10),
            x=1.02, y=1.0,
            xanchor="left", yanchor="top",
            orientation="v",
        ),
        # 왼쪽 Y축 — 가격 ($)
        yaxis=dict(
            title=dict(text="Price ($)", font=dict(color=C["orange"], size=10)),
            gridcolor=C["border"], zeroline=False,
            tickfont=dict(size=9, color=C["orange"]),
        ),
        # 오른쪽 Y축 — OVX 변동성
        yaxis2=dict(
            title=dict(text="OVX (Volatility)", font=dict(color=C["red"], size=10)),
            overlaying="y",
            side="right",
            gridcolor="rgba(0,0,0,0)",
            zeroline=False,
            tickfont=dict(size=9, color=C["red"]),
        ),
        xaxis=dict(gridcolor=C["border"], zeroline=False, tickfont=dict(size=9)),
    )

    # WTI & Brent → 왼쪽 축
    for s_key, name, color, dash in [
        ("WTI",   "WTI ($)",   C["orange"], "solid"),
        ("Brent", "Brent ($)", C["gold"],   "dot"),
    ]:
        s = m(s_key).dropna()
        if not s.empty:
            fig.add_trace(go.Scatter(
                x=s.index, y=s.values, name=name,
                line=dict(color=color, width=2, dash=dash),
                yaxis="y",
                hovertemplate=f"<b>{name}</b><br>%{{x|%Y-%m-%d}}<br>${{y:.1f}}<extra></extra>",
            ))

    # OVX → 오른쪽 축
    ovx_s = m("OVX").dropna()
    if not ovx_s.empty:
        fig.add_trace(go.Scatter(
            x=ovx_s.index, y=ovx_s.values, name="OVX (vol)",
            line=dict(color=C["red"], width=2, dash="dash"),
            yaxis="y2",
            hovertemplate="<b>OVX</b><br>%{x|%Y-%m-%d}<br>%{y:.1f}<extra></extra>",
        ))

    # 임계선
    fig.add_hline(y=90, line_dash="dash", line_color=C["red"], line_width=1,
                  annotation_text="WTI $90", annotation_font_size=9,
                  annotation_font_color=C["red"], annotation_position="top left",
                  yref="y")

    # subtitle annotation (제목 아래 설명)
    fig.add_annotation(
        text="WTI above $90 = inflation risk  |  OVX above 40 = panic  |  Brent-WTI above $5 = supply disruption",
        xref="paper", yref="paper",
        x=0.0, y=1.055,
        showarrow=False,
        font=dict(size=9, color=C["subtext"], family="Arial, sans-serif"),
        xanchor="left", yanchor="bottom",
    )
    paths.append(save(fig, "06_oil_ovx.png"))

    # ⑦ Inflation & Real Rate
    fig = base_fig("7. Breakeven Inflation & Real Rate",
                   "BEI above 3% = elevated inflation expectations  |  Real Rate below 0 = gold bullish")
    add_line(fig, f("BEI_5Y"),     "5Y BEI (%)",    C["orange"])
    add_line(fig, f("BEI_10Y"),    "10Y BEI (%)",   C["gold"], "dot")
    add_line(fig, dv("REAL_RATE"), "Real Rate (%)", C["red"],  "dash")
    hline(fig, 0,   C["subtext"], "")
    hline(fig, 3.0, C["orange"],  "BEI 3%")
    paths.append(save(fig, "07_inflation_realrate.png"))

    # ⑧ Yield Curve
    fig = base_fig("8. Yield Curve Spread (10Y - 2Y)",
                   "Below 0 = inverted curve = recession leading indicator (avg 12-18mo lead)")
    add_bar(fig, dv("TERM_SPREAD"), "10Y-2Y Spread")
    hline(fig, 0, C["subtext"], "Inversion Line")
    paths.append(save(fig, "08_yield_curve.png"))

    # ⑨ Geopolitical
    fig = base_fig("9. Geopolitical Monitor (normalized to 100)",
                   "ILS/KSA falling = Middle East stress  |  BDRY falling = global trade slowdown")
    add_line(fig, normalize(m("ILS")),  "ILS (Israel Shekel)", C["blue"])
    add_line(fig, normalize(m("KSA")),  "KSA (Saudi ETF)",     C["gold"], "dot")
    add_line(fig, normalize(m("BDRY")), "BDRY (Shipping ETF)", C["cyan"], "dash")
    paths.append(save(fig, "09_geopolitical.png"))

    print(f"  [OK] {len(paths)} charts saved")
    return paths


# ─────────────────────────────────────────────────────
# 6. 텔레그램
# ─────────────────────────────────────────────────────

def tg_text(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
        }, timeout=15)
        print(f"  Text: {r.status_code}")
    except Exception as e:
        print(f"  [ERR] text: {e}")


def tg_photo(path, caption=""):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        with open(path, "rb") as f:
            r = requests.post(url,
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
                files={"photo": f},
                timeout=30,
            )
        print(f"  Photo [{os.path.basename(path)}]: {r.status_code}")
        return r.status_code == 200
    except Exception as e:
        print(f"  [ERR] photo {path}: {e}")
        return False


def tg_media_group(paths, captions):
    """여러 장을 하나의 앨범으로 묶어 전송 (최대 10장)"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMediaGroup"
        files = {}
        media = []
        for i, (path, cap) in enumerate(zip(paths, captions)):
            key = f"photo{i}"
            files[key] = open(path, "rb")
            item = {"type": "photo", "media": f"attach://{key}"}
            if i == 0:
                item["caption"] = cap  # 첫 장에만 캡션
            media.append(item)

        import json
        r = requests.post(url,
            data={"chat_id": TELEGRAM_CHAT_ID, "media": json.dumps(media)},
            files=files,
            timeout=60,
        )
        for f in files.values():
            f.close()
        print(f"  MediaGroup: {r.status_code}")
        return r.status_code == 200
    except Exception as e:
        print(f"  [ERR] media_group: {e}")
        return False


# ─────────────────────────────────────────────────────
# 7. 메시지 포맷
# ─────────────────────────────────────────────────────

def format_message(signals, danger, total):
    now    = datetime.now().strftime("%Y-%m-%d %H:%M")
    status = overall_status(danger, total)
    lines  = [
        f"📊 <b>Macro Stress Report v2</b>",
        f"🕐 {now} KST",
        f"Status: {status}",
        f"Alerts: <b>{danger}/{total}</b>",
        "━━━━━━━━━━━━━━━━━━━━━",
    ]
    for cat, items in signals.items():
        lines.append(f"\n<b>{cat}</b>")
        for name, _, val_str, s in items:
            lines.append(f"  {s} {name}: <b>{val_str}</b>")
    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━",
        "🎯 <b>Fed Pivot Trigger Checklist</b>",
        "• TED Spread &gt; 0.5bp",
        "• Fed RRP 8wk drain -30%",
        "• IG Spread &gt; 1.5%",
        "• 3+ EM FX simultaneous weakness",
        "• VIX&gt;30 + OVX&gt;40 simultaneously",
        "• Copper/Gold ratio declining trend",
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────
# 8. 메인
# ─────────────────────────────────────────────────────

def run():
    print("=" * 55)
    print(f"  Macro Dashboard v2  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    mkt     = fetch_market(TICKERS)
    fred    = fetch_all_fred()
    derived = derive(mkt, fred)
    signals = build_signals(mkt, fred, derived)
    danger, total = count_alerts(signals)

    print(f"\n{'='*55}")
    print(f"  Status: {overall_status(danger, total)}")
    print(f"  Alerts: {danger}/{total}")
    print(f"{'='*55}")
    for cat, items in signals.items():
        print(f"\n{cat}")
        for name, _, val_str, s in items:
            print(f"  {s} {name}: {val_str}")

    # 차트 9장 생성
    print("\n[Charts generating...]")
    chart_paths = []
    try:
        chart_paths = make_all_charts(mkt, derived, fred)
    except Exception as e:
        print(f"  [WARN] Charts failed: {e}")

    # 텔레그램 전송
    print("\n[Telegram sending...]")
    print(f"  TOKEN:   {'SET' if TELEGRAM_TOKEN else 'MISSING'}")
    print(f"  CHAT_ID: {'SET' if TELEGRAM_CHAT_ID else 'MISSING'}")

    # ① 텍스트 리포트 먼저
    tg_text(format_message(signals, danger, total))

    # ② 차트 9장을 앨범(묶음)으로 전송
    if chart_paths:
        date_str = datetime.now().strftime("%Y-%m-%d")
        summary  = f"Macro Charts | {date_str} | Alerts {danger}/{total}"
        success  = tg_media_group(chart_paths, [summary] + [""] * (len(chart_paths)-1))

        # 앨범 전송 실패 시 개별 전송으로 fallback
        if not success:
            print("  [FALLBACK] Sending individually...")
            for path in chart_paths:
                tg_photo(path, os.path.basename(path))

    print(f"\n[DONE] Alerts {danger}/{total}")


if __name__ == "__main__":
    run()
