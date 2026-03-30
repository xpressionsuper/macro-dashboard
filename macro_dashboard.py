"""
매크로 스트레스 대시보드 v2 - 차트 어노테이션 수정판
"""

import os
import requests
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN",   "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
FRED_API_KEY     = os.environ.get("FRED_API_KEY",     "")

PERIOD     = "1y"
INTERVAL   = "1wk"
CHART_FILE = "/tmp/macro_chart_v2.png"

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

# 패널 설명 — 제목에 포함 (줄바꿈)
PANEL_TITLES = [
    "① DXY & VIX<br><sub>DXY>104=dollar stress | VIX>25=fear</sub>",
    "② Gold vs Silver (normalized)<br><sub>G/S Ratio>80 = silver undervalued</sub>",
    "③ Credit Spreads<br><sub>IG>1.5% | HY>4.5% = credit stress</sub>",
    "④ EM Currencies (normalized)<br><sub>Rising = EM currency weakness vs USD</sub>",
    "⑤ Copper Ratios<br><sub>Cu/Gold down=recession | Cu/WTI down=stagflation</sub>",
    "⑥ Oil & Volatility<br><sub>WTI>$90=inflation risk | OVX>40=panic</sub>",
    "⑦ Inflation & Real Rate<br><sub>BEI=breakeven infl | Real Rate<0=gold bullish</sub>",
    "⑧ Yield Curve (10Y-2Y)<br><sub>Below 0 = inverted = recession leading signal</sub>",
    "⑨ Geopolitical Monitor<br><sub>ILS/KSA weak=Mid-East stress | BDRY down=trade slump</sub>",
]

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
    if r >= 0.6:    return "🚨 CRITICAL - Complex Crisis Signal"
    elif r >= 0.4:  return "🔴 DANGER - Multiple Alerts Triggered"
    elif r >= 0.25: return "⚠️ WARNING - Partial Stress Detected"
    elif r >= 0.1:  return "🟡 WATCH - Minor Anomalies"
    else:           return "✅ STABLE - Major Indicators Normal"


# ─────────────────────────────────────────────────────
# 4. 차트 생성
# ─────────────────────────────────────────────────────

def normalize(s):
    if not isinstance(s, pd.Series): return pd.Series(dtype=float)
    s = s.dropna()
    return (s / s.iloc[0] * 100) if len(s) else s


def make_chart(mkt, derived, fred):
    m  = lambda k: mkt.get(k, pd.Series(dtype=float))
    dv = lambda k: derived.get(k, pd.Series(dtype=float))
    f  = lambda k: fred.get(k, pd.Series(dtype=float))

    fig = make_subplots(
        rows=3, cols=3,
        subplot_titles=PANEL_TITLES,
        vertical_spacing=0.14,
        horizontal_spacing=0.08,
    )

    def line(s, row, col, name, color, dash="solid"):
        if not isinstance(s, pd.Series): return
        s = s.dropna()
        if s.empty: return
        fig.add_trace(go.Scatter(
            x=s.index, y=s.values, name=name,
            line=dict(color=color, width=1.8, dash=dash),
            hovertemplate=f"<b>{name}</b><br>%{{x|%Y-%m-%d}}<br>%{{y:.2f}}<extra></extra>",
        ), row=row, col=col)

    def bar(s, row, col):
        if not isinstance(s, pd.Series): return
        s = s.dropna()
        if s.empty: return
        colors = [C["green"] if v >= 0 else C["red"] for v in s.values]
        fig.add_trace(go.Bar(
            x=s.index, y=s.values,
            marker_color=colors, showlegend=False,
        ), row=row, col=col)

    def hline(y, row, col, color, label=""):
        fig.add_hline(
            y=y, line_dash="dash", line_color=color,
            line_width=1,
            annotation_text=label,
            annotation_font_size=8,
            annotation_font_color=color,
            row=row, col=col,
        )

    # ① DXY & VIX
    line(m("DXY"), 1,1,"DXY", C["blue"])
    line(m("VIX"), 1,1,"VIX", C["red"],"dot")
    hline(104, 1,1, C["orange"], "DXY 104")
    hline(25,  1,1, C["red"],    "VIX 25")

    # ② Gold vs Silver
    line(normalize(m("Gold")),   1,2,"Gold",     C["gold"])
    line(normalize(m("Silver")), 1,2,"Silver",   C["silver"],"dot")
    line(dv("GOLD_SILVER"),      1,2,"G/S Ratio",C["orange"],"dash")
    hline(80, 1,2, C["red"], "Ratio 80")

    # ③ Credit Spreads
    line(f("IG_SPREAD"), 1,3,"IG Spread", C["blue"])
    line(f("HY_SPREAD"), 1,3,"HY Spread", C["red"],"dot")
    hline(1.5, 1,3, C["orange"], "IG 1.5%")
    hline(4.5, 1,3, C["red"],    "HY 4.5%")

    # ④ EM Currencies
    for key,lbl,clr in [("KRW","KRW",C["blue"]),("TRY","TRY",C["red"]),
                         ("ZAR","ZAR",C["orange"]),("BRL","BRL",C["green"]),
                         ("INR","INR",C["purple"])]:
        line(normalize(m(key)), 2,1, lbl, clr)

    # ⑤ Copper Ratios
    line(dv("COPPER_GOLD"), 2,2,"Cu/Gold", C["cyan"])
    line(dv("COPPER_WTI"),  2,2,"Cu/WTI",  C["pink"],"dot")

    # ⑥ Oil & OVX
    line(m("WTI"),   2,3,"WTI",   C["orange"])
    line(m("Brent"), 2,3,"Brent", C["gold"],"dot")
    line(m("OVX"),   2,3,"OVX",   C["red"],"dash")
    hline(90,  2,3, C["red"],    "WTI $90")
    hline(40,  2,3, C["orange"], "OVX 40")

    # ⑦ Inflation & Real Rate
    line(f("BEI_5Y"),    3,1,"5Y BEI",    C["orange"])
    line(f("BEI_10Y"),   3,1,"10Y BEI",   C["gold"],"dot")
    line(dv("REAL_RATE"),3,1,"Real Rate", C["red"],"dash")
    hline(0,   3,1, C["subtext"], "")
    hline(3.0, 3,1, C["orange"],  "BEI 3%")

    # ⑧ Yield Curve
    bar(dv("TERM_SPREAD"), 3,2)
    hline(0, 3,2, C["subtext"], "Inversion")

    # ⑨ Geopolitical
    line(normalize(m("ILS")),  3,3,"ILS",  C["blue"])
    line(normalize(m("KSA")),  3,3,"KSA",  C["gold"],"dot")
    line(normalize(m("BDRY")), 3,3,"BDRY", C["cyan"],"dash")

    # ── 레이아웃 ─────────────────────────────────────
    fig.update_layout(
        paper_bgcolor=C["bg"],
        plot_bgcolor=C["panel"],
        font=dict(family="Arial, sans-serif", color=C["text"], size=10),
        height=1200, width=1400,
        margin=dict(l=55, r=35, t=90, b=40),
        title=dict(
            text=(f"Macro Stress Dashboard v2  |  "
                  f"{datetime.now().strftime('%Y-%m-%d')}  |  "
                  f"Stagflation & Dollar Cycle Monitor"),
            font=dict(size=15, color=C["text"], family="Arial, sans-serif"),
            x=0.5,
        ),
        legend=dict(
            bgcolor=C["panel"], bordercolor=C["border"],
            borderwidth=1, font=dict(size=9),
        ),
    )

    # subplot 제목 폰트 (HTML 태그 포함이라 크기 조정)
    for ann in fig.layout.annotations:
        ann.font = dict(size=10, color="#adbac7", family="Arial, sans-serif")

    fig.update_xaxes(gridcolor=C["border"], zeroline=False,
                     tickfont=dict(size=8, family="Arial, sans-serif"))
    fig.update_yaxes(gridcolor=C["border"], zeroline=False,
                     tickfont=dict(size=8, family="Arial, sans-serif"))

    fig.write_image(CHART_FILE, scale=2)
    print("  [OK] Chart saved")
    return CHART_FILE


# ─────────────────────────────────────────────────────
# 5. 텔레그램
# ─────────────────────────────────────────────────────

def tg_text(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"
        }, timeout=15)
        print(f"  Text sent: {r.status_code}")
    except Exception as e:
        print(f"  [ERR] text: {e}")

def tg_photo(path):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        with open(path, "rb") as f:
            r = requests.post(url, data={
                "chat_id": TELEGRAM_CHAT_ID,
                "caption": f"Macro Chart | {datetime.now().strftime('%Y-%m-%d')}",
            }, files={"photo": f}, timeout=30)
        print(f"  Photo sent: {r.status_code}")
    except Exception as e:
        print(f"  [ERR] photo: {e}")


# ─────────────────────────────────────────────────────
# 6. 메시지 포맷
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
# 7. 메인
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

    print("\n[Chart generating...]")
    chart_ok = False
    try:
        make_chart(mkt, derived, fred)
        chart_ok = True
    except Exception as e:
        print(f"  [WARN] Chart failed: {e}")

    print("\n[Telegram sending...]")
    print(f"  TOKEN:   {'SET' if TELEGRAM_TOKEN else 'MISSING'}")
    print(f"  CHAT_ID: {'SET' if TELEGRAM_CHAT_ID else 'MISSING'}")

    if chart_ok:
        tg_photo(CHART_FILE)
    tg_text(format_message(signals, danger, total))
    print(f"\n[DONE] Alerts {danger}/{total}")


if __name__ == "__main__":
    run()
