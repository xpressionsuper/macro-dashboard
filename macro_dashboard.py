"""
매크로 스트레스 대시보드 v2 - 버그 수정판
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
    "금":    "GLD",
    "은":    "SLV",
    "WTI":   "CL=F",
    "Brent": "BZ=F",
    "구리":  "HG=F",
    "VIX":   "^VIX",
    "OVX":   "^OVX",
    "GVZ":   "^GVZ",
    "VXEEM": "VXEEM",
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

def fetch_market(tickers):
    print("📡 시장 데이터 수집 중...")
    data = {}
    for label, ticker in tickers.items():
        try:
            df = yf.download(ticker, period=PERIOD, interval=INTERVAL,
                             progress=False, auto_adjust=True)
            if df.empty:
                print(f"  ⚠️  {label} — 데이터 없음")
                continue
            s = df["Close"]
            if isinstance(s, pd.DataFrame):
                s = s.iloc[:, 0]
            s = s.squeeze()
            if not isinstance(s, pd.Series):
                s = pd.Series([float(s)], name=label)
            data[label] = s.dropna()
            print(f"  ✅ {label}")
        except Exception as e:
            print(f"  ❌ {label} — {e}")
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
        print(f"  ✅ FRED: {label}")
        return s.dropna()
    except Exception as e:
        print(f"  ❌ FRED {label} — {e}")
        return pd.Series(dtype=float, name=label)

def fetch_all_fred():
    print("\n📊 FRED 경제 지표 수집 중...")
    return {k: fetch_fred(v, k) for k, v in FRED_SERIES.items()}

def safe_s(mkt, key):
    s = mkt.get(key, pd.Series(dtype=float))
    if not isinstance(s, pd.Series):
        return pd.Series(dtype=float)
    return s.dropna()

def derive(mkt, fred):
    d = {}
    try:
        if "10Y" in mkt and "2Y" in mkt:
            d["TERM_SPREAD"] = safe_s(mkt,"10Y") - safe_s(mkt,"2Y")
        if "금" in mkt and "은" in mkt:
            d["GOLD_SILVER"] = safe_s(mkt,"금") / safe_s(mkt,"은")
        if "구리" in mkt and "금" in mkt:
            d["COPPER_GOLD"] = safe_s(mkt,"구리") / safe_s(mkt,"금")
        if "구리" in mkt and "WTI" in mkt:
            d["COPPER_WTI"] = safe_s(mkt,"구리") / safe_s(mkt,"WTI")
        if "Brent" in mkt and "WTI" in mkt:
            d["BRENT_WTI"] = safe_s(mkt,"Brent") - safe_s(mkt,"WTI")
        bei = fred.get("BEI_10Y", pd.Series(dtype=float))
        if "10Y" in mkt and not bei.empty:
            t10 = safe_s(mkt, "10Y")
            d["REAL_RATE"] = t10 - bei.reindex(t10.index, method="ffill")
    except Exception as e:
        print(f"  ⚠️  파생지표 오류: {e}")
    return d

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
    gvz=latest(m("GVZ")); vxeem=latest(m("VXEEM"))
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
        "🔴 유동성 & 달러 수요": [
            ("DXY 달러인덱스",       dxy,      fmt(dxy),           sig(dxy,104)),
            ("TED 스프레드(bp)",     ted,      fmt(ted,"",2),      sig(ted,0.5)),
            ("RRP 8주 변화율",       rrp_chg,  fmt(rrp_chg,"%"),   sig(rrp_chg,-30,"below")),
        ],
        "🟠 신용 & 채권 스트레스": [
            ("장단기 스프레드(10-2Y)",term_sp, fmt(term_sp,"%p",2),sig(term_sp,0,"below")),
            ("IG 크레딧 스프레드",   ig_sp,    fmt(ig_sp,"%",2),   sig(ig_sp,1.5)),
            ("HY 크레딧 스프레드",   hy_sp,    fmt(hy_sp,"%",2),   sig(hy_sp,4.5)),
            ("EMB 4주 수익률",       emb_chg,  fmt(emb_chg,"%"),   sig(emb_chg,-3,"below")),
            ("HYG 하이일드 ETF",     hyg,      fmt(hyg),           sig(hyg,75,"below")),
        ],
        "🟡 변동성 & 심리": [
            ("VIX",                  vix,      fmt(vix),           sig(vix,25)),
            ("OVX (원유변동성)",     ovx,      fmt(ovx),           sig(ovx,40)),
            ("GVZ (금변동성)",       gvz,      fmt(gvz),           sig(gvz,20)),
            ("VXEEM (신흥국변동성)", vxeem,    fmt(vxeem),         sig(vxeem,30)),
        ],
        "🟢 실물 경기 선행": [
            ("구리/금 비율",         cu_gold,  fmt(cu_gold,"",4),  sig(cu_gold,0.0018,"below")),
            ("구리/WTI 비율",        cu_wti,   fmt(cu_wti,"",3),   sig(cu_wti,0.05,"below")),
            ("BDRY 해운ETF 4주",     bdry_chg, fmt(bdry_chg,"%"),  sig(bdry_chg,-10,"below")),
            ("주간 실업수당",        icsa,     f"{icsa/1000:.0f}K" if icsa else "N/A", sig(icsa,250000)),
        ],
        "🔵 인플레 & 실질금리": [
            ("5Y 기대인플레(BEI)",   bei_5y,   fmt(bei_5y,"%",2),  sig(bei_5y,3.0)),
            ("10Y 기대인플레(BEI)",  bei_10y,  fmt(bei_10y,"%",2), sig(bei_10y,2.8)),
            ("실질금리(10Y-BEI)",    real_rate,fmt(real_rate,"%",2),sig(real_rate,0,"below")),
            ("금/은 비율",           gs_ratio, fmt(gs_ratio),      sig(gs_ratio,80)),
        ],
        "⚫ 지정학 (호르무즈 특화)": [
            ("Brent-WTI 스프레드",   brent_wti,f"${brent_wti:.2f}" if brent_wti else "N/A", sig(brent_wti,5)),
            ("WTI 원유",             wti,      f"${wti:.1f}" if wti else "N/A", sig(wti,90)),
            ("USD/ILS (이스라엘)",   ils,      fmt(ils,"",3),      sig(ils,3.8)),
            ("KSA 사우디ETF 4주",    ksa_chg,  fmt(ksa_chg,"%"),   sig(ksa_chg,-5,"below")),
        ],
        "🌏 신흥국 통화": [
            ("USD/KRW",              krw,      fmt(krw,"",0),      sig(krw,1380)),
            ("USD/TRY",              try_r,    fmt(try_r),         sig(try_r,35)),
            ("USD/ZAR",              zar,      fmt(zar),           sig(zar,19)),
            ("USD/BRL",              brl,      fmt(brl,"",2),      sig(brl,5.5)),
            ("USD/INR",              inr,      fmt(inr),           sig(inr,85)),
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
    if r >= 0.6:    return "🚨 극도 위험 — 복합 위기 신호"
    elif r >= 0.4:  return "🔴 위험 — 다수 경보 점등"
    elif r >= 0.25: return "⚠️ 주의 — 일부 스트레스 감지"
    elif r >= 0.1:  return "🟡 관찰 — 소수 지표 이상"
    else:           return "✅ 안정 — 주요 지표 정상권"

def normalize(s):
    if not isinstance(s, pd.Series): return pd.Series(dtype=float)
    s = s.dropna()
    return (s / s.iloc[0] * 100) if len(s) else s

def make_chart(mkt, derived, fred):
    m  = lambda k: mkt.get(k, pd.Series(dtype=float))
    dv = lambda k: derived.get(k, pd.Series(dtype=float))
    f  = lambda k: fred.get(k, pd.Series(dtype=float))

    fig = make_subplots(rows=3, cols=3,
        subplot_titles=["① DXY & VIX","② 금·은 & 금/은비율","③ 크레딧 스프레드",
                        "④ 신흥국 통화","⑤ 구리/금·구리/WTI","⑥ WTI·Brent·OVX",
                        "⑦ 기대인플레 & 실질금리","⑧ 장단기 스프레드","⑨ ILS·KSA·BDRY"],
        vertical_spacing=0.10, horizontal_spacing=0.07)

    def line(s, row, col, name, color, dash="solid"):
        if not isinstance(s, pd.Series): return
        s = s.dropna()
        if s.empty: return
        fig.add_trace(go.Scatter(x=s.index, y=s.values, name=name,
            line=dict(color=color, width=1.8, dash=dash)), row=row, col=col)

    def bar(s, row, col):
        if not isinstance(s, pd.Series): return
        s = s.dropna()
        if s.empty: return
        colors = [C["green"] if v >= 0 else C["red"] for v in s.values]
        fig.add_trace(go.Bar(x=s.index, y=s.values, marker_color=colors,
            showlegend=False), row=row, col=col)

    line(m("DXY"),1,1,"DXY",C["blue"]); line(m("VIX"),1,1,"VIX",C["red"],"dot")
    line(normalize(m("금")),1,2,"금",C["gold"])
    line(normalize(m("은")),1,2,"은",C["silver"],"dot")
    line(dv("GOLD_SILVER"),1,2,"금/은",C["orange"],"dash")
    line(f("IG_SPREAD"),1,3,"IG",C["blue"]); line(f("HY_SPREAD"),1,3,"HY",C["red"],"dot")
    for key,lbl,clr in [("KRW","원화",C["blue"]),("TRY","리라",C["red"]),
                         ("ZAR","랜드",C["orange"]),("BRL","헤알",C["green"]),("INR","루피",C["purple"])]:
        line(normalize(m(key)),2,1,lbl,clr)
    line(dv("COPPER_GOLD"),2,2,"구리/금",C["cyan"]); line(dv("COPPER_WTI"),2,2,"구리/WTI",C["pink"],"dot")
    line(m("WTI"),2,3,"WTI",C["orange"]); line(m("Brent"),2,3,"Brent",C["gold"],"dot"); line(m("OVX"),2,3,"OVX",C["red"],"dash")
    line(f("BEI_5Y"),3,1,"5Y BEI",C["orange"]); line(f("BEI_10Y"),3,1,"10Y BEI",C["gold"],"dot")
    line(dv("REAL_RATE"),3,1,"실질금리",C["red"],"dash")
    fig.add_hline(y=0,line_dash="dash",line_color=C["subtext"],row=3,col=1)
    bar(dv("TERM_SPREAD"),3,2)
    fig.add_hline(y=0,line_dash="dash",line_color=C["subtext"],row=3,col=2)
    line(normalize(m("ILS")),3,3,"ILS",C["blue"]); line(normalize(m("KSA")),3,3,"KSA",C["gold"],"dot")
    line(normalize(m("BDRY")),3,3,"BDRY",C["cyan"],"dash")

    fig.update_layout(paper_bgcolor=C["bg"],plot_bgcolor=C["panel"],
        font=dict(color=C["text"],size=10),height=1050,width=1300,
        margin=dict(l=50,r=30,t=80,b=30),
        title=dict(text=f"🌐 매크로 스트레스 대시보드 v2  |  {datetime.now().strftime('%Y-%m-%d')}",
            font=dict(size=16,color=C["text"]),x=0.5),
        legend=dict(bgcolor=C["panel"],bordercolor=C["border"],borderwidth=1,font=dict(size=9)))
    for ann in fig.layout.annotations:
        ann.font.size=11; ann.font.color=C["subtext"]
    fig.update_xaxes(gridcolor=C["border"],zeroline=False)
    fig.update_yaxes(gridcolor=C["border"],zeroline=False)
    fig.write_image(CHART_FILE, scale=2)
    print("  ✅ 차트 저장 완료")
    return CHART_FILE

def tg_text(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, data={"chat_id":TELEGRAM_CHAT_ID,"text":text,"parse_mode":"HTML"}, timeout=15)
        print(f"  텍스트 전송: {r.status_code} {r.text[:100]}")
    except Exception as e:
        print(f"  ❌ 텍스트 전송 실패: {e}")

def tg_photo(path):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
        with open(path, "rb") as f:
            r = requests.post(url, data={"chat_id":TELEGRAM_CHAT_ID,
                "caption":f"매크로 차트 | {datetime.now().strftime('%Y-%m-%d')}"},
                files={"photo":f}, timeout=30)
        print(f"  사진 전송: {r.status_code} {r.text[:100]}")
    except Exception as e:
        print(f"  ❌ 사진 전송 실패: {e}")

def format_message(signals, danger, total):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"📊 <b>매크로 스트레스 리포트 v2</b>",
        f"🕐 {now}",
        f"종합: {overall_status(danger, total)}",
        f"경보: <b>{danger}/{total}</b>개 점등",
        "━━━━━━━━━━━━━━━━━━━━━",
    ]
    for cat, items in signals.items():
        lines.append(f"\n<b>{cat}</b>")
        for name, _, val_str, s in items:
            lines.append(f"  {s} {name}: <b>{val_str}</b>")
    lines += ["","━━━━━━━━━━━━━━━━━━━━━",
        "🎯 <b>피벗 트리거 체크</b>",
        "• TED &gt; 0.5bp",
        "• RRP 8주 -30% 급감",
        "• IG 스프레드 &gt; 1.5%",
        "• 신흥국 통화 3개국+ 동시 약세",
        "• VIX&gt;30 + OVX&gt;40 동시"]
    return "\n".join(lines)

def run():
    print("="*55)
    print(f"  매크로 대시보드 v2  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*55)
    mkt = fetch_market(TICKERS)
    fred = fetch_all_fred()
    derived = derive(mkt, fred)
    signals = build_signals(mkt, fred, derived)
    danger, total = count_alerts(signals)

    print(f"\n{'='*55}")
    print(f"  종합: {overall_status(danger, total)}")
    print(f"  경보: {danger}/{total}")
    print(f"{'='*55}")
    for cat, items in signals.items():
        print(f"\n{cat}")
        for name, _, val_str, s in items:
            print(f"  {s} {name}: {val_str}")

    print("\n📈 차트 생성 중...")
    chart_ok = False
    try:
        make_chart(mkt, derived, fred)
        chart_ok = True
    except Exception as e:
        print(f"  ⚠️  차트 실패: {e}")

    print("\n📨 텔레그램 전송 중...")
    print(f"  TOKEN: {'설정됨' if TELEGRAM_TOKEN else '❌ 없음'}")
    print(f"  CHAT_ID: {'설정됨' if TELEGRAM_CHAT_ID else '❌ 없음'}")

    if chart_ok:
        tg_photo(CHART_FILE)
    tg_text(format_message(signals, danger, total))
    print(f"\n✅ 완료 — 경보 {danger}/{total}")

if __name__ == "__main__":
    run()
