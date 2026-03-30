"""
매크로 스트레스 대시보드 v2 - 종합 위기 선행 지표
=====================================================
pip install yfinance pandas plotly requests kaleido

FRED API Key (무료): https://fred.stlouisfed.org/docs/api/api_key.html
Telegram Bot:        @BotFather 에서 발급
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

# ─────────────────────────────────────────────────────
# ⚙️  설정 — 환경변수 우선, 없으면 직접 입력
# ─────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN",   "여기에_봇_토큰")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "여기에_chat_id")
FRED_API_KEY     = os.environ.get("FRED_API_KEY",     "여기에_FRED_키")

PERIOD       = "1y"
INTERVAL     = "1wk"
CHART_FILE   = "/tmp/macro_chart_v2.png"

# ─────────────────────────────────────────────────────
# 색상 팔레트
# ─────────────────────────────────────────────────────
C = dict(
    bg      = "#0d1117",
    panel   = "#161b22",
    border  = "#30363d",
    gold    = "#d4af37",
    silver  = "#c0c0c0",
    red     = "#f85149",
    green   = "#3fb950",
    blue    = "#58a6ff",
    orange  = "#e3b341",
    purple  = "#bc8cff",
    cyan    = "#39d0d8",
    pink    = "#ff7b9c",
    text    = "#e6edf3",
    subtext = "#8b949e",
)

# ─────────────────────────────────────────────────────
# 1. 데이터 수집
# ─────────────────────────────────────────────────────

TICKERS = {
    # ── 달러 & 금리 ──────────────────────────────────
    "DXY":      "DX-Y.NYB",
    "10Y":      "^TNX",
    "2Y":       "^IRX",

    # ── 원자재 ───────────────────────────────────────
    "금":       "GLD",
    "은":       "SLV",
    "WTI":      "CL=F",
    "Brent":    "BZ=F",
    "구리":     "HG=F",

    # ── 변동성 지수 ──────────────────────────────────
    "VIX":      "^VIX",
    "OVX":      "^OVX",    # 원유 변동성
    "GVZ":      "^GVZ",    # 금 변동성
    "VXEEM":    "^VXEEM",  # 신흥국 변동성

    # ── 크레딧 ───────────────────────────────────────
    "EMB":      "EMB",     # 신흥국 채권
    "HYG":      "HYG",     # 미국 하이일드
    "LQD":      "LQD",     # 미국 투자등급
    "TLT":      "TLT",     # 미국 장기국채

    # ── 신흥국 통화 ──────────────────────────────────
    "KRW":      "KRW=X",
    "TRY":      "TRY=X",
    "ZAR":      "ZAR=X",
    "BRL":      "BRL=X",
    "INR":      "INR=X",

    # ── 지정학 특화 ──────────────────────────────────
    "ILS":      "ILS=X",   # 이스라엘 셰켈
    "TASI":     "^TASI",   # 사우디 증시

    # ── 주식 ─────────────────────────────────────────
    "SPY":      "SPY",
    "EEM":      "EEM",

    # ── 실물 선행 ────────────────────────────────────
    "BDI":      "^BDI",    # 발틱운임지수
}


def fetch_market(tickers: dict) -> dict:
    print("📡 시장 데이터 수집 중...")
    data = {}
    for label, ticker in tickers.items():
        try:
            df = yf.download(ticker, period=PERIOD, interval=INTERVAL,
                             progress=False, auto_adjust=True)
            if not df.empty:
                data[label] = df["Close"].squeeze().dropna()
                print(f"  ✅ {label}")
            else:
                print(f"  ⚠️  {label} — 데이터 없음")
        except Exception as e:
            print(f"  ❌ {label} — {e}")
    return data


FRED_SERIES = {
    "RRP":          "RRPONTSYD",        # 연준 역레포 잔액 (유동성)
    "TED":          "TEDRATE",          # TED 스프레드
    "IG_SPREAD":    "BAMLC0A0CM",       # IG 크레딧 스프레드
    "HY_SPREAD":    "BAMLH0A0HYM2",     # HY 크레딧 스프레드
    "BEI_5Y":       "T5YIE",            # 5Y 기대인플레
    "BEI_10Y":      "T10YIE",           # 10Y 기대인플레
    "REALRATE_5Y":  "REAINTRATREARAT5YE", # 5Y 실질금리
    "ICSA":         "ICSA",             # 주간 실업수당 청구
    "CPI":          "CPIAUCSL",         # CPI
}


def fetch_fred(series_id: str, label: str) -> pd.Series:
    if not FRED_API_KEY or FRED_API_KEY == "여기에_FRED_키":
        return pd.Series(dtype=float, name=label)
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_API_KEY}"
           f"&file_type=json&observation_start="
           f"{(datetime.today()-timedelta(days=400)).strftime('%Y-%m-%d')}")
    try:
        obs = requests.get(url, timeout=10).json().get("observations", [])
        s = pd.Series(
            {o["date"]: float(o["value"]) for o in obs if o["value"] != "."},
            name=label,
        )
        s.index = pd.to_datetime(s.index)
        print(f"  ✅ FRED: {label}")
        return s.dropna()
    except Exception as e:
        print(f"  ❌ FRED {label} — {e}")
        return pd.Series(dtype=float, name=label)


def fetch_all_fred() -> dict:
    print("\n📊 FRED 경제 지표 수집 중...")
    return {k: fetch_fred(v, k) for k, v in FRED_SERIES.items()}


# ─────────────────────────────────────────────────────
# 2. 파생 지표 계산
# ─────────────────────────────────────────────────────

def latest(s):
    s = s.dropna() if isinstance(s, pd.Series) else s
    return float(s.iloc[-1]) if len(s) else None

def pct_chg(s, periods=4):
    s = s.dropna()
    if len(s) < periods + 1: return None
    return (s.iloc[-1] / s.iloc[-periods-1] - 1) * 100

def derive(mkt: dict, fred: dict) -> dict:
    d = {}

    # 장단기 스프레드
    if "10Y" in mkt and "2Y" in mkt:
        d["TERM_SPREAD"] = mkt["10Y"] - mkt["2Y"]

    # 금/은 비율
    if "금" in mkt and "은" in mkt:
        d["GOLD_SILVER"] = mkt["금"] / mkt["은"]

    # 구리/금 비율 (경기 온도계)
    if "구리" in mkt and "금" in mkt:
        d["COPPER_GOLD"] = mkt["구리"] / mkt["금"]

    # 구리/WTI 비율 (스태그플레이션 신호)
    if "구리" in mkt and "WTI" in mkt:
        d["COPPER_WTI"] = mkt["구리"] / mkt["WTI"]

    # Brent-WTI 스프레드 (중동 공급 차질)
    if "Brent" in mkt and "WTI" in mkt:
        d["BRENT_WTI"] = mkt["Brent"] - mkt["WTI"]

    # HYG/LQD 비율 (크레딧 리스크 선호도)
    if "HYG" in mkt and "LQD" in mkt:
        d["HYG_LQD"] = mkt["HYG"] / mkt["LQD"]

    # 실질금리 근사 (10Y - BEI_10Y)
    if "10Y" in mkt and not fred.get("BEI_10Y", pd.Series()).empty:
        bei = fred["BEI_10Y"].reindex(mkt["10Y"].index, method="ffill")
        d["REAL_RATE"] = mkt["10Y"] - bei

    return d


# ─────────────────────────────────────────────────────
# 3. 시그널 판단 엔진
# ─────────────────────────────────────────────────────

def s(val, thr, direction="above"):
    if val is None: return "⚪"
    return "🔴" if (val > thr if direction == "above" else val < thr) else "🟢"

def build_all_signals(mkt, fred, derived):

    def m(k): return mkt.get(k, pd.Series())
    def f(k): return fred.get(k, pd.Series())
    def dv(k): return derived.get(k, pd.Series())

    # 값 추출
    dxy        = latest(m("DXY"))
    vix        = latest(m("VIX"))
    ovx        = latest(m("OVX"))
    gvz        = latest(m("GVZ"))
    vxeem      = latest(m("VXEEM"))
    krw        = latest(m("KRW"))
    try_r      = latest(m("TRY"))
    zar        = latest(m("ZAR"))
    brl        = latest(m("BRL"))
    inr        = latest(m("INR"))
    ils        = latest(m("ILS"))
    emb_chg    = pct_chg(m("EMB"), 4)
    hyg        = latest(m("HYG"))
    wti        = latest(m("WTI"))
    bdi        = latest(m("BDI"))
    tasi_chg   = pct_chg(m("TASI"), 4)
    term_sp    = latest(dv("TERM_SPREAD"))
    gs_ratio   = latest(dv("GOLD_SILVER"))
    cu_gold    = latest(dv("COPPER_GOLD"))
    cu_wti     = latest(dv("COPPER_WTI"))
    brent_wti  = latest(dv("BRENT_WTI"))
    real_rate  = latest(dv("REAL_RATE"))
    rrp        = latest(f("RRP"))
    rrp_chg    = pct_chg(f("RRP"), 8)
    ted        = latest(f("TED"))
    ig_sp      = latest(f("IG_SPREAD"))
    hy_sp      = latest(f("HY_SPREAD"))
    bei_5y     = latest(f("BEI_5Y"))
    bei_10y    = latest(f("BEI_10Y"))
    icsa       = latest(f("ICSA"))

    signals = {

        "🔴 유동성 & 달러 수요": [
            ("DXY 달러인덱스",          dxy,     f"{dxy:.1f}" if dxy else "N/A",           s(dxy, 104)),
            ("TED 스프레드 (bp)",       ted,     f"{ted:.2f}" if ted else "N/A",            s(ted, 0.5)),
            ("RRP 잔액 8주 변화율",     rrp_chg, f"{rrp_chg:.1f}%" if rrp_chg else "N/A",  s(rrp_chg, -30, "below")),
        ],

        "🟠 신용 & 채권 스트레스": [
            ("장단기 스프레드(10-2Y)",   term_sp, f"{term_sp:.2f}%p" if term_sp else "N/A", s(term_sp, 0, "below")),
            ("IG 크레딧 스프레드",      ig_sp,   f"{ig_sp:.2f}%" if ig_sp else "N/A",       s(ig_sp, 1.5)),
            ("HY 크레딧 스프레드",      hy_sp,   f"{hy_sp:.2f}%" if hy_sp else "N/A",       s(hy_sp, 4.5)),
            ("EMB 4주 수익률",          emb_chg, f"{emb_chg:.1f}%" if emb_chg else "N/A",   s(emb_chg, -3, "below")),
            ("HYG 하이일드 ETF",        hyg,     f"{hyg:.1f}" if hyg else "N/A",            s(hyg, 75, "below")),
        ],

        "🟡 변동성 & 심리": [
            ("VIX (S&P500 변동성)",     vix,     f"{vix:.1f}" if vix else "N/A",            s(vix, 25)),
            ("OVX (원유 변동성)",       ovx,     f"{ovx:.1f}" if ovx else "N/A",            s(ovx, 40)),
            ("GVZ (금 변동성)",         gvz,     f"{gvz:.1f}" if gvz else "N/A",            s(gvz, 20)),
            ("VXEEM (신흥국 변동성)",   vxeem,   f"{vxeem:.1f}" if vxeem else "N/A",        s(vxeem, 30)),
        ],

        "🟢 실물 경기 선행": [
            ("구리/금 비율",            cu_gold, f"{cu_gold:.4f}" if cu_gold else "N/A",     s(cu_gold, 0.0018, "below")),
            ("구리/WTI 비율",           cu_wti,  f"{cu_wti:.3f}" if cu_wti else "N/A",       s(cu_wti, 0.05, "below")),
            ("발틱운임지수(BDI)",        bdi,     f"{bdi:.0f}" if bdi else "N/A",            s(bdi, 1000, "below")),
            ("주간 실업수당(만명)",      icsa,    f"{icsa/1000:.0f}K" if icsa else "N/A",    s(icsa, 250000)),
        ],

        "🔵 인플레 & 실질금리": [
            ("5Y 기대인플레(BEI)",      bei_5y,  f"{bei_5y:.2f}%" if bei_5y else "N/A",     s(bei_5y, 3.0)),
            ("10Y 기대인플레(BEI)",     bei_10y, f"{bei_10y:.2f}%" if bei_10y else "N/A",   s(bei_10y, 2.8)),
            ("실질금리(10Y-BEI)",       real_rate,f"{real_rate:.2f}%" if real_rate else "N/A", s(real_rate, 0, "below")),
            ("금/은 비율",              gs_ratio,f"{gs_ratio:.1f}" if gs_ratio else "N/A",   s(gs_ratio, 80)),
        ],

        "⚫ 지정학 (호르무즈 특화)": [
            ("Brent-WTI 스프레드",      brent_wti,f"${brent_wti:.2f}" if brent_wti else "N/A", s(brent_wti, 5)),
            ("WTI 원유",                wti,     f"${wti:.1f}" if wti else "N/A",            s(wti, 90)),
            ("USD/ILS (이스라엘)",      ils,     f"{ils:.3f}" if ils else "N/A",             s(ils, 3.8)),
            ("사우디 TASI 4주 수익률",  tasi_chg,f"{tasi_chg:.1f}%" if tasi_chg else "N/A", s(tasi_chg, -5, "below")),
        ],

        "🌏 신흥국 통화": [
            ("USD/KRW 원화",            krw,     f"{krw:.0f}" if krw else "N/A",            s(krw, 1380)),
            ("USD/TRY 터키",            try_r,   f"{try_r:.1f}" if try_r else "N/A",        s(try_r, 35)),
            ("USD/ZAR 남아공",          zar,     f"{zar:.1f}" if zar else "N/A",            s(zar, 19)),
            ("USD/BRL 브라질",          brl,     f"{brl:.2f}" if brl else "N/A",            s(brl, 5.5)),
            ("USD/INR 인도",            inr,     f"{inr:.1f}" if inr else "N/A",            s(inr, 85)),
        ],
    }

    return signals


def count_alerts(signals: dict) -> tuple:
    total, danger = 0, 0
    for items in signals.values():
        for row in items:
            total += 1
            if row[3] == "🔴": danger += 1
    return danger, total


def overall_status(danger, total) -> str:
    ratio = danger / total if total else 0
    if ratio >= 0.6:   return "🚨 극도 위험 — 복합 위기 신호"
    elif ratio >= 0.4: return "🔴 위험 — 다수 경보 점등"
    elif ratio >= 0.25: return "⚠️ 주의 — 일부 스트레스 감지"
    elif ratio >= 0.1: return "🟡 관찰 — 소수 지표 이상"
    else:              return "✅ 안정 — 주요 지표 정상권"


# ─────────────────────────────────────────────────────
# 4. 차트 생성 (3x3 = 9개 패널)
# ─────────────────────────────────────────────────────

def normalize(s):
    s = s.dropna()
    return (s / s.iloc[0] * 100) if len(s) else s

def make_chart(mkt: dict, derived: dict, fred: dict) -> str:

    def m(k): return mkt.get(k, pd.Series())
    def dv(k): return derived.get(k, pd.Series())
    def f(k): return fred.get(k, pd.Series())

    titles = [
        "① DXY & 변동성(VIX)",
        "② 금·은 정규화 & 금/은 비율",
        "③ 크레딧 스프레드 (IG·HY)",
        "④ 신흥국 통화 (정규화)",
        "⑤ 구리/금 비율 (경기선행)",
        "⑥ 원유 (WTI·Brent & OVX)",
        "⑦ 기대인플레 & 실질금리",
        "⑧ 장단기 스프레드",
        "⑨ 지정학 (ILS·TASI·BDI)",
    ]

    fig = make_subplots(
        rows=3, cols=3,
        subplot_titles=titles,
        vertical_spacing=0.10,
        horizontal_spacing=0.07,
    )

    def line(s, row, col, name, color, dash="solid", yaxis2=False):
        s = s.dropna()
        if s.empty: return
        fig.add_trace(go.Scatter(
            x=s.index, y=s.values, name=name,
            line=dict(color=color, width=1.8, dash=dash),
        ), row=row, col=col)

    def bar_chart(s, row, col, name, pos_color, neg_color):
        s = s.dropna()
        if s.empty: return
        colors = [pos_color if v >= 0 else neg_color for v in s.values]
        fig.add_trace(go.Bar(
            x=s.index, y=s.values, name=name,
            marker_color=colors, showlegend=False,
        ), row=row, col=col)

    # ① DXY & VIX
    line(m("DXY"), 1,1, "DXY", C["blue"])
    line(m("VIX"), 1,1, "VIX", C["red"], dash="dot")

    # ② 금·은 & 금/은 비율
    line(normalize(m("금")),     1,2, "금(정규화)",  C["gold"])
    line(normalize(m("은")),     1,2, "은(정규화)",  C["silver"], dash="dot")
    line(dv("GOLD_SILVER"),      1,2, "금/은비율",   C["orange"], dash="dash")

    # ③ 크레딧 스프레드 (FRED)
    line(f("IG_SPREAD"), 1,3, "IG 스프레드", C["blue"])
    line(f("HY_SPREAD"), 1,3, "HY 스프레드", C["red"], dash="dot")

    # ④ 신흥국 통화 정규화
    em_pairs = [("KRW","원화",C["blue"]),("TRY","리라",C["red"]),
                ("ZAR","랜드",C["orange"]),("BRL","헤알",C["green"]),
                ("INR","루피",C["purple"])]
    for key, lbl, clr in em_pairs:
        line(normalize(m(key)), 2,1, lbl, clr)

    # ⑤ 구리/금 비율
    line(dv("COPPER_GOLD"), 2,2, "구리/금", C["cyan"])
    line(dv("COPPER_WTI"),  2,2, "구리/WTI", C["pink"], dash="dot")

    # ⑥ 원유 & OVX
    line(m("WTI"),   2,3, "WTI",   C["orange"])
    line(m("Brent"), 2,3, "Brent", C["gold"], dash="dot")
    line(m("OVX"),   2,3, "OVX",   C["red"],  dash="dash")

    # ⑦ 기대인플레 & 실질금리
    line(f("BEI_5Y"),    3,1, "5Y BEI",   C["orange"])
    line(f("BEI_10Y"),   3,1, "10Y BEI",  C["gold"],  dash="dot")
    line(dv("REAL_RATE"),3,1, "실질금리", C["red"],   dash="dash")
    fig.add_hline(y=0, line_dash="dash", line_color=C["subtext"], row=3, col=1)

    # ⑧ 장단기 스프레드
    bar_chart(dv("TERM_SPREAD"), 3,2, "스프레드", C["green"], C["red"])
    fig.add_hline(y=0, line_dash="dash", line_color=C["subtext"], row=3, col=2)

    # ⑨ 지정학 (ILS·TASI·BDI 정규화)
    line(normalize(m("ILS")),  3,3, "ILS(셰켈)", C["blue"])
    line(normalize(m("TASI")), 3,3, "TASI(사우디)", C["gold"], dash="dot")
    line(normalize(m("BDI")),  3,3, "BDI(운임)", C["cyan"], dash="dash")

    # ── 레이아웃 ──────────────────────────────────────
    fig.update_layout(
        paper_bgcolor=C["bg"],
        plot_bgcolor=C["panel"],
        font=dict(color=C["text"], size=10),
        height=1050, width=1300,
        margin=dict(l=50, r=30, t=80, b=30),
        title=dict(
            text=(f"🌐 매크로 스트레스 대시보드 v2  |  "
                  f"{datetime.now().strftime('%Y-%m-%d')}"),
            font=dict(size=16, color=C["text"]), x=0.5,
        ),
        legend=dict(bgcolor=C["panel"], bordercolor=C["border"],
                    borderwidth=1, font=dict(size=9)),
    )
    for ann in fig.layout.annotations:
        ann.font.size  = 11
        ann.font.color = C["subtext"]
    fig.update_xaxes(gridcolor=C["border"], zeroline=False)
    fig.update_yaxes(gridcolor=C["border"], zeroline=False)

    fig.write_image(CHART_FILE, scale=2)
    print(f"  ✅ 차트 저장: {CHART_FILE}")
    return CHART_FILE


# ─────────────────────────────────────────────────────
# 5. 텔레그램 전송
# ─────────────────────────────────────────────────────

def tg_text(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }, timeout=15)

def tg_photo(path: str, caption: str = ""):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"
    with open(path, "rb") as f:
        requests.post(url, data={
            "chat_id": TELEGRAM_CHAT_ID,
            "caption": caption,
        }, files={"photo": f}, timeout=30)


# ─────────────────────────────────────────────────────
# 6. 메시지 조합
# ─────────────────────────────────────────────────────

def format_message(signals: dict, danger: int, total: int) -> str:
    now    = datetime.now().strftime("%Y-%m-%d %H:%M")
    status = overall_status(danger, total)

    lines = [
        f"📊 <b>매크로 스트레스 리포트 v2</b>",
        f"🕐 {now}",
        f"",
        f"종합: {status}",
        f"경보: <b>{danger}/{total}</b>개 점등",
        f"━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    for category, items in signals.items():
        lines.append(f"\n<b>{category}</b>")
        for name, _, val_str, sig in items:
            lines.append(f"  {sig} {name}: <b>{val_str}</b>")

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━",
        "🎯 <b>연준 피벗 트리거 체크</b>",
        "• TED 스프레드 0.5bp 돌파",
        "• RRP 잔액 8주 -30% 이상 급감",
        "• IG 스프레드 1.5% 초과",
        "• 신흥국 통화 동시 약세 (3개국+)",
        "• VIX 30 초과 + OVX 40 초과 동시",
        "• 구리/금 비율 하락 추세 지속",
    ]

    return "\n".join(lines)


# ─────────────────────────────────────────────────────
# 7. 메인
# ─────────────────────────────────────────────────────

def run():
    print("=" * 55)
    print("  매크로 스트레스 대시보드 v2 시작")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    # 데이터 수집
    mkt     = fetch_market(TICKERS)
    fred    = fetch_all_fred()
    derived = derive(mkt, fred)

    # 시그널 계산
    signals         = build_all_signals(mkt, fred, derived)
    danger, total   = count_alerts(signals)

    # 터미널 출력
    print("\n" + "=" * 55)
    print(f"  종합: {overall_status(danger, total)}")
    print(f"  경보: {danger}/{total}")
    print("=" * 55)
    for cat, items in signals.items():
        print(f"\n{cat}")
        for name, _, val_str, sig in items:
            print(f"  {sig} {name}: {val_str}")

    # 차트 생성
    chart_ok = False
    try:
        chart_path = make_chart(mkt, derived, fred)
        chart_ok = True
    except Exception as e:
        print(f"\n  ⚠️  차트 생성 실패: {e}")

    # 텔레그램 전송
    print("\n📨 텔레그램 전송 중...")
    if chart_ok:
        try:
            tg_photo(chart_path,
                     caption=f"매크로 차트 | {datetime.now().strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"  ⚠️  사진 전송 실패: {e}")

    msg = format_message(signals, danger, total)
    tg_text(msg)
    print(f"  ✅ 전송 완료 — 경보 {danger}/{total}")


if __name__ == "__main__":
    run()
