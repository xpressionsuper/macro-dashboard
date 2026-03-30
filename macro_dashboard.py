"""
Macro Stress Dashboard - 스태그플레이션 & 달러 사이클 모니터링
=============================================================
필요 패키지 설치:
    pip install yfinance pandas plotly requests

FRED API Key (무료):
    https://fred.stlouisfed.org/docs/api/api_key.html 에서 발급
    아래 FRED_API_KEY 변수에 입력
"""

import os
import yfinance as yf
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
FRED_API_KEY      = os.environ.get("FRED_API_KEY", "60e03b329fa73a5a9f24021c9223e7e5")
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_BOT_TOKEN", "8645824137:AAH9LImqFfVWnp1RGUuOf6eXYU2cFkn3fHE")
TELEGRAM_CHAT_ID  = os.environ.get("TELEGRAM_CHAT_ID", "521751814")
PERIOD   = "2y"   # 조회 기간 (1y / 2y / 5y)
INTERVAL = "1wk"  # 데이터 주기 (1d / 1wk)

# ─────────────────────────────────────────
# 1. 데이터 수집
# ─────────────────────────────────────────

def fetch_yfinance(tickers: dict) -> pd.DataFrame:
    """yfinance로 종가 데이터 수집"""
    print("📡 시장 데이터 수집 중...")
    data = {}
    for label, ticker in tickers.items():
        try:
            df = yf.download(ticker, period=PERIOD, interval=INTERVAL,
                             progress=False, auto_adjust=True)
            if not df.empty:
                data[label] = df["Close"].squeeze()
                print(f"  ✅ {label} ({ticker})")
            else:
                print(f"  ⚠️  {label} ({ticker}) - 데이터 없음")
        except Exception as e:
            print(f"  ❌ {label} ({ticker}) - 오류: {e}")
    return pd.DataFrame(data)


def fetch_fred(series_id: str, label: str) -> pd.Series:
    """FRED에서 경제 지표 수집"""
    if FRED_API_KEY == "your_fred_api_key_here":
        print(f"  ⚠️  FRED API 키 미설정 → {label} 스킵")
        return pd.Series(dtype=float, name=label)
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_API_KEY}"
           f"&file_type=json&observation_start="
           f"{(datetime.today() - timedelta(days=730)).strftime('%Y-%m-%d')}")
    try:
        r = requests.get(url, timeout=10)
        obs = r.json().get("observations", [])
        s = pd.Series(
            {o["date"]: float(o["value"]) for o in obs if o["value"] != "."},
            name=label
        )
        s.index = pd.to_datetime(s.index)
        print(f"  ✅ {label} (FRED: {series_id})")
        return s
    except Exception as e:
        print(f"  ❌ {label} (FRED) - 오류: {e}")
        return pd.Series(dtype=float, name=label)


# ── 티커 정의 ──────────────────────────────
MARKET_TICKERS = {
    # 달러 & 금리
    "DXY (달러인덱스)":        "DX-Y.NYB",
    "미국10Y 국채금리":         "^TNX",
    "미국2Y 국채금리":          "^IRX",

    # 금·은·원자재
    "금 (GLD)":                "GLD",
    "은 (SLV)":                "SLV",
    "WTI 원유":                "CL=F",

    # 신흥국 통화 (vs USD ETF or 직접)
    "신흥국통화 ETF (CEW)":    "CEW",
    "한국 원화 (USD/KRW)":     "KRW=X",
    "터키 리라 (USD/TRY)":     "TRY=X",
    "남아공 랜드 (USD/ZAR)":   "ZAR=X",
    "인도 루피 (USD/INR)":     "INR=X",
    "브라질 헤알 (USD/BRL)":   "BRL=X",

    # 신흥국 채권 스프레드 프록시
    "신흥국 채권 ETF (EMB)":   "EMB",
    "미국 하이일드 (HYG)":     "HYG",

    # 주식
    "S&P500":                  "^GSPC",
    "신흥국 주식 (EEM)":       "EEM",
}

print("=" * 55)
print("  매크로 스트레스 대시보드 - 데이터 로딩")
print("=" * 55)
mkt = fetch_yfinance(MARKET_TICKERS)

# FRED 지표
print("\n📊 FRED 경제 지표 수집 중...")
cpi          = fetch_fred("CPIAUCSL",  "미국 CPI")
pce          = fetch_fred("PCEPI",     "PCE 물가")
real_rate_5y = fetch_fred("REAINTRATREARAT5YE", "5Y 기대실질금리")
em_reserves  = fetch_fred("TRESEGUSM052N", "신흥국 외환보유고(USD)")


# ─────────────────────────────────────────
# 2. 파생 지표 계산
# ─────────────────────────────────────────

def safe_col(df, name):
    return df[name] if name in df.columns else pd.Series(dtype=float)

# 금/은 비율 (높을수록 은 저평가)
gold_silver_ratio = pd.Series(dtype=float)
if "금 (GLD)" in mkt.columns and "은 (SLV)" in mkt.columns:
    gold_silver_ratio = mkt["금 (GLD)"] / mkt["은 (SLV)"]
    gold_silver_ratio.name = "금/은 비율"

# 장단기 금리 스프레드 (10Y - 2Y): 역전 시 경기침체 선행 신호
term_spread = pd.Series(dtype=float)
if "미국10Y 국채금리" in mkt.columns and "미국2Y 국채금리" in mkt.columns:
    term_spread = mkt["미국10Y 국채금리"] - mkt["미국2Y 국채금리"]
    term_spread.name = "장단기 스프레드(10Y-2Y)"

# EMB 크레딧 스프레드 프록시 (EMB 가격 하락 = 스프레드 확대)
emb_pct = pd.Series(dtype=float)
if "신흥국 채권 ETF (EMB)" in mkt.columns:
    emb_pct = mkt["신흥국 채권 ETF (EMB)"].pct_change(4) * 100  # 4주 수익률
    emb_pct.name = "EMB 4주 수익률(%)"


# ─────────────────────────────────────────
# 3. 시그널 계산
# ─────────────────────────────────────────

def latest(s: pd.Series):
    s = s.dropna()
    return s.iloc[-1] if len(s) else None

def signal_row(name, value, threshold, direction="above", unit=""):
    """
    direction: 'above' → value > threshold 이면 위험
               'below' → value < threshold 이면 위험
    """
    if value is None:
        return {"지표": name, "현재값": "N/A", "임계값": f"{threshold}{unit}",
                "신호": "⚪ 데이터없음", "해석": "-"}
    val_str = f"{value:.2f}{unit}"
    thr_str = f"{threshold}{unit}"
    if direction == "above":
        danger = value > threshold
    else:
        danger = value < threshold
    sign = "🔴 위험" if danger else "🟢 정상"
    return {"지표": name, "현재값": val_str, "임계값": thr_str, "신호": sign}

signals = [
    signal_row("DXY 달러인덱스",    latest(safe_col(mkt,"DXY (달러인덱스)")),   104,  "above"),
    signal_row("장단기 금리 스프레드", latest(term_spread),                       0,   "below", "%p"),
    signal_row("EMB 4주 수익률",    latest(emb_pct),                            -3,   "below", "%"),
    signal_row("HYG (하이일드)",    latest(safe_col(mkt,"미국 하이일드 (HYG)")), 75,   "below"),
    signal_row("금/은 비율",        latest(gold_silver_ratio),                   80,   "above"),
    signal_row("신흥국 주식(EEM)",  latest(safe_col(mkt,"신흥국 주식 (EEM)")),   35,   "below"),
    signal_row("USD/KRW",          latest(safe_col(mkt,"한국 원화 (USD/KRW)")), 1380, "above"),
    signal_row("USD/TRY",          latest(safe_col(mkt,"터키 리라 (USD/TRY)")), 35,   "above"),
]

sig_df = pd.DataFrame(signals)
print("\n" + "=" * 55)
print("  📊 매크로 스트레스 시그널 요약")
print("=" * 55)
print(sig_df.to_string(index=False))


# ─────────────────────────────────────────
# 4. 시각화 (Plotly)
# ─────────────────────────────────────────

COLOR = {
    "bg":      "#0d1117",
    "panel":   "#161b22",
    "border":  "#30363d",
    "gold":    "#d4af37",
    "silver":  "#c0c0c0",
    "red":     "#f85149",
    "green":   "#3fb950",
    "blue":    "#58a6ff",
    "orange":  "#e3b341",
    "purple":  "#bc8cff",
    "text":    "#e6edf3",
    "subtext": "#8b949e",
}

fig = make_subplots(
    rows=4, cols=2,
    subplot_titles=[
        "① DXY 달러인덱스",         "② 금 vs 은 (정규화)",
        "③ 장단기 금리 스프레드",    "④ 신흥국 통화 (USD 대비, 정규화)",
        "⑤ EMB 신흥국채권 & HYG",   "⑥ WTI 원유",
        "⑦ S&P500 vs 신흥국(EEM)",  "⑧ 금/은 비율",
    ],
    vertical_spacing=0.09,
    horizontal_spacing=0.07,
)

def add_line(fig, s, row, col, name, color, dash="solid", secondary=False):
    s = s.dropna()
    if s.empty:
        return
    fig.add_trace(go.Scatter(
        x=s.index, y=s.values, name=name,
        line=dict(color=color, width=1.8, dash=dash),
        showlegend=True,
    ), row=row, col=col)

def normalize(s):
    s = s.dropna()
    return (s / s.iloc[0] * 100) if len(s) else s

# ① DXY
add_line(fig, safe_col(mkt,"DXY (달러인덱스)"), 1,1, "DXY", COLOR["blue"])

# ② 금 vs 은 정규화
add_line(fig, normalize(safe_col(mkt,"금 (GLD)")), 1,2, "금(GLD)", COLOR["gold"])
add_line(fig, normalize(safe_col(mkt,"은 (SLV)")), 1,2, "은(SLV)", COLOR["silver"], dash="dot")

# ③ 장단기 스프레드
if not term_spread.empty:
    ts = term_spread.dropna()
    colors = [COLOR["red"] if v < 0 else COLOR["green"] for v in ts.values]
    fig.add_trace(go.Bar(
        x=ts.index, y=ts.values, name="스프레드",
        marker_color=colors, showlegend=False,
    ), row=2, col=1)
    fig.add_hline(y=0, line_dash="dash", line_color=COLOR["subtext"], row=2, col=1)

# ④ 신흥국 통화 정규화 (환율 상승 = 신흥국 통화 약세)
em_currencies = {
    "KRW": "한국 원화 (USD/KRW)",
    "TRY": "터키 리라 (USD/TRY)",
    "ZAR": "남아공 랜드 (USD/ZAR)",
    "BRL": "브라질 헤알 (USD/BRL)",
    "INR": "인도 루피 (USD/INR)",
}
cur_colors = [COLOR["blue"], COLOR["red"], COLOR["orange"], COLOR["green"], COLOR["purple"]]
for (label, col_name), c in zip(em_currencies.items(), cur_colors):
    add_line(fig, normalize(safe_col(mkt, col_name)), 2, 2, label, c)

# ⑤ EMB & HYG
add_line(fig, safe_col(mkt,"신흥국 채권 ETF (EMB)"), 3,1, "EMB", COLOR["orange"])
add_line(fig, safe_col(mkt,"미국 하이일드 (HYG)"),   3,1, "HYG", COLOR["red"], dash="dot")

# ⑥ WTI
add_line(fig, safe_col(mkt,"WTI 원유"), 3,2, "WTI", COLOR["orange"])

# ⑦ S&P500 vs EEM
add_line(fig, normalize(safe_col(mkt,"S&P500")),         4,1, "S&P500", COLOR["blue"])
add_line(fig, normalize(safe_col(mkt,"신흥국 주식 (EEM)")), 4,1, "EEM",    COLOR["green"], dash="dot")

# ⑧ 금/은 비율
if not gold_silver_ratio.empty:
    add_line(fig, gold_silver_ratio, 4,2, "금/은 비율", COLOR["gold"])
    fig.add_hline(y=80, line_dash="dash", line_color=COLOR["red"],
                  annotation_text="위험선 80", row=4, col=2)

# ── 레이아웃 ──────────────────────────────
fig.update_layout(
    title=dict(
        text="🌐 매크로 스트레스 대시보드  |  스태그플레이션 & 달러 사이클 모니터링",
        font=dict(size=18, color=COLOR["text"]),
        x=0.5,
    ),
    paper_bgcolor=COLOR["bg"],
    plot_bgcolor=COLOR["panel"],
    font=dict(color=COLOR["text"], size=11),
    legend=dict(
        bgcolor=COLOR["panel"],
        bordercolor=COLOR["border"],
        borderwidth=1,
        font=dict(size=10),
    ),
    height=1100,
    margin=dict(l=50, r=50, t=80, b=40),
)

for i in fig.layout.annotations:
    i.font.size = 12
    i.font.color = COLOR["subtext"]

fig.update_xaxes(gridcolor=COLOR["border"], showgrid=True, zeroline=False)
fig.update_yaxes(gridcolor=COLOR["border"], showgrid=True, zeroline=False)

output_path = "macro_dashboard.html"
fig.write_html(output_path)
print(f"\n✅ 대시보드 저장 완료: {output_path}")
print("   브라우저에서 열어 확인하세요.")

# ─────────────────────────────────────────
# 5. 피벗 타이밍 체크리스트 출력
# ─────────────────────────────────────────

print("\n" + "=" * 55)
print("  🎯 연준 피벗 타이밍 체크리스트")
print("=" * 55)
checklist = [
    ("DXY 급등 후 고점 형성",              "DXY 104 돌파 후 하락 반전 확인"),
    ("신흥국 외환보유고 급감",              "FRED: TRESEGUSM052N 시리즈 모니터"),
    ("EMBI 스프레드 급등",                 "EMB ETF 가격 급락 = 스프레드 확대"),
    ("연준 의사록 '금융안정' 언급 증가",    "FOMC 의사록 키워드 서치"),
    ("미국 신용 스프레드 확대",             "HYG 급락 / IG 스프레드 확대"),
    ("장단기 역전 해소 (재역전 아닌 정상화)","10Y-2Y 역전 해소 시 침체 진입 신호"),
]
for i, (title, detail) in enumerate(checklist, 1):
    print(f"  [{i}] {title}")
    print(f"      → {detail}\n")


# ─────────────────────────────────────────
# 6. 텔레그램 알림 전송
# ─────────────────────────────────────────

def send_telegram_message(text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️  텔레그램 설정 미완료 → 스킵")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                              "parse_mode": "HTML"}, timeout=10)


def send_telegram_document(file_path: str, caption: str = ""):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    with open(file_path, "rb") as f:
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
                      files={"document": f}, timeout=30)


# 시그널 요약 메시지 구성
today_str = datetime.today().strftime("%Y-%m-%d")
danger_count = sig_df["신호"].str.contains("위험").sum()
normal_count = sig_df["신호"].str.contains("정상").sum()

lines = [f"<b>🌐 매크로 스트레스 대시보드 | {today_str}</b>",
         f"🔴 위험 {danger_count}개  🟢 정상 {normal_count}개\n"]
for _, row in sig_df.iterrows():
    lines.append(f"{row['신호']}  <b>{row['지표']}</b>  현재: {row['현재값']} (임계: {row['임계값']})")

lines += [
    "",
    "<b>🎯 연준 피벗 체크리스트</b>",
]
for i, (title, detail) in enumerate(checklist, 1):
    lines.append(f"[{i}] {title}\n    → {detail}")

message = "\n".join(lines)
print("\n📨 텔레그램 전송 중...")
send_telegram_message(message)
send_telegram_document(output_path, caption=f"📊 매크로 대시보드 HTML | {today_str}")
print("✅ 텔레그램 전송 완료")
