#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════╗
║         🏦  미국 달러 유동성 종합 모니터  v2.0                        ║
║                                                                      ║
║  수집 지표 (총 13개 시리즈 + 3개 파생):                                ║
║    ① Fed 대차대조표 · 국채 · MBS · RRP · 은행 지준금                   ║
║    ② TGA (Treasury General Account)                                  ║
║    ③ SOFR · IORB · 기준금리 · M2                                     ║
║    ④ HY/IG 크레딧 스프레드 · VIX · 달러 지수                           ║
║    ⑤ 파생: 넷 유동성 · SOFR-IORB 스프레드 · M2 전년비                  ║
║                                                                      ║
║  출력:  HTML 인터랙티브 대시보드 + 콘솔 진단 리포트 + 텔레그램 알림       ║
╚══════════════════════════════════════════════════════════════════════╝

필수 설치:
    pip install fredapi plotly requests pandas numpy

──────────────────────────────────────────────────────────────────────
환경변수 설정 (GitHub Actions Secrets 또는 로컬 .env)
──────────────────────────────────────────────────────────────────────
  FRED_API_KEY      : fred.stlouisfed.org 에서 무료 발급
  TELEGRAM_TOKEN    : @BotFather 에서 봇 생성 후 발급
  TELEGRAM_CHAT_ID  : @userinfobot 에서 본인 chat_id 확인

GitHub Actions 설정 예시 (.github/workflows/liquidity.yml):
──────────────────────────────────────────────────────────────────────
  name: Liquidity Monitor
  on:
    schedule:
      - cron: '0 22 * * 1-5'   # 한국시간 매일 오전 7시 (UTC 22:00 전일)
    workflow_dispatch:           # 수동 실행 버튼

  jobs:
    run:
      runs-on: ubuntu-latest
      steps:
        - uses: actions/checkout@v4
        - uses: actions/setup-python@v5
          with:
            python-version: '3.11'
        - run: pip install fredapi plotly requests pandas numpy
        - run: python liquidity_monitor.py
          env:
            FRED_API_KEY:     ${{ secrets.FRED_API_KEY }}
            TELEGRAM_TOKEN:   ${{ secrets.TELEGRAM_TOKEN }}
            TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
        - uses: actions/upload-artifact@v4
          with:
            name: dashboard
            path: liquidity_dashboard.html
──────────────────────────────────────────────────────────────────────
"""

import os
import sys
import time
import warnings
warnings.filterwarnings('ignore')
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import numpy as np
import requests

# ── 의존성 확인 ─────────────────────────────────────────────────────────
_missing = []
for _pkg in ['fredapi', 'plotly']:
    try:
        __import__(_pkg)
    except ImportError:
        _missing.append(_pkg)
if _missing:
    print(f"❌ 미설치 패키지: {', '.join(_missing)}")
    print(f"   실행: pip install {' '.join(_missing)}")
    sys.exit(1)

from fredapi import Fred
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio


# ══════════════════════════════════════════════════════════════════════
#  ★ CONFIG  ─  환경변수에서 읽어옵니다 (GitHub Actions Secrets 연동)
# ══════════════════════════════════════════════════════════════════════
FRED_API_KEY     = os.environ.get("FRED_API_KEY",     "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN",   "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── 로컬 실행 시 직접 입력 (GitHub Actions 에서는 Secrets 로 주입) ────
# 아래 세 줄은 .env 미사용 시에만 주석 해제하여 사용
# FRED_API_KEY     = "your_fred_api_key"
# TELEGRAM_TOKEN   = "123456789:AAxxxxxxxxxxxxxxxxxxxxxx"
# TELEGRAM_CHAT_ID = "987654321"

LOOKBACK_DAYS = 365 * 3               # 기본 3년 (최대 10년 가능)
OUTPUT_HTML   = "liquidity_dashboard.html"
AUTO_OPEN     = False                 # GitHub Actions 환경에서는 False 유지


# ══════════════════════════════════════════════════════════════════════
#  FRED 시리즈 매핑
#  형식: key → (series_id,  단위환산_제수,  표시명)
#  단위: WALCL / TREAST / WSHOMCB는 FRED에서 million$ → /1000 = billion$
# ══════════════════════════════════════════════════════════════════════
FRED_MAP: dict[str, tuple] = {
    # ① Fed 직접 공급
    "BS":       ("WALCL",        1_000, "Fed 총자산 ($B)"),
    "UST":      ("TREAST",       1_000, "Fed 국채 보유 ($B)"),
    "MBS":      ("WSHOMCB",      1_000, "Fed MBS 보유 ($B)"),
    "RRP":      ("RRPONTSYD",        1, "RRP 잔고 ($B)"),
    "RESERVES": ("WRBWFRBL",         1, "은행 지준금 ($B)"),
    # ② 통화 / 금리
    "M2":       ("M2SL",             1, "M2 ($B)"),
    "SOFR":     ("SOFR",             1, "SOFR (%)"),
    "IORB":     ("IORB",             1, "IORB (%)"),
    "FFR":      ("DFF",              1, "기준금리 (%)"),
    # ── 수익률 곡선 (합성 유동성 핵심 지표) ──────────────────────────────
    "DGS2":     ("DGS2",             1, "2년물 국채금리 (%)"),
    "DGS10":    ("DGS10",            1, "10년물 국채금리 (%)"),
    "T10Y2Y":   ("T10Y2Y",           1, "수익률 곡선 10Y-2Y (%)"),
    # ③ 시장 스트레스
    "HY_OAS":   ("BAMLH0A0HYM2",     1, "HY 스프레드 (%)"),
    "IG_OAS":   ("BAMLC0A0CM",       1, "IG 스프레드 (%)"),
    "VIX":      ("VIXCLS",           1, "VIX"),
    # ④ 달러 / 글로벌
    "DXY":      ("DTWEXBGS",         1, "달러 지수 (Broad)"),
    "SWPT":     ("SWPT",             1, "Fed 통화스왑라인 ($B)"),
    # ⑤ 신용 공급 (은행 대출 충동)
    "TOTCI":    ("TOTCI",            1, "상업은행 대출 ($B)"),
}


# ══════════════════════════════════════════════════════════════════════
#  데이터 수집
# ══════════════════════════════════════════════════════════════════════
class DataFetcher:
    """FRED + Treasury Fiscal Data API에서 데이터를 수집합니다."""

    def __init__(self, api_key: str, lookback: int):
        self.fred  = Fred(api_key=api_key)
        self.start = (datetime.now() - timedelta(days=lookback)).strftime("%Y-%m-%d")
        self.raw: dict[str, pd.Series] = {}

    # ── FRED ─────────────────────────────────────────────────────────
    def _fred(self, key: str, sid: str, div: float, name: str,
              retries: int = 3) -> None:
        """재시도(최대 3회) + 호출 간격 0.4 s 으로 레이트 리밋 회피."""
        for attempt in range(retries):
            try:
                s = self.fred.get_series(sid, observation_start=self.start)
                if s is None or s.dropna().empty:
                    raise ValueError("빈 시리즈 반환")
                self.raw[key] = (s / div).rename(name)
                print(f"  ✅  {name:<34}  FRED: {sid}")
                time.sleep(0.4)          # API 레이트 리밋 회피
                return
            except Exception as e:
                wait = 2 ** attempt      # 1 s → 2 s → 4 s
                if attempt < retries - 1:
                    print(f"  ↺   {sid}  재시도 {attempt+1}/{retries} "
                          f"({wait}s 대기)…  ({e})")
                    time.sleep(wait)
                else:
                    print(f"  ❌  {name:<34}  최종 실패 → {e}")

    # ── Treasury TGA ──────────────────────────────────────────────────
    def _tga(self, retries: int = 3) -> None:
        """
        Treasury Fiscal Data API에서 TGA (Federal Reserve Account 잔고) 수집.
        account_type 이름이 바뀌는 경우를 대비해 부분 문자열 매칭 사용.
        """
        url = (
            "https://api.fiscaldata.treasury.gov/services/api/v1"
            "/accounting/dts/operating_cash_balance"
        )
        params = {
            "fields":      "record_date,open_today_bal,account_type",
            "filter":      f"record_date:gte:{self.start}",
            "sort":        "record_date",
            "page[size]":  10_000,
        }

        for attempt in range(retries):
            try:
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                rows = resp.json().get("data", [])

                # account_type 에 'Federal Reserve' 포함 여부로 필터
                rows = [r for r in rows
                        if "Federal Reserve" in str(r.get("account_type", ""))]
                if not rows:
                    raise ValueError(
                        f"Federal Reserve Account 행 없음 "
                        f"(전체 {len(resp.json().get('data',[]))}행 중)"
                    )

                df = pd.DataFrame(rows)
                df["record_date"]    = pd.to_datetime(df["record_date"])
                df["open_today_bal"] = (
                    df["open_today_bal"]
                    .astype(str).str.replace(",", "", regex=False)
                )
                df["open_today_bal"] = pd.to_numeric(
                    df["open_today_bal"], errors="coerce"
                )
                df = df.dropna(subset=["open_today_bal"])
                df = df.sort_values("record_date").set_index("record_date")
                if df.empty:
                    raise ValueError("숫자 변환 후 데이터 없음")

                self.raw["TGA"] = (df["open_today_bal"] / 1_000).rename("TGA 잔고 ($B)")
                print(f"  ✅  {'TGA 잔고 ($B)':<34}  Treasury Fiscal Data API "
                      f"({len(df)}행)")
                return

            except Exception as e:
                wait = 2 ** attempt
                if attempt < retries - 1:
                    print(f"  ↺   TGA 재시도 {attempt+1}/{retries} "
                          f"({wait}s 대기)…  ({e})")
                    time.sleep(wait)
                else:
                    print(f"  ❌  {'TGA 잔고':<34}  최종 실패 → {e}")

    # ── 전체 수집 ────────────────────────────────────────────────────
    def fetch(self) -> dict:
        print("\n  FRED 시리즈 수집 중...\n")
        for key, (sid, div, name) in FRED_MAP.items():
            self._fred(key, sid, div, name)
        print()
        self._tga()
        n = len(self.raw)
        print(f"\n  → 총 {n}개 시리즈 수집 완료 (목표 {len(FRED_MAP)+1}개)\n")
        return self.raw


# ══════════════════════════════════════════════════════════════════════
#  파생 지표 계산
# ══════════════════════════════════════════════════════════════════════
def compute_derived(raw: dict) -> dict:
    """
    파생 지표를 계산합니다.
      ① 넷 유동성   = BS − RRP − TGA           (월가 표준 유동성 척도)
      ② SOFR-IORB  = (SOFR − IORB) × 100 bps  (레포 시장 스트레스)
      ③ M2 전년비   = M2 YoY 증가율 (%)
      ④ 은행대출 전년비 = TOTCI YoY 증가율 (%)   (신용 공급 충동)
    """
    der: dict[str, pd.Series] = {}

    # ① 넷 유동성
    if all(k in raw for k in ["BS", "RRP", "TGA"]):
        start = max(raw[k].dropna().index.min() for k in ["BS", "RRP", "TGA"])
        idx   = pd.date_range(start=start, end=pd.Timestamp.now(), freq="B")
        bs    = raw["BS"].reindex(idx).ffill()
        rrp   = raw["RRP"].reindex(idx).ffill()
        tga   = raw["TGA"].reindex(idx).ffill()
        der["NET_LIQ"] = (bs - rrp - tga).dropna().rename("넷 유동성 ($B)")

    # ② SOFR − IORB (bps)
    if "SOFR" in raw and "IORB" in raw:
        idx  = raw["SOFR"].index.union(raw["IORB"].index)
        sofr = raw["SOFR"].reindex(idx).ffill()
        iorb = raw["IORB"].reindex(idx).ffill()
        der["SOFR_SPR"] = ((sofr - iorb) * 100).dropna().rename("SOFR−IORB (bps)")

    # ③ M2 전년비 (%)
    if "M2" in raw:
        try:
            m2m = raw["M2"].resample("ME").last()
        except Exception:
            m2m = raw["M2"].resample("M").last()
        der["M2_YOY"] = m2m.pct_change(12).mul(100).dropna().rename("M2 전년비 (%)")

    # ④ 상업은행 대출 전년비 (%)
    if "TOTCI" in raw:
        try:
            ci = raw["TOTCI"].resample("ME").last()
        except Exception:
            ci = raw["TOTCI"].resample("M").last()
        der["BANK_YOY"] = ci.pct_change(12).mul(100).dropna().rename("은행 대출 전년비 (%)")

    return der


# ══════════════════════════════════════════════════════════════════════
#  진단 결과 데이터 클래스
# ══════════════════════════════════════════════════════════════════════
class DiagRes:
    __slots__ = ("name", "val_str", "status", "score", "summary", "detail")

    def __init__(self, name: str, val_str: str, status: str,
                 score: int, summary: str, detail: str):
        self.name    = name
        self.val_str = val_str
        self.status  = status   # 'GREEN' | 'YELLOW' | 'RED'
        self.score   = score    # 0~10  (10 = 완전 완화)
        self.summary = summary
        self.detail  = detail


# ══════════════════════════════════════════════════════════════════════
#  진단 엔진  ─  8개 지표별 임계값 기반 상태 판정
# ══════════════════════════════════════════════════════════════════════
class DiagEngine:
    """
    유동성 진단 임계값 기준표
    ─────────────────────────────────────────────────────────────────
    지표            GREEN               YELLOW              RED
    넷 유동성       4주 +2%↑           ±2% 범위           4주 -2%↓
    RRP 잔고        >$500B             $100~500B           <$100B
    TGA 잔고        <$400B & 감소      중간                >$700B & 증가
    은행 지준금     >$3,000B           $2,500~3,000B       <$2,500B
    SOFR-IORB       ≤5bps              5~15bps             >15bps
    HY 스프레드     <3.5%              3.5~5.5%            >5.5%
    VIX             <18                18~30               >30
    달러 지수       4주 -2%↓           ±2% 범위           4주 +2%↑
    ─────────────────────────────────────────────────────────────────
    """

    def __init__(self, raw: dict, der: dict):
        self.R   = raw
        self.D   = der
        self.out: list[DiagRes] = []

    def _last(self, key: str, src: str = "R") -> Optional[float]:
        d = self.R if src == "R" else self.D
        s = d.get(key, pd.Series(dtype=float)).dropna()
        return float(s.iloc[-1]) if not s.empty else None

    def _chg(self, key: str, periods: int = 20, src: str = "R") -> Optional[float]:
        """최근 N 거래일 대비 % 변화 (기본값 = 약 4주)"""
        d = self.R if src == "R" else self.D
        s = d.get(key, pd.Series(dtype=float)).dropna()
        if len(s) < periods + 1:
            return None
        return float((s.iloc[-1] / s.iloc[-1 - periods] - 1) * 100)

    def _push(self, name: str, val_str: str, status: str,
              score: int, summary: str, detail: str) -> None:
        self.out.append(DiagRes(name, val_str, status, score, summary, detail))

    # ── 개별 진단 메서드 ───────────────────────────────────────────────

    def dx_net_liq(self) -> None:
        v = self._last("NET_LIQ", "D")
        c = self._chg("NET_LIQ", src="D") or 0.0
        if v is None:
            return
        if c > 2:
            st, sc = "GREEN",  9
            note   = f"4주 {c:+.1f}% 증가. 시중 달러 공급 확대 → 위험자산(주식·금·크레딧) 보유 우호."
        elif c > -2:
            st, sc = "YELLOW", 6
            note   = f"4주 {c:+.1f}% 변화. 현상 유지. 추세 전환 여부 점검."
        else:
            st, sc = "RED",    2
            note   = f"4주 {c:+.1f}% 감소. 달러 유동성 수축 국면 → 방어 포지션 전환 권고."
        self._push("넷 유동성 (BS − RRP − TGA)", f"${v:,.0f}B",
                   st, sc, f"${v:,.0f}B | 4주 변화 {c:+.1f}%", note)

    def dx_rrp(self) -> None:
        v = self._last("RRP")
        if v is None:
            return
        if v > 500:
            st, sc = "GREEN",  8
            note   = "MMF 완충재 풍부. QT 지속에도 지준금 감소 속도 완충 가능. 레포 경색 위험 낮음."
        elif v > 100:
            st, sc = "YELLOW", 5
            note   = "완충재 소진 진행 중. QT 계속 시 향후 지준금 하락 속도 빨라질 수 있음."
        else:
            st, sc = "RED",    2
            note   = "⚠️ 완충재 거의 소진! 2019년 9월 레포 경색 재현 위험 증가. 연준 QT 중단 검토 시점."
        self._push("RRP 잔고", f"${v:,.0f}B", st, sc, f"${v:,.0f}B", note)

    def dx_tga(self) -> None:
        v = self._last("TGA")
        c = self._chg("TGA") or 0.0
        if v is None:
            return
        if v < 400 and c < 0:
            st, sc = "GREEN",  8
            note   = "낮은 잔고·감소 추세. 재무부 지출로 시중에 달러 방출 중. 유동성 확장 효과."
        elif v > 700 and c > 0:
            st, sc = "RED",    2
            note   = "⚠️ 잔고 급증. 재무부가 시중 유동성 흡수 중. 부채한도 해제 후 현금 재축적 패턴."
        else:
            st, sc = "YELLOW", 6
            note   = "중립 수준. 부채한도 협상 일정 및 분기 환급 공고(QRA) 주시 권고."
        self._push("TGA 잔고", f"${v:,.0f}B",
                   st, sc, f"${v:,.0f}B | 4주 변화 {c:+.1f}%", note)

    def dx_reserves(self) -> None:
        v = self._last("RESERVES")
        if v is None:
            return
        if v > 3_000:
            st, sc = "GREEN",  9
            note   = "과잉 지준 상태. 인터뱅크 금리 안정. 레포 경색 위험 없음."
        elif v > 2_500:
            st, sc = "YELLOW", 6
            note   = "적정 수준이나 QT 지속 시 감소 속도 모니터링 필요. SOFR 스프레드와 함께 관찰."
        else:
            st, sc = "RED",    2
            note   = "⚠️ 희소 구간 진입! SOFR 급등·레포 경색 위험. 연준 QT 중단 또는 레포 창구 확대 압박."
        self._push("은행 지준금", f"${v:,.0f}B", st, sc, f"${v:,.0f}B", note)

    def dx_sofr(self) -> None:
        v = self._last("SOFR_SPR", "D")
        if v is None:
            return
        if v <= 5:
            st, sc = "GREEN",  9
            note   = "레포 시장 완전 안정. 지준금 풍부 신호."
        elif v <= 15:
            st, sc = "YELLOW", 6
            note   = "소폭 상승. 은행 단기 자금 조달 비용 증가 초기 신호. 추이 지속 모니터링."
        else:
            st, sc = "RED",    2
            note   = "⚠️ 레포 경색 신호! 2019년 9월 재현 가능성. 연준 즉각 개입(레포 오퍼·RRP 축소) 필요 수준."
        self._push("SOFR − IORB 스프레드", f"{v:.1f}bps", st, sc, f"{v:.1f}bps", note)

    def dx_hy(self) -> None:
        v = self._last("HY_OAS")
        if v is None:
            return
        if v < 3.5:
            st, sc = "GREEN",  9
            note   = "역사적으로 좁은 수준. 신용시장 유동성 풍부. 위험 선호 환경."
        elif v < 5.5:
            st, sc = "YELLOW", 5
            note   = "정상 범위. 경기 둔화 우려 일부 반영. 기업 자금 조달 환경 중립."
        else:
            st, sc = "RED",    2
            note   = "⚠️ 신용 경색 신호. 기업 자금 조달 비용 급등. 하이일드 채권 디폴트 우려 고조."
        self._push("HY 크레딧 스프레드", f"{v:.2f}%", st, sc, f"{v:.2f}%", note)

    def dx_vix(self) -> None:
        v = self._last("VIX")
        if v is None:
            return
        if v < 18:
            st, sc = "GREEN",  9
            note   = "저변동성 구간. 시장 공포 낮음. 유동성 환경 안정. 위험자산 매수 우호."
        elif v < 30:
            st, sc = "YELLOW", 5
            note   = "중간 변동성. 불확실성 상존. 방어적 포지션 일부 고려."
        else:
            st, sc = "RED",    2
            note   = "⚠️ 고변동성. 공포지수 급등. 유동성 공급에도 불구하고 위험 회피 심화."
        self._push("VIX", f"{v:.1f}", st, sc, f"{v:.1f}", note)

    def dx_dxy(self) -> None:
        v = self._last("DXY")
        c = self._chg("DXY") or 0.0
        if v is None:
            return
        if c < -2:
            st, sc = "GREEN",  8
            note   = f"달러 약세({c:+.1f}%/4주). 글로벌 달러 유동성 완화. 신흥국·원자재 우호. 역외 조달 비용 감소."
        elif c > 2:
            st, sc = "RED",    3
            note   = f"달러 강세({c:+.1f}%/4주). 글로벌 달러 유동성 긴축. 신흥국 달러 부채 상환 부담 증가."
        else:
            st, sc = "YELLOW", 6
            note   = f"달러 횡보({c:+.1f}%/4주). 글로벌 달러 유동성 중립 환경."
        self._push("달러 지수 (Broad DXY)", f"{v:.1f}",
                   st, sc, f"{v:.1f} | 4주 {c:+.1f}%", note)

    # ══ 추가 진단 메서드 (6개) ══════════════════════════════════════════

    def dx_yield_curve(self) -> None:
        """
        수익률 곡선 (10Y − 2Y 스프레드)
        ─────────────────────────────────────────────────────────────
        > 0%    : 정상 (장기금리 > 단기금리) → 경기 확장 기대
        -0.5~0% : 평탄화 진행 → 경기 둔화 예고
        < -0.5% : 역전 → 역사적으로 12~18개월 후 침체
        막 정상화 중 : 역전 해소 시점 = 실제 침체 임박 신호
        ─────────────────────────────────────────────────────────────
        """
        v = self._last("T10Y2Y")
        if v is None:
            return
        # 4주 전 값으로 역전 해소 방향 감지
        c = self._chg("T10Y2Y") or 0.0
        if v > 0:
            st, sc = "GREEN",  8
            note   = f"정상 곡선({v:+.2f}%). 경기 확장 기대. 은행 NIM 개선 → 신용 공급 우호."
        elif v > -0.5:
            st, sc = "YELLOW", 5
            note   = (f"평탄화 구간({v:+.2f}%). 경기 둔화 우려. "
                      f"4주 변화 {c:+.2f}%p → "
                      f"{'역전 해소 중(침체 임박 신호 주의)' if c > 0.3 else '역전 심화 주의'}.")
        else:
            if c > 0.3:  # 역전 해소 방향 = 침체 임박 고경보
                st, sc = "RED", 1
                note   = (f"⚠️ 역전 해소 진행 중({v:+.2f}%, 4주 {c:+.2f}%p). "
                          "역사적으로 역전 해소 후 1~6개월 내 침체 시작. 최고 경보.")
            else:
                st, sc = "RED", 2
                note   = (f"⚠️ 수익률 곡선 역전({v:+.2f}%). "
                          "과거 8번 중 7번 침체 선행. 유동성 긴축 환경 지속.")
        self._push("수익률 곡선 (10Y − 2Y)", f"{v:+.2f}%",
                   st, sc, f"{v:+.2f}% | 4주 {c:+.2f}%p 변화", note)

    def dx_2y_yield(self) -> None:
        """
        2년물 국채금리 (Fed 피벗 선행지표)
        ─────────────────────────────────────────────────────────────
        2년물은 시장의 향후 2년간 기준금리 기대를 반영.
        FFR 대비 2년물이 크게 낮아지면 시장이 인하를 선반영 중.
        ─────────────────────────────────────────────────────────────
        """
        v2 = self._last("DGS2")
        vf = self._last("FFR")
        if v2 is None or vf is None:
            return
        spread = v2 - vf   # 2Y − FFR 스프레드 (음수 = 시장이 인하 선반영)
        c      = self._chg("DGS2") or 0.0
        if spread < -0.5:
            st, sc = "GREEN",  8
            note   = (f"2년물({v2:.2f}%) ≪ 기준금리({vf:.2f}%), 차이 {spread:+.2f}%p. "
                      "시장이 상당폭 금리 인하를 선반영 중. 향후 유동성 완화 기대.")
        elif spread < 0:
            st, sc = "YELLOW", 6
            note   = (f"2년물({v2:.2f}%) < 기준금리({vf:.2f}%), 차이 {spread:+.2f}%p. "
                      "소폭 인하 선반영. CME FedWatch 함께 점검.")
        else:
            st, sc = "RED",    3
            note   = (f"2년물({v2:.2f}%) ≥ 기준금리({vf:.2f}%). "
                      "시장이 추가 인상 또는 동결 장기화를 예상 중. 유동성 긴축 지속 신호.")
        self._push("2년물 국채금리 (Fed 피벗 신호)", f"{v2:.2f}%",
                   st, sc, f"{v2:.2f}% | 2Y-FFR {spread:+.2f}%p", note)

    def dx_m2(self) -> None:
        """
        M2 전년비 증가율
        ─────────────────────────────────────────────────────────────
        M2 > +6% : 역사적 통화 완화 (1970~2000 평균 ~7%)
        M2 0~6%  : 중립 (2010년대 평균 ~5%)
        M2 < 0%  : 통화 수축 → 디플레이션 위험 (2022~2023 경험)
        ─────────────────────────────────────────────────────────────
        """
        v = self._last("M2_YOY", "D")
        if v is None:
            return
        if v >= 6:
            st, sc = "GREEN",  8
            note   = (f"M2 전년비 +{v:.1f}%. 역사적 통화 완화 구간. "
                      "자산 가격 지지. 단, 6% 초과 시 인플레이션 재점화 위험 병행 주시.")
        elif v >= 0:
            st, sc = "YELLOW", 6
            note   = f"M2 전년비 +{v:.1f}%. 완만한 통화 증가. 중립 환경."
        else:
            st, sc = "RED",    2
            note   = (f"⚠️ M2 전년비 {v:.1f}%. 통화 수축! "
                      "2022~2023년 재현. 자산 가격 하락 압력. 연준 피벗 시점 모니터링.")
        self._push("M2 전년비 증가율", f"{v:.1f}%", st, sc, f"{v:.1f}%", note)

    def dx_ig(self) -> None:
        """
        IG 크레딧 스프레드 (투자등급 회사채)
        ─────────────────────────────────────────────────────────────
        IG 스프레드는 HY보다 안정적이나 급등 시 기업 전반의
        자금 조달 비용 상승을 의미. 100bps(1%) 돌파 시 경보.
        ─────────────────────────────────────────────────────────────
        """
        v = self._last("IG_OAS")
        if v is None:
            return
        if v < 0.9:
            st, sc = "GREEN",  9
            note   = f"IG 스프레드 {v:.2f}%. 역사적 저점 수준. 기업 자금 조달 환경 최우호."
        elif v < 1.5:
            st, sc = "YELLOW", 6
            note   = (f"IG 스프레드 {v:.2f}%. 정상 범위. "
                      "경기 불확실성 일부 반영. HY 스프레드 동반 확대 여부 병행 관찰.")
        else:
            st, sc = "RED",    2
            note   = (f"⚠️ IG 스프레드 {v:.2f}%. 신용 경색 신호. "
                      "투자등급 기업까지 자금 조달 비용 급등. 시스템 리스크 단계 진입.")
        self._push("IG 크레딧 스프레드 (투자등급)", f"{v:.2f}%",
                   st, sc, f"{v:.2f}%", note)

    def dx_bank_lending(self) -> None:
        """
        상업은행 대출 전년비 (신용 공급 충동)
        ─────────────────────────────────────────────────────────────
        은행 대출 증가율은 실물 경제에 직접 공급되는 민간 신용량.
        > +6%  : 신용 완화·경기 부양
        0~6%   : 중립
        < 0%   : 신용 수축 → 금융 긴축 전달 확인
        ─────────────────────────────────────────────────────────────
        """
        v = self._last("BANK_YOY", "D")
        if v is None:
            return
        if v >= 6:
            st, sc = "GREEN",  8
            note   = f"은행 대출 전년비 +{v:.1f}%. 신용 공급 확대. 실물 경기 부양 효과 확인."
        elif v >= 0:
            st, sc = "YELLOW", 6
            note   = f"은행 대출 전년비 +{v:.1f}%. 완만한 성장. 중립 환경."
        else:
            st, sc = "RED",    2
            note   = (f"⚠️ 은행 대출 전년비 {v:.1f}%. 신용 수축! "
                      "금리 인상 효과가 실물에 전달 중. 기업·가계 자금 조달 위축.")
        self._push("상업은행 대출 전년비", f"{v:.1f}%",
                   st, sc, f"{v:.1f}%", note)

    def dx_swap_lines(self) -> None:
        """
        Fed 통화스왑라인 잔고 (글로벌 달러 수요)
        ─────────────────────────────────────────────────────────────
        ECB·BOJ·BOE 등 주요 중앙은행에 달러를 직접 공급하는 창구.
        잔고 급증 = 글로벌 달러 부족 신호 (2008·2020년 급등 사례).
        평상시 거의 0 → 급등 시 즉각 경보.
        ─────────────────────────────────────────────────────────────
        """
        v = self._last("SWPT")
        if v is None:
            return
        if v < 10:
            st, sc = "GREEN",  9
            note   = f"스왑라인 잔고 ${v:.0f}B. 정상 수준. 글로벌 달러 부족 없음."
        elif v < 100:
            st, sc = "YELLOW", 5
            note   = (f"스왑라인 잔고 ${v:.0f}B. 소폭 증가. "
                      "특정 지역 달러 조달 압박 초기 신호. 원인 파악 필요.")
        else:
            st, sc = "RED",    1
            note   = (f"⚠️ 스왑라인 잔고 ${v:.0f}B. 글로벌 달러 위기 수준! "
                      "2020년 3월($449B) 수준 접근. 즉각 안전자산(달러·금·단기채) 대피.")
        self._push("Fed 통화스왑라인 잔고", f"${v:.0f}B",
                   st, sc, f"${v:.0f}B", note)

    # ── 종합 판정 ──────────────────────────────────────────────────────
    def run(self) -> tuple:
        """모든 진단을 실행하고 (results, avg_score, verdict, detail)을 반환합니다."""
        ALL_DX = [
            # ─ Layer 1: Fed 직접 공급 ─────────────────
            self.dx_net_liq,       # 넷 유동성 (BS−RRP−TGA)
            self.dx_rrp,           # RRP 잔고
            self.dx_reserves,      # 은행 지준금
            self.dx_sofr,          # SOFR−IORB 레포 스프레드
            # ─ Layer 2: Treasury ─────────────────────
            self.dx_tga,           # TGA 잔고
            # ─ Layer 3: 합성 유동성 ──────────────────
            self.dx_yield_curve,   # 수익률 곡선 10Y−2Y  ★신규
            self.dx_2y_yield,      # 2년물 금리 (Fed 피벗 신호) ★신규
            self.dx_m2,            # M2 전년비  ★신규
            # ─ Layer 4: 시장 스트레스 ─────────────────
            self.dx_hy,            # HY 크레딧 스프레드
            self.dx_ig,            # IG 크레딧 스프레드  ★신규
            self.dx_vix,           # VIX
            # ─ Layer 5: 글로벌 달러 ──────────────────
            self.dx_dxy,           # 달러 지수
            self.dx_swap_lines,    # Fed 통화스왑라인  ★신규
            # ─ 신용 공급 충동 ─────────────────────────
            self.dx_bank_lending,  # 상업은행 대출 전년비  ★신규
        ]
        for fn in ALL_DX:
            try:
                fn()
            except Exception as e:
                print(f"  진단 오류 ({fn.__name__}): {e}")

        scores = [r.score for r in self.out]
        avg    = int(np.mean(scores)) if scores else 5
        n      = len(self.out)
        g      = sum(1 for r in self.out if r.status == "GREEN")
        y      = sum(1 for r in self.out if r.status == "YELLOW")
        rd     = sum(1 for r in self.out if r.status == "RED")

        if avg >= 7:
            verdict = f"🟢 완화적 (Accommodative)  ·  종합 {avg}/10"
            detail  = (f"{n}개 지표 → 🟢{g} 🟡{y} 🔴{rd} "
                       "| 달러 유동성 풍부. 위험자산(주식·크레딧·금) 보유 우호 환경.")
        elif avg >= 5:
            verdict = f"🟡 중립 (Neutral)  ·  종합 {avg}/10"
            detail  = (f"{n}개 지표 → 🟢{g} 🟡{y} 🔴{rd} "
                       "| 혼재 환경. 개별 지표 추세 점검 후 포지션 조절 권고.")
        else:
            verdict = f"🔴 긴축적 (Tightening)  ·  종합 {avg}/10"
            detail  = (f"{n}개 지표 → 🟢{g} 🟡{y} 🔴{rd} "
                       "| 달러 유동성 긴축. 방어적 전략·현금 비중 확대 권고.")

        return self.out, avg, verdict, detail


# ══════════════════════════════════════════════════════════════════════
#  대시보드 ─ Plotly 인터랙티브 HTML
# ══════════════════════════════════════════════════════════════════════

# ── 색상 ─────────────────────────────────────────────────────────────
C_BLU  = "#58A6FF"   # 파란 계열 (넷 유동성)
C_GRN  = "#56D364"   # 초록 (BS Total, FFR)
C_RED  = "#F78166"   # 붉은 계열 (RRP, HY)
C_ORG  = "#E3B341"   # 주황 (TGA, IG, DXY)
C_PRP  = "#BC8CFF"   # 보라 (지준금)
C_CYN  = "#79C0FF"   # 청록 (국채)
C_MUT  = "#8B949E"   # 회색 (보조)
C_WHT  = "#E6EDF3"   # 밝은 흰색 (제목)
S_GRN  = "#2EA043"   # 상태: 초록
S_YLW  = "#D29922"   # 상태: 노랑
S_RED  = "#DA3633"   # 상태: 빨강


def _rgba(hex_c: str, a: float = 0.13) -> str:
    """hex → rgba 변환 (fillcolor용)"""
    h = hex_c.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{a})"


def _hline(fig: go.Figure, y: float, color: str, label: str,
           row: int, col: int, dash: str = "dot") -> None:
    """서브플롯에 수평 참조선 + 레이블 추가"""
    fig.add_hline(
        y=y,
        line=dict(color=color, width=0.9, dash=dash),
        annotation_text=label,
        annotation_font_size=9,
        annotation_font_color=color,
        row=row, col=col,
    )


def build_dashboard(
    raw: dict, der: dict,
    results: list, avg_score: int, verdict: str, detail: str,
    output: str, auto_open: bool,
) -> None:

    today = datetime.now().strftime("%Y년 %m월 %d일")

    # ── 서브플롯 레이아웃 (6행 × 3열 + 하단 테이블) ────────────────────
    #  Row 1: ① 넷 유동성      | ② Fed BS 구성      | ③ RRP 잔고
    #  Row 2: ④ TGA            | ⑤ 은행 지준금       | ⑥ SOFR-IORB
    #  Row 3: ⑦ HY/IG 스프레드 | ⑧ VIX              | ⑨ M2 전년비
    #  Row 4: ⑩ 달러 지수      | ⑪ 수익률 곡선       | ⑫ 2Y/10Y 금리
    #  Row 5: ⑬ 은행 대출 증가율| ⑭ Fed 스왑라인     | ⑮ 지표별 점수
    #  Row 6: ⑯ 종합 진단 테이블 (colspan=3)
    TITLES = [
        "① 넷 유동성 (BS − RRP − TGA,  $B)",
        "② Fed 대차대조표 구성  ($B)",
        "③ RRP 잔고  ($B)",
        "④ TGA 잔고  ($B)",
        "⑤ 은행 지준금  ($B)",
        "⑥ SOFR − IORB 스프레드  (bps)",
        "⑦ HY / IG 크레딧 스프레드  (%)",
        "⑧ VIX",
        "⑨ M2 전년비 증가율  (%)",
        "⑩ 달러 지수 (Broad DXY)",
        "⑪ 수익률 곡선 10Y − 2Y  (%)",
        "⑫ 2년물 / 10년물 국채금리  (%)",
        "⑬ 상업은행 대출 전년비  (%)",
        "⑭ Fed 통화스왑라인  ($B)",
        "⑮ 지표별 유동성 점수  (0~10)",
        "⑯ 종합 유동성 진단",
    ]

    fig = make_subplots(
        rows=6, cols=3,
        subplot_titles=TITLES,
        specs=[
            [{}, {}, {}],
            [{}, {}, {}],
            [{}, {}, {}],
            [{}, {}, {}],
            [{}, {}, {}],
            [{"colspan": 3, "type": "table"}, None, None],
        ],
        vertical_spacing=0.055,
        horizontal_spacing=0.055,
    )

    # ── 차트 헬퍼 ──────────────────────────────────────────────────────
    def S(key: str, src: str = "R") -> Optional[pd.Series]:
        """None-safe 시리즈 조회"""
        d = raw if src == "R" else der
        s = d.get(key, pd.Series(dtype=float)).dropna()
        return s if not s.empty else None

    # 데이터가 추가된 서브플롯 (row, col) 추적 → 빈 칸에 "데이터 없음" 표시
    _filled: set = set()

    def _no_data(row: int, col: int) -> None:
        """빈 서브플롯에 안내 텍스트 삽입"""
        mid = datetime.now() - timedelta(days=LOOKBACK_DAYS // 2)
        fig.add_trace(go.Scatter(
            x=[mid], y=[0],
            mode="text",
            text=["데이터 없음"],
            textfont=dict(color="#555555", size=13),
            showlegend=False,
            hoverinfo="skip",
        ), row=row, col=col)

    def area(row: int, col: int, s: Optional[pd.Series],
             color: str, name: str) -> None:
        """영역 채우기 라인 차트"""
        if s is None:
            return
        _filled.add((row, col))
        fig.add_trace(go.Scatter(
            x=s.index, y=s.values, name=name, mode="lines",
            line=dict(color=color, width=1.8),
            fill="tozeroy", fillcolor=_rgba(color),
            showlegend=False,
            hovertemplate=f"%{{x|%Y-%m-%d}}<br>{name}: %{{y:,.1f}}<extra></extra>",
        ), row=row, col=col)

    def ln(row: int, col: int, s: Optional[pd.Series],
           color: str, name: str, width: float = 1.6,
           dash: str = "solid", legend: bool = False,
           legend_group: str = "legend") -> None:
        """일반 라인 차트 (legend_group: 'legend' | 'legend2')"""
        if s is None:
            return
        _filled.add((row, col))
        # legend2 → Plotly 두 번째 독립 범례 그룹
        extra = {"legend": legend_group} if legend else {}
        fig.add_trace(go.Scatter(
            x=s.index, y=s.values, name=name, mode="lines",
            line=dict(color=color, width=width, dash=dash),
            showlegend=legend,
            hovertemplate=f"%{{x|%Y-%m-%d}}<br>{name}: %{{y:,.2f}}<extra></extra>",
            **extra,
        ), row=row, col=col)

    def bar(row: int, col: int, s: Optional[pd.Series],
            cfn, name: str) -> None:
        """컬러 맵핑 막대 차트"""
        if s is None:
            return
        _filled.add((row, col))
        fig.add_trace(go.Bar(
            x=s.index, y=s.values, name=name,
            marker_color=[cfn(v) for v in s.values],
            showlegend=False,
            hovertemplate=f"%{{x|%Y-%m-%d}}<br>{name}: %{{y:,.2f}}<extra></extra>",
        ), row=row, col=col)

    # ══════════════════════════════════════════════════════════════════
    #  Row 1 ─ 넷 유동성 | BS 구성 | RRP
    # ══════════════════════════════════════════════════════════════════
    area(1, 1, S("NET_LIQ", "D"), C_BLU, "넷 유동성")

    # ② BS 구성 ─ legend 그룹 'legend' (좌상단)
    ln(1, 2, S("BS"),  C_GRN, "BS Total",   legend=True,  legend_group="legend")
    ln(1, 2, S("UST"), C_CYN, "국채 (UST)", legend=True,  legend_group="legend")
    ln(1, 2, S("MBS"), C_BLU, "MBS",        legend=True,  legend_group="legend")

    area(1, 3, S("RRP"), C_RED, "RRP 잔고")
    _hline(fig, 500, S_YLW, "$500B 경계", 1, 3)
    _hline(fig, 100, S_RED,  "$100B 위험", 1, 3)

    # ══════════════════════════════════════════════════════════════════
    #  Row 2 ─ TGA | 지준금 | SOFR-IORB
    # ══════════════════════════════════════════════════════════════════
    area(2, 1, S("TGA"), C_ORG, "TGA 잔고")
    _hline(fig, 700, S_RED,  "$700B 경계",  2, 1)
    _hline(fig, 400, S_YLW, "$400B 기준",  2, 1)

    area(2, 2, S("RESERVES"), C_PRP, "은행 지준금")
    _hline(fig, 3_000, S_GRN, "$3T 안정",   2, 2)
    _hline(fig, 2_500, S_YLW, "$2.5T 경계", 2, 2)

    bar(2, 3, S("SOFR_SPR", "D"),
        lambda v: S_GRN if v <= 5 else (S_YLW if v <= 15 else S_RED),
        "SOFR−IORB")
    _hline(fig,  5, S_YLW, "5bps 경계",  2, 3)
    _hline(fig, 15, S_RED,  "15bps 경고", 2, 3)

    # ══════════════════════════════════════════════════════════════════
    #  Row 3 ─ HY/IG | VIX | M2 YoY
    # ══════════════════════════════════════════════════════════════════
    # ⑦ HY/IG ─ legend2 그룹 (BS 범례와 분리)
    ln(3, 1, S("HY_OAS"), C_RED, "HY OAS",
       width=2.2, legend=True, legend_group="legend2")
    ln(3, 1, S("IG_OAS"), C_ORG, "IG OAS",
       width=2.2, legend=True, legend_group="legend2")
    _hline(fig, 3.5, S_GRN, "완화선 3.5%", 3, 1)
    _hline(fig, 5.5, S_RED,  "위기선 5.5%", 3, 1)

    bar(3, 2, S("VIX"),
        lambda v: S_GRN if v < 18 else (S_YLW if v < 30 else S_RED),
        "VIX")
    _hline(fig, 18, S_YLW, "18 경계", 3, 2)
    _hline(fig, 30, S_RED,  "30 위기", 3, 2)

    bar(3, 3, S("M2_YOY", "D"),
        lambda v: S_GRN if v > 5 else (S_YLW if v > 0 else S_RED),
        "M2 YoY")
    _hline(fig, 0,  C_MUT, "0% 기준선", 3, 3, dash="solid")
    _hline(fig, 5,  S_GRN, "+5% 완화선", 3, 3)

    # ══════════════════════════════════════════════════════════════════
    #  Row 4 ─ 달러 지수 | 수익률 곡선 | 2Y / 10Y / FFR 금리
    # ══════════════════════════════════════════════════════════════════
    area(4, 1, S("DXY"), C_ORG, "달러 지수")

    # ⑪ 수익률 곡선 (10Y-2Y) 막대 ─ 역전 구간 빨간색으로 자동 표시
    bar(4, 2, S("T10Y2Y"),
        lambda v: S_GRN if v > 0 else (S_YLW if v > -0.5 else S_RED),
        "10Y−2Y")
    _hline(fig,    0, C_MUT,  "0% 기준",    4, 2, dash="solid")
    _hline(fig, -0.5, S_RED,  "-0.5% 경보", 4, 2)

    # ⑫ 2년물 / 10년물 / 기준금리 비교
    ln(4, 3, S("DGS2"),  C_RED, "2년물",   legend=True, legend_group="legend3")
    ln(4, 3, S("DGS10"), C_BLU, "10년물",  legend=True, legend_group="legend3")
    ln(4, 3, S("FFR"),   C_MUT, "기준금리", legend=True, legend_group="legend3",
       dash="dot", width=1.2)

    # ══════════════════════════════════════════════════════════════════
    #  Row 5 ─ 상업은행 대출 전년비 | Fed 스왑라인 | 지표별 점수
    # ══════════════════════════════════════════════════════════════════
    # ⑬ 상업은행 대출 전년비
    bar(5, 1, S("BANK_YOY", "D"),
        lambda v: S_GRN if v >= 6 else (S_YLW if v >= 0 else S_RED),
        "은행 대출 전년비")
    _hline(fig, 6, S_GRN, "+6% 완화", 5, 1)
    _hline(fig, 0, C_MUT, "0% 기준",  5, 1, dash="solid")

    # ⑭ Fed 통화스왑라인
    area(5, 2, S("SWPT"), C_PRP, "통화스왑라인")
    _hline(fig, 100, S_YLW, "$100B 경계", 5, 2)
    _hline(fig, 300, S_RED,  "$300B 위기", 5, 2)

    # ⑮ 지표별 점수 (수평 바) ─ 지표 수가 늘었으므로 마진 조정
    if results:
        r_names  = [r.name for r in results]
        r_scores = [r.score for r in results]
        r_colors = [{"GREEN": S_GRN, "YELLOW": S_YLW, "RED": S_RED}[r.status]
                    for r in results]
        _filled.add((5, 3))
        fig.add_trace(go.Bar(
            x=r_scores, y=r_names, orientation="h",
            marker_color=r_colors,
            text=[f"{s}/10" for s in r_scores],
            textposition="outside",
            showlegend=False,
            hovertemplate="%{y}<br>점수: %{x}/10<extra></extra>",
        ), row=5, col=3)
        fig.update_xaxes(range=[0, 13.5], row=5, col=3)
        fig.add_vline(
            x=avg_score,
            line=dict(color=C_WHT, width=1.2, dash="dash"),
            annotation_text=f"평균 {avg_score}",
            annotation_font_size=9,
            annotation_font_color=C_WHT,
            row=5, col=3,
        )

    # ══════════════════════════════════════════════════════════════════
    #  Row 6 ─ 종합 진단 테이블
    # ══════════════════════════════════════════════════════════════════
    if results:
        ST_LABEL = {"GREEN": "🟢 완화", "YELLOW": "🟡 중립", "RED": "🔴 긴축"}
        ST_BG    = {
            "GREEN":  "rgba(46,160,67,0.14)",
            "YELLOW": "rgba(210,153,34,0.14)",
            "RED":    "rgba(218,54,51,0.14)",
        }
        fig.add_trace(go.Table(
            columnwidth=[175, 100, 88, 210, 370],
            header=dict(
                values=["<b>지표명</b>", "<b>현재값</b>", "<b>상태</b>",
                        "<b>진단 요약</b>", "<b>상세 설명</b>"],
                fill_color="#21262D",
                font=dict(color="#C9D1D9", size=12),
                line_color="#30363D",
                align=["left", "center", "center", "left", "left"],
                height=34,
            ),
            cells=dict(
                values=[
                    [r.name              for r in results],
                    [r.val_str           for r in results],
                    [ST_LABEL[r.status]  for r in results],
                    [r.summary           for r in results],
                    [r.detail            for r in results],
                ],
                fill_color=[[ST_BG[r.status] for r in results]] * 5,
                font=dict(color="#C9D1D9", size=11),
                line_color="#30363D",
                align=["left", "center", "center", "left", "left"],
                height=52,
            ),
        ), row=6, col=1)

    # ══════════════════════════════════════════════════════════════════
    #  빈 서브플롯에 "데이터 없음" 텍스트 삽입
    # ══════════════════════════════════════════════════════════════════
    ALL_PLOTS = [(r, c) for r in range(1, 6) for c in range(1, 4)]
    for _r, _c in ALL_PLOTS:
        if (_r, _c) not in _filled:
            _no_data(_r, _c)

    # ══════════════════════════════════════════════════════════════════
    #  글로벌 스타일
    # ══════════════════════════════════════════════════════════════════
    AX_STYLE = dict(
        showgrid=True,   gridwidth=0.5, gridcolor="#1C2128",
        showline=True,   linecolor="#30363D",
        tickfont=dict(color=C_MUT, size=9),
        zerolinecolor="#30363D",
    )
    for r in range(1, 5):
        for c in range(1, 4):
            try:
                fig.update_xaxes(**AX_STYLE, row=r, col=c)
                fig.update_yaxes(**AX_STYLE, row=r, col=c)
            except Exception:
                pass

    # 서브플롯 제목 폰트
    for ann in fig.layout.annotations:
        ann.font.update(size=11, color=C_MUT)

    # ── legend  : BS 구성 (상단 중앙) ────────────────────────────────
    # ── legend2 : HY/IG 크레딧 스프레드 (⑦ 차트 내부 우상단) ──────────
    fig.update_layout(
        legend=dict(
            bgcolor="rgba(22,27,34,0.85)", bordercolor="#30363D",
            font=dict(color=C_MUT, size=10),
            x=0.37, y=0.999, orientation="h",
            title=dict(text="② Fed 대차대조표", font=dict(size=9, color=C_MUT)),
        ),
        legend2=dict(
            bgcolor="rgba(22,27,34,0.85)", bordercolor="#30363D",
            font=dict(color=C_MUT, size=10),
            x=0.005, y=0.565, orientation="v",
            title=dict(text="⑦ 크레딧", font=dict(size=9, color=C_MUT)),
        ),
        legend3=dict(
            bgcolor="rgba(22,27,34,0.85)", bordercolor="#30363D",
            font=dict(color=C_MUT, size=10),
            x=0.680, y=0.345, orientation="v",
            title=dict(text="⑫ 금리", font=dict(size=9, color=C_MUT)),
        ),
    )

    fig.update_layout(
        title=dict(
            text=(
                f"<b>🏦 미국 달러 유동성 종합 모니터</b>"
                f"  <span style='font-size:14px;color:{C_MUT};'>·  {today}</span><br>"
                f"<span style='font-size:16px;'>{verdict}</span><br>"
                f"<span style='font-size:12px;color:{C_MUT};'>{detail}</span>"
            ),
            x=0.5, xanchor="center",
            font=dict(size=20, color=C_WHT),
        ),
        paper_bgcolor="#0D1117",
        plot_bgcolor="#161B22",
        font=dict(
            family="Malgun Gothic, Apple SD Gothic Neo, NanumGothic, Arial",
            color="#C9D1D9",
        ),
        height=2600,
        margin=dict(l=65, r=50, t=190, b=40),
        barmode="group",
    )

    pio.write_html(
        fig,
        file=output,
        auto_open=auto_open,
        include_plotlyjs="cdn",   # CDN 사용으로 파일 경량화 (~30KB vs 3MB)
        config={"displayModeBar": True, "scrollZoom": True, "responsive": True},
    )
    print(f"\n  ✅  대시보드 저장 완료: {os.path.abspath(output)}")
    if auto_open:
        print("     → 브라우저가 자동으로 열립니다.\n")


# ══════════════════════════════════════════════════════════════════════
#  텔레그램 알림 전송
# ══════════════════════════════════════════════════════════════════════
class TelegramSender:
    """
    Bot API를 통해 진단 리포트와 HTML 대시보드를 텔레그램으로 전송합니다.

    전송 순서:
      ① 종합 요약 메시지 (sendMessage, HTML parse mode)
      ② 지표별 상세 메시지 (4096자 초과 시 자동 분할)
      ③ HTML 대시보드 파일 첨부 (sendDocument)
    """

    BASE = "https://api.telegram.org/bot{token}/{method}"
    MAX_LEN = 4000   # 텔레그램 최대 4096자, 여유분 확보

    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self._ok     = bool(token and chat_id)

    def _url(self, method: str) -> str:
        return self.BASE.format(token=self.token, method=method)

    def _post(self, method: str, **kwargs) -> bool:
        """API 호출 래퍼. 실패해도 예외를 올리지 않습니다."""
        try:
            r = requests.post(self._url(method), timeout=20, **kwargs)
            data = r.json()
            if not data.get("ok"):
                print(f"  ⚠️  Telegram {method} 실패: {data.get('description')}")
                return False
            return True
        except Exception as e:
            print(f"  ⚠️  Telegram 연결 오류: {e}")
            return False

    def _send_text(self, text: str, disable_preview: bool = True) -> bool:
        return self._post(
            "sendMessage",
            json={
                "chat_id":                  self.chat_id,
                "text":                     text,
                "parse_mode":               "HTML",
                "disable_web_page_preview": disable_preview,
            },
        )

    def _send_document(self, path: str, caption: str = "") -> bool:
        if not os.path.exists(path):
            print(f"  ⚠️  파일 없음: {path}")
            return False
        with open(path, "rb") as f:
            return self._post(
                "sendDocument",
                data={"chat_id": self.chat_id, "caption": caption, "parse_mode": "HTML"},
                files={"document": (os.path.basename(path), f, "text/html")},
            )

    # ── 메시지 빌더 ────────────────────────────────────────────────────

    @staticmethod
    def _esc(text: str) -> str:
        """HTML 특수문자를 이스케이프합니다."""
        return (text.replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;"))

    def _build_summary(self, verdict: str, detail: str,
                       results: list, avg_score: int) -> str:
        """① 종합 요약 메시지 텍스트 생성"""
        now   = datetime.now().strftime("%Y-%m-%d %H:%M KST")
        bar   = self._score_bar(avg_score)
        lines = [
            f"🏦 <b>미국 달러 유동성 모니터</b>",
            f"📅 {now}",
            "",
            f"<b>종합 판정</b>: {self._esc(verdict)}",
            f"<b>총점</b>: {bar} {avg_score}/10",
            f"<i>{self._esc(detail)}</i>",
            "",
            "─────────────────────",
        ]

        # 한 줄 요약표
        EMJ = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}
        for r in results:
            score_bar = self._score_bar(r.score, length=5)
            lines.append(
                f"{EMJ[r.status]} <b>{self._esc(r.name)}</b>  "
                f"<code>{self._esc(r.val_str)}</code>  {score_bar} {r.score}/10"
            )

        lines += ["", "─────────────────────",
                  "📊 상세 분석은 다음 메시지를 확인하세요."]
        return "\n".join(lines)

    def _build_details(self, results: list) -> list[str]:
        """
        ② 지표별 상세 설명 블록 생성.
        4000자 초과 시 자동으로 여러 메시지로 분할합니다.
        """
        EMJ = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}
        STATUS_KO = {"GREEN": "완화", "YELLOW": "중립", "RED": "긴축"}

        blocks: list[str] = []
        for r in results:
            block = (
                f"\n{EMJ[r.status]} <b>{self._esc(r.name)}</b>\n"
                f"   현재값 : <code>{self._esc(r.val_str)}</code>  "
                f"[{STATUS_KO[r.status]}  {r.score}/10]\n"
                f"   요약   : {self._esc(r.summary)}\n"
                f"   진단   : {self._esc(r.detail)}\n"
            )
            blocks.append(block)

        # 4000자 기준으로 메시지 분할
        messages: list[str] = []
        buf = "📋 <b>지표별 상세 진단</b>\n━━━━━━━━━━━━━━━━━━━━━"
        for block in blocks:
            if len(buf) + len(block) > self.MAX_LEN:
                messages.append(buf)
                buf = "📋 <b>지표별 상세 진단 (계속)</b>\n━━━━━━━━━━━━━━━━━━━━━"
            buf += block
        if buf:
            messages.append(buf)
        return messages

    @staticmethod
    def _score_bar(score: int, length: int = 10) -> str:
        """점수를 시각적 막대로 변환 (●○ 형식)"""
        filled = round(score / 10 * length)
        return "●" * filled + "○" * (length - filled)

    # ── 공개 메서드 ────────────────────────────────────────────────────

    def send_report(self, results: list, avg_score: int,
                    verdict: str, detail: str,
                    html_path: str) -> None:
        """진단 리포트 전체를 텔레그램으로 전송합니다."""
        if not self._ok:
            print("  ⚠️  TELEGRAM_TOKEN / TELEGRAM_CHAT_ID 미설정 → 텔레그램 전송 건너뜀")
            return

        print("  → 텔레그램 전송 시작...")

        # ① 종합 요약
        summary = self._build_summary(verdict, detail, results, avg_score)
        ok1 = self._send_text(summary)
        if ok1:
            print("  ✅  요약 메시지 전송 완료")

        # ② 지표별 상세 (분할 전송)
        detail_msgs = self._build_details(results)
        for i, msg in enumerate(detail_msgs, 1):
            ok = self._send_text(msg)
            if ok:
                print(f"  ✅  상세 메시지 {i}/{len(detail_msgs)} 전송 완료")

        # ③ HTML 대시보드 파일 첨부
        caption = (
            f"📊 <b>유동성 대시보드</b> · {datetime.now().strftime('%Y-%m-%d')}\n"
            f"브라우저에서 열면 인터랙티브 차트를 확인할 수 있습니다."
        )
        ok3 = self._send_document(html_path, caption=caption)
        if ok3:
            print("  ✅  HTML 대시보드 파일 첨부 완료")

        if ok1:
            print()


# ══════════════════════════════════════════════════════════════════════
#  콘솔 리포트
# ══════════════════════════════════════════════════════════════════════
def print_report(results: list, verdict: str, detail: str) -> None:
    EMJ = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}
    W   = 68
    print(f"\n{'═'*W}")
    print(f"  🏦  미국 달러 유동성 진단 리포트")
    print(f"  ⏰  {datetime.now():%Y-%m-%d %H:%M}")
    print(f"{'═'*W}")
    print(f"\n  종합 판정: {verdict}")
    print(f"  {detail}\n")
    print(f"{'─'*W}")
    for r in results:
        print(f"\n  {EMJ[r.status]}  {r.name}")
        print(f"     현재값: {r.val_str}")
        print(f"     요약  : {r.summary}")
        print(f"     진단  : {r.detail}")
    print(f"\n{'═'*W}\n")


# ══════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════
def main() -> None:
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║         🏦  미국 달러 유동성 종합 모니터  v2.0                        ║
║  Fed BS · RRP · TGA · 지준금 · SOFR · HY Spread · VIX · DXY        ║
╚══════════════════════════════════════════════════════════════════════╝""")

    # ── 환경변수 검증 ──────────────────────────────────────────────────
    errors = []
    if not FRED_API_KEY:
        errors.append(
            "FRED_API_KEY 가 설정되지 않았습니다.\n"
            "   → https://fred.stlouisfed.org/docs/api/api_key.html 에서 무료 발급"
        )
    if not TELEGRAM_TOKEN:
        print("  ℹ️  TELEGRAM_TOKEN 미설정 → 텔레그램 전송 비활성화 (실행은 계속됩니다)")
    if not TELEGRAM_CHAT_ID:
        print("  ℹ️  TELEGRAM_CHAT_ID 미설정 → 텔레그램 전송 비활성화 (실행은 계속됩니다)")

    if errors:
        for e in errors:
            print(f"\n❌  {e}")
        sys.exit(1)

    print(f"\n  환경변수 상태:")
    print(f"    FRED_API_KEY     : {'✅ 설정됨' if FRED_API_KEY     else '❌ 없음'}")
    print(f"    TELEGRAM_TOKEN   : {'✅ 설정됨' if TELEGRAM_TOKEN   else '⚠️  없음 (전송 건너뜀)'}")
    print(f"    TELEGRAM_CHAT_ID : {'✅ 설정됨' if TELEGRAM_CHAT_ID else '⚠️  없음 (전송 건너뜀)'}")
    print()

    # ── [1] 데이터 수집 ────────────────────────────────────────────────
    print("[1/5]  데이터 수집")
    raw = DataFetcher(FRED_API_KEY, LOOKBACK_DAYS).fetch()

    # ── [2] 파생 지표 ──────────────────────────────────────────────────
    print("[2/5]  파생 지표 계산")
    der = compute_derived(raw)
    print(f"  → 넷 유동성:     {len(der.get('NET_LIQ',  pd.Series()))} 포인트")
    print(f"  → SOFR 스프레드: {len(der.get('SOFR_SPR', pd.Series()))} 포인트")
    print(f"  → M2 전년비:     {len(der.get('M2_YOY',   pd.Series()))} 포인트\n")

    # ── [3] 진단 ───────────────────────────────────────────────────────
    print("[3/5]  유동성 환경 진단")
    results, avg, verdict, detail = DiagEngine(raw, der).run()
    print_report(results, verdict, detail)

    # ── [4] 대시보드 생성 ──────────────────────────────────────────────
    print("[4/5]  대시보드 생성")
    build_dashboard(raw, der, results, avg, verdict, detail, OUTPUT_HTML, AUTO_OPEN)

    # ── [5] 텔레그램 전송 ──────────────────────────────────────────────
    print("[5/5]  텔레그램 전송")
    TelegramSender(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID).send_report(
        results, avg, verdict, detail, OUTPUT_HTML
    )

    print("  🎉  모든 작업 완료!\n")


if __name__ == "__main__":
    main()
