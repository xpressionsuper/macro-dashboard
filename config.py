#!/usr/bin/env python3
"""
매크로 모니터 v3 ─ 전역 설정 & 지표 메타데이터
=====================================================================
모든 FRED 시리즈 ID, 표시명, 의미, 방향성을 한 곳에서 관리합니다.
새 지표 추가 시 이 파일만 수정하면 됩니다.
"""

import os
from dataclasses import dataclass, field
from typing import Optional

# ── 환경변수 ──────────────────────────────────────────────────────────
FRED_API_KEY     = os.environ.get("FRED_API_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── 수집·출력 설정 ────────────────────────────────────────────────────
LOOKBACK_DAYS = 365 * 5      # 5년치 데이터 수집
OUTPUT_HTML   = "macro_dashboard.html"
AUTO_OPEN     = False
REQUEST_DELAY = 0.4          # FRED API 레이트 리밋 회피 간격(초)


# ══════════════════════════════════════════════════════════════════════
#  지표 메타데이터 구조체
# ══════════════════════════════════════════════════════════════════════
@dataclass
class IndicatorMeta:
    """단일 지표의 모든 메타 정보를 보유합니다."""
    key: str                        # 내부 식별자 (고유값)
    name_ko: str                    # 한글 표시명
    name_en: str                    # 영문 표시명
    description: str                # 의미 및 해석
    direction: str                  # 방향성 설명 ("↑유동성 증가" 등)
    source: str                     # 데이터 출처
    category: int                   # 카테고리 번호 1~5
    fred_id: Optional[str] = None   # FRED 시리즈 ID (없으면 파생/외부)
    unit: str = ""                  # 표시 단위 ("$B", "%", "bps" 등)
    divisor: float = 1.0            # 단위 환산 제수 (million → billion: 1000)
    available: bool = True          # 데이터 수집 가능 여부


# ══════════════════════════════════════════════════════════════════════
#  ① 연준(Fed) 직접 공급 지표
#  Fed가 직접 조절하는 유동성 채널
# ══════════════════════════════════════════════════════════════════════
FED_SUPPLY: list[IndicatorMeta] = [
    IndicatorMeta(
        key="BS", name_ko="Fed 대차대조표", name_en="Balance Sheet",
        description="총자산 규모. QE(국채·MBS 매입)로 팽창, QT로 수축. 시중 본원통화와 직결.",
        direction="↑유동성 증가", source="FRED: WALCL", category=1,
        fred_id="WALCL", unit="$B", divisor=1_000,
    ),
    IndicatorMeta(
        key="UST", name_ko="국채 보유량", name_en="UST Holdings",
        description="대차대조표 내 국채 비중. QT 속도의 직접 지표.",
        direction="↑유동성 증가", source="FRED: TREAST", category=1,
        fred_id="TREAST", unit="$B", divisor=1_000,
    ),
    IndicatorMeta(
        key="MBS", name_ko="MBS 보유량", name_en="MBS Holdings",
        description="모기지 담보증권 보유량. 주택시장·금리에 연동.",
        direction="↑유동성 증가", source="FRED: WSHOMCB", category=1,
        fred_id="WSHOMCB", unit="$B", divisor=1_000,
    ),
    IndicatorMeta(
        key="RRP", name_ko="역레포 잔고", name_en="RRP / ON RRP",
        description="MMF 등이 연준에 맡기는 단기 자금. 잔고 감소 = 시중으로 유동성 방출.",
        direction="↓유동성 증가", source="FRED: RRPONTSYD", category=1,
        fred_id="RRPONTSYD", unit="$B",
    ),
    IndicatorMeta(
        key="RESERVES", name_ko="지준금 잔고", name_en="Reserve Balances",
        description="은행들이 연준에 예치한 지급준비금. 감소하면 은행 유동성 긴박 신호.",
        direction="↑유동성 증가", source="FRED: WRBWFRBL", category=1,
        fred_id="WRBWFRBL", unit="$B",
    ),
    IndicatorMeta(
        key="DISC_WIN", name_ko="할인창구 대출", name_en="Discount Window",
        description="은행의 연준 직접 단기대출. 급증은 유동성 위기 신호.",
        direction="↑스트레스 증가", source="FRED: WDISCAL", category=1,
        fred_id="WDISCAL", unit="$B",
    ),
]


# ══════════════════════════════════════════════════════════════════════
#  ② 재무부(Treasury) 채널 지표
#  TGA와 발행 전략으로 시중 유동성을 흡수/방출
# ══════════════════════════════════════════════════════════════════════
TREASURY_CHANNEL: list[IndicatorMeta] = [
    IndicatorMeta(
        key="TGA", name_ko="TGA 잔고", name_en="Treasury General Account",
        description="연준 내 재무부 당좌예금. 감소=시중 달러 방출, 증가=유동성 흡수. 부채한도 협상과 연동.",
        direction="↓유동성 증가", source="TreasuryDirect (매일)", category=2,
        fred_id=None, unit="$B",   # Treasury Fiscal Data API로 별도 수집
    ),
]


# ══════════════════════════════════════════════════════════════════════
#  ③ 합성(Net) 유동성 지표
#  연준·재무부 데이터를 결합한 실질 유동성 계량
# ══════════════════════════════════════════════════════════════════════
NET_LIQUIDITY: list[IndicatorMeta] = [
    IndicatorMeta(
        key="NET_LIQ", name_ko="넷 유동성", name_en="Net Liquidity (BS−RRP−TGA)",
        description="월가에서 가장 많이 쓰는 합성 지표. 연준 대차대조표에서 RRP·TGA를 차감한 값. S&P500과 높은 상관관계.",
        direction="↑유동성 증가", source="파생 계산 (BS−RRP−TGA)", category=3,
        fred_id=None, unit="$B",
    ),
    IndicatorMeta(
        key="SOFR_SPR", name_ko="SOFR−IORB 스프레드", name_en="SOFR−IORB Spread",
        description="단기 레포 금리가 IORB 위로 뜨면 지준금 희소 신호. 2019년 9월처럼 급등 시 유동성 위기.",
        direction="↑스트레스 증가", source="파생 계산 (SOFR−IORB)", category=3,
        fred_id=None, unit="bps",
    ),
    IndicatorMeta(
        key="M2", name_ko="M2 통화량", name_en="M2 Money Stock",
        description="광의통화 절대 잔고. QE·QT 효과가 시중에 파급된 결과물.",
        direction="↑유동성 증가", source="FRED: M2SL", category=3,
        fred_id="M2SL", unit="$B",
    ),
    IndicatorMeta(
        key="M2_YOY", name_ko="M2 전년비 증가율", name_en="M2 YoY Growth",
        description="광의통화 전년비 증가율. 시차 존재(3~6개월). 마이너스 전환 시 유동성 긴축 확인.",
        direction="↑유동성 증가", source="파생 계산 (M2SL YoY)", category=3,
        fred_id=None, unit="%",
    ),
]


# ══════════════════════════════════════════════════════════════════════
#  ④ 금융시장 유동성 지표
#  시장에서 관측되는 유동성 가격·스트레스 신호
# ══════════════════════════════════════════════════════════════════════
FINANCIAL_MARKET: list[IndicatorMeta] = [
    IndicatorMeta(
        key="SOFR", name_ko="SOFR", name_en="Secured Overnight Financing Rate",
        description="단기 레포 금리. IORB와의 스프레드로 지준금 희소도를 측정.",
        direction="", source="FRED: SOFR", category=4,
        fred_id="SOFR", unit="%",
    ),
    IndicatorMeta(
        key="IORB", name_ko="IORB", name_en="Interest on Reserve Balances",
        description="연준이 지급하는 지준금 이자율. SOFR과의 스프레드 계산 기준.",
        direction="", source="FRED: IORB", category=4,
        fred_id="IORB", unit="%",
    ),
    IndicatorMeta(
        key="HY_OAS", name_ko="HY 크레딧 스프레드", name_en="HY OAS",
        description="고수익채권 스프레드. 확대=신용 유동성 긴축. HY 스프레드가 위기 선행하는 경향.",
        direction="↑유동성 악화", source="FRED: BAMLH0A0HYM2", category=4,
        fred_id="BAMLH0A0HYM2", unit="bps",
    ),
    IndicatorMeta(
        key="IG_OAS", name_ko="IG 크레딧 스프레드", name_en="IG OAS",
        description="투자등급 채권 스프레드. HY보다 후행하지만 더 광범위한 신용환경 반영.",
        direction="↑유동성 악화", source="FRED: BAMLC0A0CM", category=4,
        fred_id="BAMLC0A0CM", unit="bps",
    ),
    IndicatorMeta(
        key="NFCI", name_ko="금융조건지수 (시카고 Fed)", name_en="NFCI",
        description="금리·신용·주식·환율 복합 지수. 0 기준 양수=긴축, 음수=완화.",
        direction="↓조건 완화", source="FRED: NFCI", category=4,
        fred_id="NFCI", unit="",
    ),
    IndicatorMeta(
        key="VIX", name_ko="VIX 변동성 지수", name_en="VIX",
        description="시장 공포지수. 급등 시 유동성 위기와 동반. 20↑ 주의, 30↑ 위험.",
        direction="↑스트레스 증가", source="FRED: VIXCLS", category=4,
        fred_id="VIXCLS", unit="",
    ),
    IndicatorMeta(
        key="DGS2", name_ko="2년물 국채금리", name_en="2Y Treasury Yield",
        description="단기 정책금리 기대치 반영. 수익률 곡선의 단기 앵커.",
        direction="", source="FRED: DGS2", category=4,
        fred_id="DGS2", unit="%",
    ),
    IndicatorMeta(
        key="DGS10", name_ko="10년물 국채금리", name_en="10Y Treasury Yield",
        description="장기 성장·인플레 기대치 반영. 할인율의 핵심.",
        direction="", source="FRED: DGS10", category=4,
        fred_id="DGS10", unit="%",
    ),
    IndicatorMeta(
        key="T10Y2Y", name_ko="수익률 곡선 (10Y−2Y)", name_en="Yield Curve",
        description="장단기 금리차. 음수(역전)=경기침체 선행 신호. 유동성 긴축과 연동.",
        direction="↑정상화", source="FRED: T10Y2Y", category=4,
        fred_id="T10Y2Y", unit="%",
    ),
]


# ══════════════════════════════════════════════════════════════════════
#  ⑤ 글로벌 달러 유동성 지표
#  역외 달러 공급과 환율 메커니즘
# ══════════════════════════════════════════════════════════════════════
GLOBAL_DOLLAR: list[IndicatorMeta] = [
    IndicatorMeta(
        key="SWPT", name_ko="Fed 스왑라인 잔고", name_en="Central Bank Swaps",
        description="글로벌 중앙은행에 달러 공급. 잔고 급증=역외 달러 부족 위기(2008·2020 재현).",
        direction="↑달러부족 신호", source="FRED: SWPT", category=5,
        fred_id="SWPT", unit="$B",
    ),
    IndicatorMeta(
        key="DXY", name_ko="달러 지수 (Broad)", name_en="USD Broad Index",
        description="달러 강세=글로벌 달러 유동성 부족 신호. 특히 신흥국 달러 부채와 연동.",
        direction="↑달러 공급 감소", source="FRED: DTWEXBGS", category=5,
        fred_id="DTWEXBGS", unit="",
    ),
    IndicatorMeta(
        key="JPY", name_ko="엔/달러 환율", name_en="JPY/USD",
        description="일본의 엔캐리 청산은 글로벌 달러 유동성에 충격. 엔 급등(수치 하락) 시 달러 자산 매도 연쇄.",
        direction="엔↑ → 리스크", source="FRED: DEXJPUS", category=5,
        fred_id="DEXJPUS", unit="JPY/USD",
    ),
]


# ── 카테고리 통합 딕셔너리 ────────────────────────────────────────────
CATEGORY_META: dict[int, tuple[str, str, list[IndicatorMeta]]] = {
    1: ("① 연준(Fed) 직접 공급 지표",    "Fed가 직접 조절하는 유동성 채널",               FED_SUPPLY),
    2: ("② 재무부(Treasury) 채널 지표",  "TGA와 발행 전략으로 시중 유동성을 흡수/방출",   TREASURY_CHANNEL),
    3: ("③ 합성(Net) 유동성 지표",       "연준·재무부 데이터를 결합한 실질 유동성 계량",  NET_LIQUIDITY),
    4: ("④ 금융시장 유동성 지표",        "시장에서 관측되는 유동성 가격·스트레스 신호",    FINANCIAL_MARKET),
    5: ("⑤ 글로벌 달러 유동성 지표",    "역외 달러 공급과 환율 메커니즘",               GLOBAL_DOLLAR),
}

ALL_INDICATORS: list[IndicatorMeta] = (
    FED_SUPPLY + TREASURY_CHANNEL + NET_LIQUIDITY + FINANCIAL_MARKET + GLOBAL_DOLLAR
)
