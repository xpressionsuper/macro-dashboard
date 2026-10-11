#!/usr/bin/env python3
"""
config_inflation.py ─ 물가(인플레이션) 모니터 전역 설정 & 지표 메타데이터
=====================================================================
CPI·PCE 계열은 FRED에서 지수(레벨) 형태로만 제공되므로, main_combined.py에서
수집 직후 전년동월비(YoY %) 로 변환합니다. YOY_TRANSFORM_KEYS에 나열된 키만
변환 대상이며, 나머지(기대인플레이션·유가 등)는 원래 단위 그대로 씁니다.
"""

from config import IndicatorMeta

OUTPUT_HTML   = "inflation_dashboard.html"

# 수집 직후 YoY(%) 로 변환해야 하는 지표 키 (레벨 지수 → 전년동월비 %)
YOY_TRANSFORM_KEYS: set[str] = {"CPI_ALL", "CPI_CORE", "SHELTER", "PCE_ALL", "PCE_CORE", "PPI", "WAGE"}


# ══════════════════════════════════════════════════════════════════════
#  ① 소비자물가 (CPI)
#  가장 널리 인용되는 물가지표. 헤드라인(전체)과 코어(식품·에너지 제외)를
#  함께 봐야 일시적 충격과 근원 추세를 구분할 수 있음
# ══════════════════════════════════════════════════════════════════════
CPI_GROUP: list[IndicatorMeta] = [
    IndicatorMeta(
        key="CPI_ALL", name_ko="소비자물가지수 (헤드라인, YoY)", name_en="CPI All Items YoY",
        description="도시 소비자가 실제로 구매하는 상품·서비스 바스켓 전체의 가격을 전년 동월과 비교한 상승률입니다. 유가·식료품 등 변동성 큰 항목이 그대로 포함돼 있어 체감물가에 가장 가깝지만, 일시적 충격에도 크게 흔들립니다.",
        direction="↑물가 압력 증가", source="FRED: CPIAUCSL (YoY 변환)", category=1,
        fred_id="CPIAUCSL", unit="%",
        when_up="유가 급등, 공급망 차질, 임금 상승분 전가, 강한 수요 등으로 광범위한 품목의 가격이 오를 때 상승합니다. 연준의 2% 목표를 웃도는 상태가 길어질수록 긴축 정책이 오래갈 가능성이 커집니다.",
        when_down="공급망이 정상화되거나 수요가 식어 가격 인상 압력이 완화될 때 하락합니다. 다만 마이너스(디플레이션)로 가는 것은 오히려 경기침체의 신호일 수 있어 주의가 필요합니다.",
    ),
    IndicatorMeta(
        key="CPI_CORE", name_ko="근원 소비자물가지수 (Core CPI, YoY)", name_en="Core CPI YoY",
        description="변동성이 큰 식품·에너지를 제외한 물가상승률로, 연준이 통화정책을 결정할 때 헤드라인보다 더 비중 있게 보는 '진짜 추세' 지표입니다. 일시적 유가 충격에 흔들리지 않아 기조적인 물가 흐름을 보여줍니다.",
        direction="↑물가 압력 증가", source="FRED: CPILFESL (YoY 변환)", category=1,
        fred_id="CPILFESL", unit="%",
        when_up="임대료(주거비), 서비스업 임금 등 하방 경직적인 항목의 가격이 꾸준히 오를 때 상승합니다. 헤드라인보다 더디게 움직이지만, 한번 오르면 잘 안 꺾이는 특징이 있습니다.",
        when_down="주거비 상승세가 둔화되거나 서비스물가 압력이 완화될 때 하락합니다. 연준이 금리 인하를 검토하는 핵심 근거가 됩니다.",
    ),
    IndicatorMeta(
        key="STICKY", name_ko="끈적한 물가지수 (Sticky CPI, YoY)", name_en="Sticky Price CPI YoY",
        description="애틀랜타 연은이 만든 지표로, 임대료·의료비·교육비처럼 한번 오르면 잘 내려가지 않는(가격 변경 주기가 긴) 품목들만 따로 모아 계산한 근원물가입니다. 코어CPI보다도 더 '끈적한' 기조적 인플레이션을 보여줍니다.",
        direction="↑장기화 위험", source="FRED: CORESTICKM159SFRBATL (원 시리즈가 YoY %)", category=1,
        fred_id="CORESTICKM159SFRBATL", unit="%",
        when_up="가격을 자주 바꾸지 않는 서비스업종(임대, 의료, 보험 등)에서 인상이 광범위하게 퍼질 때 상승합니다. 이 지표가 높으면 물가가 쉽게 꺾이지 않을 가능성이 크다는 뜻입니다.",
        when_down="경직적 항목들의 가격 인상 압력까지 누그러질 때 하락하며, 이는 인플레이션이 '일시적'이 아니라 '진짜로' 잡히고 있다는 신뢰도 높은 신호입니다.",
    ),
    IndicatorMeta(
        key="SHELTER", name_ko="주거비 물가 (Shelter CPI, YoY)", name_en="Shelter CPI YoY",
        description="CPI 바스켓에서 가장 비중이 큰(약 3분의 1) 단일 항목인 주거비(임대료 및 자가주거비 환산분)의 상승률입니다. 실제 임대료 계약 갱신 주기 때문에 시중 집값·전월세 시세보다 6~12개월 늦게 반영되는 시차가 있습니다.",
        direction="↑물가 압력 증가", source="FRED: CUSR0000SAH1 (YoY 변환)", category=1,
        fred_id="CUSR0000SAH1", unit="%",
        when_up="주택 수요 증가나 공급 부족으로 시중 임대료가 오른 효과가, 시차를 두고 CPI 통계에 반영될 때 상승합니다. 비중이 커서 이 항목 하나가 헤드라인·코어 CPI 전체를 밀어올리는 경우가 많습니다.",
        when_down="시중 임대료 상승세가 꺾인 효과가 통계에 뒤늦게 반영되며 하락합니다. 코어 CPI가 꺾이려면 결국 이 항목이 먼저 꺾여야 하는 경우가 많습니다.",
    ),
]


# ══════════════════════════════════════════════════════════════════════
#  ② 연준이 실제로 보는 지표 (PCE)
#  연준의 공식 물가 목표(2%)는 CPI가 아니라 PCE 기준
# ══════════════════════════════════════════════════════════════════════
PCE_GROUP: list[IndicatorMeta] = [
    IndicatorMeta(
        key="PCE_ALL", name_ko="개인소비지출 물가지수 (PCE, YoY)", name_en="PCE Price Index YoY",
        description="연준이 공식적으로 '2% 목표'를 설정할 때 기준으로 삼는 물가지표입니다. CPI와 달리 소비자의 대체 구매 행동(비싸진 품목 대신 싼 대체재를 사는 것)을 반영해 계산 방식이 더 정교하다고 평가받습니다.",
        direction="↑물가 압력 증가", source="FRED: PCEPI (YoY 변환)", category=2,
        fred_id="PCEPI", unit="%",
        when_up="CPI와 마찬가지로 광범위한 물가 상승 압력이 있을 때 오르지만, 대체 구매 효과 때문에 통상 CPI보다 낮게 나옵니다.",
        when_down="물가 압력이 완화될 때 하락합니다. 연준 위원들의 발언에서 '이 지표가 2%에 가까워지고 있다'는 언급이 나오면 금리 정책 전환의 신호로 해석됩니다.",
    ),
    IndicatorMeta(
        key="PCE_CORE", name_ko="근원 PCE 물가지수 (Core PCE, YoY)", name_en="Core PCE Price Index YoY",
        description="식품·에너지를 제외한 PCE로, 연준이 통화정책 회의(FOMC)에서 가장 핵심적으로 참고하는 '단일 지표'라고 할 수 있습니다. 이 숫자 하나가 시장 예상치에서 벗어나면 금리·주식·채권 시장이 즉각 반응합니다.",
        direction="↑물가 압력 증가", source="FRED: PCEPILFE (YoY 변환)", category=2,
        fred_id="PCEPILFE", unit="%",
        when_up="서비스물가·임금 상승분 전가 등 기조적 요인으로 상승합니다. 2%를 훨씬 웃도는 상태가 이어지면 연준이 금리를 높게 오래 유지할 명분이 됩니다.",
        when_down="기조적 물가 압력이 실제로 완화되고 있다는 뜻으로, 연준의 금리 인하 논의에 가장 직접적인 근거가 됩니다.",
    ),
]


# ══════════════════════════════════════════════════════════════════════
#  ③ 기대인플레이션·시장 신호
#  '앞으로' 물가가 어떻게 될지에 대한 시장·소비자의 예상
# ══════════════════════════════════════════════════════════════════════
EXPECTATIONS: list[IndicatorMeta] = [
    IndicatorMeta(
        key="BREAKEVEN5", name_ko="5년 기대인플레이션 (시장)", name_en="5Y Breakeven Inflation",
        description="일반 국채와 물가연동국채(TIPS)의 금리 차이로 역산한, 채권시장 참여자들이 예상하는 향후 5년간의 평균 물가상승률입니다. 실제 돈이 걸린 시장 가격이라 설문조사보다 실시간성이 높습니다.",
        direction="↑기대 인플레 상승", source="FRED: T5YIE", category=3,
        fred_id="T5YIE", unit="%",
        when_up="시장이 향후에도 물가 압력이 이어질 것으로 볼 때, 혹은 유가 급등 등 즉각적 충격이 있을 때 상승합니다. 기대인플레이션 자체가 오르면 실제 물가도 따라 오르는 자기실현적 경향이 있어 연준이 예민하게 반응합니다.",
        when_down="시장이 향후 물가가 안정될 것으로 볼 때 하락합니다.",
    ),
    IndicatorMeta(
        key="BREAKEVEN10", name_ko="10년 기대인플레이션 (시장)", name_en="10Y Breakeven Inflation",
        description="같은 방식으로 계산한 10년 만기 기준 기대인플레이션입니다. 5년물보다 단기 충격에 덜 흔들려, 시장이 보는 '장기 균형 물가 수준'에 대한 신뢰도를 보여줍니다.",
        direction="↑기대 인플레 상승", source="FRED: T10YIE", category=3,
        fred_id="T10YIE", unit="%",
        when_up="장기적으로도 물가가 높게 유지될 것이란 우려가 커질 때 상승합니다. 연준의 신뢰도(2% 목표 달성 능력)에 대한 시장의 의구심을 반영하기도 합니다.",
        when_down="연준의 물가 통제 능력에 대한 신뢰가 굳건할 때 안정적으로 낮게 유지됩니다.",
    ),
    IndicatorMeta(
        key="MICH", name_ko="소비자 기대인플레이션 (미시간대, 1년)", name_en="U. Michigan 1Y Inflation Expectation",
        description="일반 소비자들에게 '앞으로 1년간 물가가 얼마나 오를 것 같은지'를 직접 설문한 결과입니다. 시장 기반 지표와 달리 일반 대중의 '체감' 기대를 보여주며, 실제 소비·임금 협상 행태에 영향을 줍니다.",
        direction="↑기대 인플레 상승", source="FRED: MICH", category=3,
        fred_id="MICH", unit="%",
        when_up="최근 겪은 물가 상승(특히 마트·주유소처럼 자주 접하는 품목)을 근거로 소비자들이 향후에도 물가가 오를 것으로 예상할 때 상승합니다.",
        when_down="최근 물가 상승세가 둔화된 걸 소비자들이 체감할 때 하락합니다.",
    ),
]


# ══════════════════════════════════════════════════════════════════════
#  ④ 공급망·비용 압력 (선행 신호)
#  소비자물가에 반영되기 전, 생산 단계에서 먼저 나타나는 비용 압력
# ══════════════════════════════════════════════════════════════════════
COST_PRESSURE: list[IndicatorMeta] = [
    IndicatorMeta(
        key="PPI", name_ko="생산자물가지수 (PPI, YoY)", name_en="Producer Price Index YoY",
        description="기업이 원자재·중간재를 사들이거나 완제품을 출하할 때의 가격 변동률입니다. 기업이 늘어난 비용을 소비자에게 전가하기까지 시차가 있어, PPI는 보통 CPI보다 몇 달 먼저 움직이는 선행지표로 쓰입니다.",
        direction="↑향후 CPI 상승압력", source="FRED: PPIFIS (YoY 변환)", category=4,
        fred_id="PPIFIS", unit="%",
        when_up="원자재 가격 상승, 공급망 차질, 운송비 상승 등으로 기업의 생산 비용이 늘어날 때 상승합니다. 이 상승분이 몇 달 뒤 소비자 가격에 전가될 가능성이 있어 선행 경보로 봅니다.",
        when_down="원자재·에너지 가격이 안정되거나 공급망이 정상화될 때 하락하며, 향후 소비자물가 압력 완화를 예고하는 경우가 많습니다.",
    ),
    IndicatorMeta(
        key="OIL", name_ko="WTI 유가", name_en="WTI Crude Oil Price",
        description="서부텍사스산 원유(WTI) 현물가격입니다. 에너지는 CPI 헤드라인 변동의 상당 부분을 설명할 뿐 아니라, 운송·생산 비용을 통해 거의 모든 물가에 간접적으로 영향을 미치는 '물가의 물가'입니다.",
        direction="↑물가 압력 증가", source="FRED: DCOILWTICO", category=4,
        fred_id="DCOILWTICO", unit="$/배럴",
        when_up="산유국 감산, 지정학적 리스크(중동 분쟁 등), 글로벌 수요 회복 등으로 상승합니다. 유가 급등은 몇 주 안에 헤드라인 CPI를 직접 밀어올리는 가장 빠른 물가 충격 경로입니다.",
        when_down="산유국 증산, 글로벌 경기 둔화로 인한 수요 감소 등으로 하락하며, 물가에 즉각적인 완화 요인으로 작용합니다.",
    ),
    IndicatorMeta(
        key="WAGE", name_ko="평균 시간당 임금 상승률 (YoY)", name_en="Average Hourly Earnings YoY",
        description="민간부문 근로자의 시간당 평균 임금이 전년 대비 얼마나 올랐는지를 보여줍니다. 임금은 특히 서비스업 물가의 가장 큰 비용 요소라, 임금 상승이 물가 상승으로, 물가 상승이 다시 임금 요구로 이어지는 '임금-물가 악순환' 여부를 판단하는 핵심 지표입니다.",
        direction="↑물가 압력 증가(단, 실질임금엔 긍정)", source="FRED: CES0500000003 (YoY 변환)", category=4,
        fred_id="CES0500000003", unit="%",
        when_up="구인난이 심하거나 노동조합의 임금 협상력이 강할 때 상승합니다. 3%대 후반을 넘는 임금 상승률이 장기간 이어지면 서비스물가·코어 인플레이션이 쉽게 안 꺾이는 배경이 됩니다.",
        when_down="노동시장이 식어 기업의 임금 인상 압박이 줄어들 때 하락합니다. 물가 안정에는 도움이 되지만, 너무 급격히 떨어지면 그 자체로 고용 둔화 신호로 해석됩니다.",
    ),
]


# ── 카테고리 통합 딕셔너리 ────────────────────────────────────────────
CATEGORY_META: dict[int, tuple[str, str, list[IndicatorMeta]]] = {
    1: ("① 소비자물가 (CPI)",        "가장 널리 쓰이는 헤드라인·근원 물가지표",         CPI_GROUP),
    2: ("② 연준 선호 지표 (PCE)",     "연준이 2% 목표의 기준으로 삼는 물가지표",         PCE_GROUP),
    3: ("③ 기대인플레이션·시장 신호", "시장과 소비자가 예상하는 향후 물가",             EXPECTATIONS),
    4: ("④ 공급망·비용 압력",         "소비자물가보다 먼저 움직이는 생산단계 신호",     COST_PRESSURE),
}

ALL_INDICATORS: list[IndicatorMeta] = (
    CPI_GROUP + PCE_GROUP + EXPECTATIONS + COST_PRESSURE
)
