#!/usr/bin/env python3
"""
config_recession.py ─ 경기침체 모니터 전역 설정 & 지표 메타데이터
=====================================================================
경기침체 선행·동행 지표를 다룹니다. IndicatorMeta 구조체는
config.py(유동성 모니터)와 동일한 것을 그대로 재사용합니다.
"""

from config import FRED_API_KEY, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, IndicatorMeta

OUTPUT_HTML   = "recession_dashboard.html"
LOOKBACK_DAYS = 365 * 10
REQUEST_DELAY = 0.4


# ══════════════════════════════════════════════════════════════════════
#  ① 노동시장 지표
#  가장 널리 쓰이는 침체 선행·확진 신호. 특히 실업률 계열은 후행처럼
#  보이지만 Sahm Rule 덕분에 실시간 확진 신호로도 쓰입니다.
# ══════════════════════════════════════════════════════════════════════
LABOR_MARKET: list[IndicatorMeta] = [
    IndicatorMeta(
        key="ICSA_R", name_ko="신규 실업수당 청구건수", name_en="Initial Jobless Claims",
        description="한 주 동안 새로 실업수당을 신청한 사람 수입니다. 매주 발표되어 월간 지표보다 반응이 훨씬 빠르고, 기업의 해고 결정이 통계에 반영되기까지 걸리는 시차가 가장 짧은 지표 중 하나입니다.",
        direction="↑스트레스 증가", source="FRED: ICSA", category=1,
        fred_id="ICSA", unit="천 건", divisor=1_000,
        when_up="기업들이 감원을 늘리기 시작할 때 상승합니다. 몇 주 반짝 튀는 건 계절 요인일 수 있지만, 수개월에 걸쳐 추세적으로 상승하면 노동시장이 식고 있다는 강한 신호입니다.",
        when_down="고용시장이 견고해 해고가 적을 때 낮은 수준(역사적으로 20만 건 안팎)을 유지합니다.",
    ),
    IndicatorMeta(
        key="CCSA", name_ko="지속 실업수당 청구건수", name_en="Continuing Jobless Claims",
        description="실업수당을 신규 신청한 뒤에도 계속 받고 있는 사람 수입니다. 신규 청구건수가 '해고가 늘었는지'를 보여준다면, 이 지표는 '해고된 사람이 새 일자리를 못 구하고 있는지'를 보여줍니다.",
        direction="↑침체 신호", source="FRED: CCSA", category=1,
        fred_id="CCSA", unit="천 명", divisor=1_000,
        when_up="구직자가 새 일자리를 구하는 데 시간이 오래 걸릴 때 상승합니다. 채용 공고가 줄고 기업들이 신중해지는 국면에서 신규 청구보다 늦게, 하지만 꾸준히 오르는 경향이 있습니다.",
        when_down="구인난이 있거나 재취업이 빠를 때 하락합니다.",
    ),
    IndicatorMeta(
        key="PAYEMS", name_ko="비농업 고용자수", name_en="Nonfarm Payrolls",
        description="농업을 제외한 전체 취업자 수로, 미국에서 가장 널리 인용되는 '고용지표'입니다. 매월 첫째 주 금요일 발표되며, 시장 전체가 예상치와의 격차에 민감하게 반응합니다.",
        direction="↓침체 신호", source="FRED: PAYEMS", category=1,
        fred_id="PAYEMS", unit="천 명", chg_as_point=True,
        when_up="기업들이 계속 순채용(채용>해고)을 이어갈 때 증가합니다. 증가 '속도'가 둔화되는 것 자체도 경기 둔화 초기 신호로 해석됩니다.",
        when_down="순감소(월간 감소)로 전환되는 것은 매우 드문 사건으로, 대부분의 전후 미국 침체 국면에서 나타났습니다.",
    ),
    IndicatorMeta(
        key="JOLTS", name_ko="구인건수 (JOLTS)", name_en="Job Openings",
        description="전체 산업에 걸쳐 현재 채용 중인 빈 일자리 수입니다. 실업률은 '일자리를 잃은 사람'만 보여주지만, 이 지표는 '기업이 얼마나 사람을 더 뽑고 싶어하는지' 수요 쪽을 직접 보여줘 노동시장 과열/냉각을 실업률보다 먼저 포착하는 경우가 많습니다.",
        direction="↓침체 신호", source="FRED: JTSJOL", category=1,
        fred_id="JTSJOL", unit="천 건", chg_as_point=True,
        when_up="기업들이 사업 확장을 위해 채용 계획을 늘릴 때 증가합니다. 팬데믹 이후 구인난 시기 사상 최고치를 기록했습니다.",
        when_down="기업들이 채용 계획을 접거나 미룰 때 감소합니다. 실제 해고로 이어지기 전, 가장 먼저 나타나는 조용한 냉각 신호입니다.",
    ),
    IndicatorMeta(
        key="QUITS", name_ko="자발적 이직률 (Quits Rate)", name_en="Quits Rate",
        description="한 달 동안 스스로 회사를 그만둔 근로자 비율입니다. 근로자들이 '더 나은 일자리를 쉽게 구할 수 있다'는 자신감이 있을 때만 자발적으로 이직하므로, 노동시장 자신감을 보여주는 대리지표로 파월 의장이 자주 인용합니다.",
        direction="↓침체 신호", source="FRED: JTSQUR", category=1,
        fred_id="JTSQUR", unit="%",
        when_up="구인난이 심해 이직이 쉬울 때 상승합니다. 임금 상승 압력과도 연동됩니다.",
        when_down="새 일자리를 구하기 어려워 근로자들이 현재 직장에 안주할 때 하락합니다. 노동시장 자신감 위축의 조용한 신호입니다.",
    ),
]


# ══════════════════════════════════════════════════════════════════════
#  ② 소비·생산 지표
#  미국 GDP의 약 70%를 차지하는 소비와, 산업 활동의 실물 체감온도
# ══════════════════════════════════════════════════════════════════════
CONSUMPTION_PRODUCTION: list[IndicatorMeta] = [
    IndicatorMeta(
        key="RSAFS", name_ko="소매판매", name_en="Retail Sales",
        description="자동차, 식당, 온라인 쇼핑 등을 포함한 소매업 전체 매출액입니다. 미국 경제의 약 70%를 차지하는 소비지출을 매달 가장 먼저, 가장 넓은 범위로 보여주는 지표입니다.",
        direction="↓침체 신호", source="FRED: RSAFS", category=2,
        fred_id="RSAFS", unit="$B", divisor=1_000,
        when_up="소비자들이 지갑을 계속 열 때(임금 상승, 낮은 실업률, 신용카드 사용 확대 등) 증가합니다. 인플레이션 조정 전 명목 수치라, 물가가 오르면 실질 소비가 줄어도 이 수치는 늘어날 수 있다는 점은 유의해야 합니다.",
        when_down="소비자들이 지출을 줄이거나 저축을 늘릴 때(고용 불안, 고금리, 신용카드 연체 증가 등) 감소·정체합니다.",
    ),
    IndicatorMeta(
        key="INDPRO", name_ko="산업생산지수", name_en="Industrial Production",
        description="제조업·광업·전력·가스 등 미국 전체 산업 부문의 실물 생산량을 지수화한 것입니다. 경기순환위원회(NBER)가 침체를 공식 판정할 때 참고하는 핵심 월간 지표 중 하나입니다.",
        direction="↓침체 신호", source="FRED: INDPRO", category=2,
        fred_id="INDPRO", unit="",
        when_up="공장들이 가동률을 높이고 생산을 늘릴 때(수요 증가, 재고 보충 등) 상승합니다.",
        when_down="수요 둔화로 공장 가동을 줄이거나, 재고가 쌓여 생산을 조절할 때 하락합니다. 몇 달 연속 하락하면 실물경기 침체가 이미 진행 중일 가능성이 큽니다.",
    ),
    IndicatorMeta(
        key="UMCSENT", name_ko="미시간대 소비자심리지수", name_en="U. Michigan Consumer Sentiment",
        description="일반 소비자들에게 현재와 향후 경기·가계 재정 상황을 설문해 만드는 심리 지수입니다. 실제 지출보다 먼저 움직이는 경향이 있어, 소비 둔화를 예고하는 선행지표로 쓰입니다.",
        direction="↓침체 신호", source="FRED: UMCSENT", category=2,
        fred_id="UMCSENT", unit="",
        when_up="고용·주식시장·물가 안정에 대한 소비자들의 낙관이 커질 때 상승합니다.",
        when_down="물가 상승(인플레이션 피로), 고용 불안, 자산가격 하락 등으로 소비자들이 미래를 비관적으로 볼 때 하락합니다. 실제 지출 위축보다 몇 달 먼저 나빠지는 경우가 많습니다.",
    ),
    IndicatorMeta(
        key="DGORDER", name_ko="내구재 신규주문", name_en="Durable Goods Orders",
        description="자동차, 가전, 기계 등 3년 이상 쓰는 내구재에 대한 제조업체의 신규 주문액입니다. 기업들이 향후 수요를 어떻게 보는지 가늠할 수 있는 기업 투자심리 지표로, 변동성이 커서 추세로 봐야 합니다.",
        direction="↓침체 신호", source="FRED: DGORDER", category=2,
        fred_id="DGORDER", unit="$B", divisor=1_000,
        when_up="기업들이 향후 수요 확대를 예상해 설비·장비 투자를 늘릴 때 증가합니다.",
        when_down="기업들이 불확실성 때문에 투자·주문을 미룰 때 감소합니다. 항공기 등 대형 품목 하나로 월별 변동이 크므로 단발성 급락보다 몇 달간의 추세가 중요합니다.",
    ),
    IndicatorMeta(
        key="CCDELQ", name_ko="신용카드 연체율", name_en="Credit Card Delinquency Rate",
        description="전체 신용카드 대출 중 90일 이상 연체된 비율입니다. 가계가 실제로 얼마나 재정적 압박을 받고 있는지 보여주는 후행 확진 지표로, 이 수치가 오르기 시작하면 소비 위축이 이미 진행 중일 가능성이 큽니다.",
        direction="↑침체 신호", source="FRED: DRCCLACBS", category=2,
        fred_id="DRCCLACBS", unit="%",
        when_up="고금리·생활비 부담으로 가계가 빚을 제때 못 갚기 시작할 때 상승합니다. 저소득층부터 먼저 반응하는 경향이 있어 소비 여력 악화의 초기 신호로 봅니다.",
        when_down="고용·임금 여건이 좋아 가계 상환 능력이 안정적일 때 낮게 유지됩니다.",
    ),
]


# ── 카테고리 통합 딕셔너리 ────────────────────────────────────────────
CATEGORY_META: dict[int, tuple[str, str, list[IndicatorMeta]]] = {
    1: ("① 노동시장 지표",       "채용·해고·실업 추세로 보는 경기 확진·선행 신호",     LABOR_MARKET),
    2: ("② 소비·생산 지표",      "GDP의 핵심 축인 소비지출과 실물 생산 활동",         CONSUMPTION_PRODUCTION),
}

ALL_INDICATORS: list[IndicatorMeta] = (
    LABOR_MARKET + CONSUMPTION_PRODUCTION
)
