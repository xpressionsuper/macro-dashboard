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
        key="UNRATE", name_ko="실업률", name_en="Unemployment Rate",
        description="전체 노동인구 중 실업자 비율입니다. 대표적인 경기 후행지표지만, 방향이 바뀌는 시점(저점 통과 후 반등)은 침체 시작과 거의 동시에 나타나는 경우가 많아 확진 신호로 자주 인용됩니다.",
        direction="↑침체 신호", source="FRED: UNRATE", category=1,
        fred_id="UNRATE", unit="%",
        when_up="기업들이 채용을 줄이거나 해고를 늘릴 때 상승합니다. 저점 대비 소폭(0.3~0.5%p)만 올라도 시장이 민감하게 반응하는데, 실업률은 한번 오르기 시작하면 관성적으로 계속 오르는 경향이 있기 때문입니다.",
        when_down="고용시장이 좋아 기업이 계속 채용을 늘릴 때 하락합니다. 완전고용에 가까운 3%대 후반~4%대 초반이 최근 수십 년간의 저점 구간이었습니다.",
    ),
    IndicatorMeta(
        key="SAHM", name_ko="Sahm Rule 경기침체 지표", name_en="Sahm Rule Indicator",
        description="실업률의 최근 3개월 평균이, 지난 12개월 중 최저치보다 0.5%p 이상 높아지면 경기침체가 '이미' 시작된 것으로 판정하는 규칙입니다. 1970년 이후 미국의 모든 침체를 오탐 없이 실시간으로 잡아낸 것으로 유명합니다.",
        direction="↑침체 확정 신호", source="FRED: SAHMREALTIME", category=1,
        fred_id="SAHMREALTIME", unit="%p",
        when_up="실업률이 저점 대비 빠르게 튀어 오를 때 상승하며, 0.50%p를 넘는 순간 규칙상 '침체 진입'으로 간주됩니다. 다른 어떤 지표보다도 명확한 이진(0.5 이상/미만) 판정 기준을 제공합니다.",
        when_down="실업률이 안정적이거나 완만하게만 움직일 때 0 근처 낮은 값을 유지합니다.",
    ),
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


# ══════════════════════════════════════════════════════════════════════
#  ③ 주택시장 지표
#  금리에 가장 민감하게 반응하며, 역사적으로 침체에 앞서 꺾이는 경우가 많음
# ══════════════════════════════════════════════════════════════════════
HOUSING: list[IndicatorMeta] = [
    IndicatorMeta(
        key="HOUST", name_ko="주택착공건수", name_en="Housing Starts",
        description="한 달 동안 새로 착공된 민간 주택 수(연율 환산)입니다. 주택은 금리에 가장 민감한 내구재라, 연준이 금리를 올리기 시작하면 다른 어떤 부문보다 먼저 냉각되는 경향이 있어 대표적인 경기 선행지표로 꼽힙니다.",
        direction="↓침체 신호", source="FRED: HOUST", category=3,
        fred_id="HOUST", unit="천 채",
        when_up="주택담보대출 금리가 낮거나 주택 수요가 견조할 때 건설사들이 착공을 늘립니다.",
        when_down="모기지 금리가 오르거나 주택 수요가 위축될 때 건설사들이 착공을 줄입니다. 2006~2007년 주택착공 급감이 2008년 금융위기의 가장 이른 전조 중 하나였습니다.",
    ),
    IndicatorMeta(
        key="PERMIT", name_ko="건축허가건수", name_en="Building Permits",
        description="착공 전 단계인 건축허가 신청 건수로, 착공보다도 한발 더 앞선 선행지표입니다. 허가를 받고도 실제 착공까지는 시차가 있어, 착공건수의 방향을 미리 가늠하는 데 쓰입니다.",
        direction="↓침체 신호", source="FRED: PERMIT", category=3,
        fred_id="PERMIT", unit="천 채",
        when_up="건설사들이 향후 주택 수요를 낙관해 신규 프로젝트를 계획할 때 증가합니다.",
        when_down="금리 상승이나 수요 둔화를 예상해 신규 프로젝트 계획을 줄일 때 감소합니다. 착공건수보다 먼저 꺾이는 경우가 많아 조기 경보로 유용합니다.",
    ),
    IndicatorMeta(
        key="CASESHILLER", name_ko="케이스실러 주택가격지수", name_en="Case-Shiller Home Price Index",
        description="착공·허가건수가 '앞으로 지을 집'을 보여준다면, 이 지수는 이미 팔린 기존주택의 실제 거래가격 변동을 보여줍니다. 미국 가계 자산의 상당 부분이 부동산에 묶여 있어, 집값 하락은 소비심리·소비지출에 직접적인 '자산효과'로 이어집니다.",
        direction="↓자산가치·소비 위축 신호", source="FRED: CSUSHPINSA", category=3,
        fred_id="CSUSHPINSA", unit="", compare_yoy=True,
        when_up="주택 수요가 공급을 웃돌 때(저금리, 인구 유입 등) 상승합니다. 집값 상승은 소비자들이 부를 더 쓰게 만드는 자산효과로 소비를 뒷받침합니다.",
        when_down="고금리로 매수 여력이 줄거나 매물이 늘어날 때 하락합니다. 2008년처럼 급락하면 가계 자산이 훼손되며 소비 위축·금융시스템 불안으로 번질 수 있습니다.",
    ),
]


# ══════════════════════════════════════════════════════════════════════
#  ④ 종합 선행·모델 지표
#  여러 지표를 하나로 압축했거나, 통계 모델로 직접 침체 확률을 계산한 지표
# ══════════════════════════════════════════════════════════════════════
COMPOSITE_LEADING: list[IndicatorMeta] = [
    IndicatorMeta(
        key="GDP", name_ko="실질 GDP 성장률", name_en="Real GDP",
        description="물가 영향을 제거한 미국 전체 경제 생산 규모로, 다른 모든 지표가 결국 예측하려는 '최종 스코어보드'입니다. NBER의 공식 침체 판정도 이 지표(및 관련 월간지표들)를 기반으로 이루어집니다. 분기 발표라 다른 월간·주간 지표보다 훨씬 늦게 나오는 대표적 후행지표입니다.",
        direction="↓침체 확정 신호", source="FRED: GDPC1", category=4,
        fred_id="GDPC1", unit="$B", divisor=1_000, compare_yoy=True,
        when_up="소비·투자·정부지출·순수출이 전반적으로 늘어날 때 증가합니다. 2개 분기 연속 감소(마이너스 성장)가 흔히 통용되는 '기술적 침체'의 기준선입니다.",
        when_down="여러 부문에서 동시에 위축이 나타날 때 감소합니다. 다른 지표들이 이미 몇 달 전부터 경고했던 내용을 뒤늦게 통계로 확인시켜주는 역할을 합니다.",
    ),
    IndicatorMeta(
        key="T10Y2Y_R", name_ko="수익률 곡선 (10Y−2Y)", name_en="Yield Curve",
        description="10년물 국채금리에서 2년물 금리를 뺀 값입니다. 연준이 단기금리를 강하게 올려 경기 둔화를 예고하면 장단기 금리 관계가 역전(음수)되는데, 1955년 이후 미국의 거의 모든 경기침체 앞에 이 역전 현상이 선행했던 가장 유명한 침체 선행지표입니다.",
        direction="↓역전 시 침체 신호", source="FRED: T10Y2Y", category=4,
        fred_id="T10Y2Y", unit="%",
        when_up="침체 우려가 완화되며 정상화되거나(장기금리가 더 오름), 연준이 금리를 내려 단기금리가 더 빠르게 떨어질 때(역전 해소) 상승합니다. 역설적으로 역전 해소 시점이 실제 침체 진입 직전인 경우가 많습니다.",
        when_down="연준이 단기금리를 계속 올려 단기금리가 장기금리를 웃돌 때(역전) 하락·음수로 전환합니다.",
    ),
    IndicatorMeta(
        key="CFNAI", name_ko="시카고 연은 국가활동지수", name_en="Chicago Fed National Activity Index",
        description="생산, 고용, 개인소비, 판매·주문·재고 등 85개 경제지표를 하나로 압축한 종합 경기활동지수입니다. 0이 '장기평균 성장세', 마이너스가 '평균 이하 성장'을 의미하며, 3개월 평균이 -0.7 이하로 떨어지면 통계적으로 침체 국면일 가능성이 매우 커집니다.",
        direction="↓침체 신호", source="FRED: CFNAI", category=4,
        fred_id="CFNAI", unit="", chg_as_point=True,
        when_up="생산·고용·소비 등 광범위한 부문에서 동시에 개선이 나타날 때 상승합니다.",
        when_down="여러 부문에서 동시다발적으로 위축이 나타날 때 하락합니다. -0.7 이하 구간은 역사적으로 침체 국면과 상당히 겹칩니다.",
    ),
    IndicatorMeta(
        key="RECPROB", name_ko="연준 침체확률 모델", name_en="Smoothed Recession Probabilities",
        description="Chauvet-Piger 모델이 산업생산, 고용, 소득, 소매판매 등을 결합해 통계적으로 계산한 '현재 미국이 경기침체 국면에 있을 확률'입니다. NBER의 공식 침체 판정과 사후적으로 매우 잘 들어맞아 온 모델 기반 지표입니다.",
        direction="↑침체 확정 신호", source="FRED: RECPROUSM156N", category=4,
        fred_id="RECPROUSM156N", unit="%",
        when_up="생산·고용·소득·판매 지표가 동시에 나빠질 때 확률이 급격히 상승합니다. 통상 20~30%를 넘어서면 침체 국면 진입 가능성을 진지하게 봐야 합니다.",
        when_down="경제지표들이 고르게 견조할 때 0에 가까운 낮은 확률을 유지합니다.",
    ),
]


# ── 카테고리 통합 딕셔너리 ────────────────────────────────────────────
CATEGORY_META: dict[int, tuple[str, str, list[IndicatorMeta]]] = {
    1: ("① 노동시장 지표",       "채용·해고·실업 추세로 보는 경기 확진·선행 신호",     LABOR_MARKET),
    2: ("② 소비·생산 지표",      "GDP의 핵심 축인 소비지출과 실물 생산 활동",         CONSUMPTION_PRODUCTION),
    3: ("③ 주택시장 지표",       "금리에 가장 민감하게 반응하는 선행 부문",            HOUSING),
    4: ("④ 종합 선행·모델 지표", "여러 지표를 압축했거나 통계 모델로 계산한 침체 신호", COMPOSITE_LEADING),
}

ALL_INDICATORS: list[IndicatorMeta] = (
    LABOR_MARKET + CONSUMPTION_PRODUCTION + HOUSING + COMPOSITE_LEADING
)
