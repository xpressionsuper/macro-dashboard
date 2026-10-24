#!/usr/bin/env python3
"""
config_investment.py ─ 투자 밸류에이션·심리 모니터 전역 설정 & 지표 메타데이터
=====================================================================
버핏 지표·CAPE 등 장기 밸류에이션 지표와, SKEW·마진부채 등 시장 심리·
레버리지 지표를 다룹니다. FRED에 없는 시리즈(CAPE, SKEW, 마진부채)는
main_combined.py에서 CBOE·multpl.com·FINRA 공개 페이지를 직접 읽어옵니다.
"""

from config import IndicatorMeta

# 내부 계산용 원시 시리즈 FRED ID (버핏 지표 계산에만 씀, 표에는 안 나옴)
EQUITY_MV_FRED_ID = "BOGZ1LM883164115Q"   # 미 전체 상장기업 시가총액 (연준 Z.1, 백만 달러)
GDP_FRED_ID        = "GDP"                 # 명목 GDP (십억 달러)


# ══════════════════════════════════════════════════════════════════════
#  ① 밸류에이션 지표
#  주식시장 전체가 실물경제·기업이익 대비 얼마나 비싼지
# ══════════════════════════════════════════════════════════════════════
VALUATION: list[IndicatorMeta] = [
    IndicatorMeta(
        key="BUFFETT", name_ko="버핏 지표 (시가총액/GDP)", name_en="Buffett Indicator",
        description="워런 버핏이 '장기적으로 가장 좋은 단일 밸류에이션 척도'라고 부른 지표로, 미국 전체 상장기업 시가총액을 명목 GDP로 나눈 값입니다. 주식시장 전체 몸값이 실물경제 생산 규모(GDP) 대비 얼마나 부풀어 있는지를 보여줘, 개별 종목이 아니라 시장 전체의 과열·저평가 여부를 가늠하는 데 씁니다. 100%를 '적정' 기준선으로 보는 경우가 많지만, 저금리 시대 이후 구조적으로 더 높은 수준에서 움직이는 경향도 있어 절대 수치보다 자기 역사적 추세 대비 위치로 해석하는 게 안전합니다.",
        direction="↑고평가", source="FRED: BOGZ1LM883164115Q ÷ GDP", category=1,
        fred_id=None, unit="%",
        when_up="주가가 기업 이익·경제성장 속도보다 더 빠르게 오를 때 상승합니다. 200%를 넘나드는 수준은 2000년 닷컴버블, 2021년 저금리 랠리 등 역사적으로 시장이 과열됐다고 평가받던 구간과 비슷한 수준입니다.",
        when_down="주가가 조정받거나(분모인 GDP는 안정적인데 분자인 시가총액이 하락), 경제성장이 주가 상승을 앞지를 때 하락합니다. 2008년, 2020년 초처럼 급락하는 구간은 시장이 저평가 영역에 들어섰다는 신호로 해석되기도 합니다.",
    ),
    IndicatorMeta(
        key="CAPE", name_ko="실러 CAPE 비율 (경기조정 PER)", name_en="Shiller CAPE Ratio",
        description="노벨경제학상 수상자 로버트 실러 교수가 만든 밸류에이션 지표로, S&P500 주가를 최근 10년 평균 인플레이션 조정 이익으로 나눈 값입니다. 한 해 이익만 쓰는 일반 PER과 달리 경기순환에 따른 이익 급등락(호황기 과대평가·불황기 과소평가)의 왜곡을 걸러내, 장기 밸류에이션을 볼 때 더 안정적인 척도로 평가받습니다.",
        direction="↑고평가", source="multpl.com (Shiller 데이터)", category=1,
        fred_id=None, unit="배",
        when_up="주가가 장기 평균 이익보다 훨씬 빠르게 오를 때 상승합니다. 역사적 평균은 약 17배이며, 30배를 넘는 구간은 1929년 대공황 직전, 2000년 닷컴버블 등 소수의 극단적 시기에만 나타났습니다.",
        when_down="주가 조정이나 기업이익 증가가 주가 상승을 앞지를 때 하락합니다. 10배 안팎까지 떨어지는 건 1920년대 말, 1980년대 초처럼 시장이 극도로 저평가됐던 드문 구간입니다.",
    ),
]


# ══════════════════════════════════════════════════════════════════════
#  ② 시장 심리·레버리지 지표
#  투자자들이 얼마나 낙관/공포에 빠져있고, 얼마나 빚을 내서 투자하는지
# ══════════════════════════════════════════════════════════════════════
SENTIMENT: list[IndicatorMeta] = [
    IndicatorMeta(
        key="SKEW", name_ko="CBOE SKEW 지수 (꼬리위험 지수)", name_en="CBOE SKEW Index",
        description="S&P500 옵션 가격에서 역산한 '블랙스완(예상 밖 대폭락) 보험료' 지수입니다. VIX가 평범한 변동성에 대한 공포를 잰다면, SKEW는 그보다 훨씬 드물고 극단적인 폭락 위험에 얼마나 대비하고 있는지를 보여줍니다. 100이 '꼬리위험 없음' 기준선이며, 평소엔 VIX가 낮아도 SKEW는 높게 유지되는 경우가 많습니다.",
        direction="↑극단적 폭락 대비 심리 증가", source="CBOE (SKEW_History.csv)", category=2,
        fred_id=None, unit="",
        when_up="옵션 시장 참여자들이 '평소엔 잠잠하지만 갑자기 크게 터질' 위험에 대비해 외가격 풋옵션을 사들일 때 상승합니다. 140~150을 넘나드는 수준은 시장이 표면적 평온함과 달리 내부적으로 꼬리위험을 크게 경계하고 있다는 신호입니다.",
        when_down="극단적 폭락 가능성에 대한 헤지 수요가 줄어들 때 100~120대의 평상시 수준으로 내려옵니다.",
    ),
    IndicatorMeta(
        key="MARGIN_DEBT", name_ko="증권사 마진부채 (레버리지 투자)", name_en="FINRA Margin Debt",
        description="투자자들이 증권사에서 빚을 내 주식을 사들인 총액입니다. 레버리지 투자가 얼마나 확산돼 있는지 보여주는 대표적인 과열 지표로, 2000년·2007년·2021년 등 시장 고점 직전마다 마진부채가 사상 최고치를 찍었던 전례가 있습니다. 빚으로 산 주식은 하락장에서 반대매매(마진콜)를 유발해 낙폭을 증폭시키는 위험 요인이기도 합니다.",
        direction="↑레버리지 과열", source="FINRA (margin-statistics)", category=2,
        fred_id=None, unit="$B",
        when_up="투자자들이 상승장에 대한 확신으로 빚을 내서까지 투자를 늘릴 때 증가합니다. 사상 최고치 경신 자체가 과열 신호로 자주 인용됩니다.",
        when_down="투자자들이 위험을 줄이려 빚을 상환하거나(디레버리징), 마진콜로 강제 청산될 때 감소합니다. 급격한 감소는 이미 조정장이 진행 중이라는 뜻인 경우가 많습니다.",
    ),
]


# ══════════════════════════════════════════════════════════════════════
#  ③ 정부부채
#  미국 연방정부 전체 부채와, 단기(재정증권)·장기(국채) 만기 구성
# ══════════════════════════════════════════════════════════════════════
GOV_DEBT: list[IndicatorMeta] = [
    IndicatorMeta(
        key="DEBT_TOTAL", name_ko="미국 연방정부 총부채", name_en="Total US Federal Debt",
        description="미국 연방정부가 발행한 국채 전체 잔액입니다. 재정적자가 누적될수록 늘어나며, 이자비용·부채한도 협상·국채 신용등급 논의의 출발점이 되는 가장 기본적인 재정 건전성 지표입니다.",
        direction="↑재정 부담 증가", source="TreasuryDirect (MSPD Table 1)", category=3,
        fred_id=None, unit="$B", compare_yoy=True,
        when_up="정부 지출이 세수보다 많은 재정적자가 계속되고, 그 적자를 메우기 위해 국채를 새로 발행할 때 늘어납니다.",
        when_down="드물게 재정흑자를 내거나(1990년대 후반처럼) 부채 상환이 발행보다 많을 때만 줄어듭니다. 실질적으로 감소하는 경우는 거의 없습니다.",
    ),
    IndicatorMeta(
        key="DEBT_SHORT", name_ko="단기국채 (재정증권, Bills)", name_en="Treasury Bills Outstanding",
        description="만기 1년 이하인 단기 국채(T-Bills) 발행 잔액입니다. 재무부가 당장 필요한 자금을 빠르게 조달할 때 주로 쓰며, 단기채 비중이 늘어나면 그만큼 자주 차환(롤오버)해야 해서 금리 변동에 취약해집니다.",
        direction="↑단기 조달 의존 증가", source="TreasuryDirect (MSPD Table 1) — Bills", category=3,
        fred_id=None, unit="$B",
        when_up="재무부가 부채한도 협상 등으로 장기채 발행이 여의치 않거나, 당장 급한 자금 수요를 메울 때 단기채 발행을 늘립니다. 단기채 비중이 과도하게 높아지면 금리 인상기에 이자비용이 빠르게 튀어 오르는 위험이 커집니다.",
        when_down="재무부가 장기채 발행을 늘려 단기 의존도를 낮추는 '만기 구성 조정(WAM 연장)'을 할 때 상대적으로 줄어듭니다.",
    ),
    IndicatorMeta(
        key="DEBT_LONG", name_ko="장기국채 (Notes+Bonds)", name_en="Treasury Notes & Bonds Outstanding",
        description="만기 2년 이상인 중장기 국채(Notes 2~10년, Bonds 20~30년) 발행 잔액을 합친 값입니다. 단기채보다 금리 변동 위험에서 자유롭고, 재정을 장기적으로 안정적으로 조달하고 있는지를 보여줍니다.",
        direction="↑장기 조달 비중 증가", source="TreasuryDirect (MSPD Table 1) — Notes+Bonds", category=3,
        fred_id=None, unit="$B",
        when_up="재무부가 낮은 장기금리를 활용해 만기를 미리 길게 잠가두거나(차환 위험 축소), 대규모 장기 프로젝트 재원을 조달할 때 늘어납니다.",
        when_down="장기금리가 높아 재무부가 장기채 발행을 꺼리고 단기채로 임시 조달할 때 상대적으로 둔화됩니다.",
    ),
    IndicatorMeta(
        key="INTEREST_GDP", name_ko="연방정부 이자비용/GDP", name_en="Federal Interest Expense / GDP",
        description="연방정부가 국채 이자로 지출하는 금액이 GDP 대비 얼마나 큰지를 보여줍니다. 부채 '잔액'보다 실제로 재정을 압박하는 정도를 더 직접적으로 보여주는 지표로, 이 비율이 계속 오르면 국방비·복지 등 다른 정책에 쓸 재정 여력이 구조적으로 줄어듭니다. 국채 발행 확대의 실질적인 '체감 임계점' 역할을 합니다.",
        direction="↑재정 압박 증가", source="FRED: A091RC1Q027SBEA ÷ GDP", category=3,
        fred_id=None, unit="%",
        when_up="부채 잔액 자체가 늘거나, 기존 저금리 국채가 만기 도래해 더 높은 금리로 차환(롤오버)될 때 상승합니다. 금리 인상기에는 부채가 그대로여도 이 비율만 빠르게 오를 수 있습니다.",
        when_down="금리가 하락해 차환 비용이 낮아지거나, 경제성장(GDP)이 부채 증가 속도보다 빠를 때 하락합니다.",
    ),
    IndicatorMeta(
        key="DEBT_SHORT_SHARE", name_ko="국채 중 단기채 비중", name_en="Short-Term Debt Share",
        description="발행된 국채(단기+장기) 중 만기 1년 이하 단기채(Bills)가 차지하는 비율입니다. 재무부는 통상 이 비중을 15~20% 안팎으로 관리하려 하는데, 이보다 높아지면 그만큼 자주 차환해야 해서 금리 변동에 취약해지고, 금리 상승기에는 이자비용(위 지표)이 더 빠르게 튀어 오르는 구조가 됩니다.",
        direction="↑차환 위험 증가", source="파생 계산 (DEBT_SHORT ÷ (DEBT_SHORT+DEBT_LONG))", category=3,
        fred_id=None, unit="%",
        when_up="재무부가 장기채 수요 부진이나 부채한도 문제로 단기채 발행에 의존할 때 상승합니다. 20%를 크게 웃돌면 재무부 자문위원회(TBAC)도 우려를 표하는 수준입니다.",
        when_down="재무부가 의도적으로 장기채 발행을 늘려 만기 구성을 다변화(WAM 연장)할 때 하락합니다.",
    ),
]


CATEGORY_META: dict[int, tuple[str, str, list[IndicatorMeta]]] = {
    1: (
        "① 밸류에이션 지표",
        "주식시장 전체가 실물경제·기업이익 대비 얼마나 비싼지",
        VALUATION,
    ),
    2: (
        "② 시장 심리·레버리지 지표",
        "투자자들이 얼마나 낙관/공포에 빠져있고, 얼마나 빚내서 투자하는지",
        SENTIMENT,
    ),
    3: (
        "③ 정부부채",
        "미국 연방정부 총부채와 단기·장기 만기 구성",
        GOV_DEBT,
    ),
}

ALL_INDICATORS: list[IndicatorMeta] = VALUATION + SENTIMENT + GOV_DEBT
