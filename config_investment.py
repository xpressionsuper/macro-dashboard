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


# ══════════════════════════════════════════════════════════════════════
#  ④ 중앙은행 금 매입
#  IMF International Liquidity 국가별 금 보유량 (data/imf_gold.py, 키 불필요)
# ══════════════════════════════════════════════════════════════════════
CB_GOLD_BASE_MONTH = "2009-12"   # 이 달 말 보유량을 0으로 두고 누적 (2010년부터 중앙은행이 순매수로 전환)
CB_GOLD_TOP_N = 10               # 차트마다 나라 수. 순위는 매 실행마다 새로 뽑음 (data/imf_gold.py)

# IMF 금 보유량에 있지만 나라가 아닌 3글자 코드 — 순위에서 뺌 (G로 시작하는 지역 합계는 코드 길이로 걸러짐)
CB_GOLD_NON_COUNTRIES = {
    "BIS",   # 국제결제은행
    "IMF",   # 국제통화기금
    "EZB",   # 유럽중앙은행
    "BCE",   # 서아프리카 중앙은행
    "BEA",   # 중앙아프리카 중앙은행
}

# GDP 순위 조회(IMF WEO)가 실패했을 때 쓸 목록 (2025년 명목 GDP 순)
CB_GOLD_GDP_FALLBACK = ["USA", "CHN", "DEU", "JPN", "GBR", "IND", "FRA", "RUS", "ITA", "CAN"]

# IMF 국가 코드 → 한글 이름. 순위에 새로 들어온 나라가 여기 없으면 코드 그대로 표시되니 추가하면 됨
CB_GOLD_NAMES: dict[str, str] = {
    "USA": "미국", "CHN": "중국", "DEU": "독일", "JPN": "일본", "GBR": "영국", "IND": "인도", "FRA": "프랑스",
    "RUS": "러시아", "ITA": "이탈리아", "CAN": "캐나다", "BRA": "브라질", "ESP": "스페인", "KOR": "한국",
    "MEX": "멕시코", "AUS": "호주", "IDN": "인도네시아", "TUR": "튀르키예", "SAU": "사우디아라비아",
    "NLD": "네덜란드", "CHE": "스위스", "POL": "폴란드", "TWN": "대만", "BEL": "벨기에", "SWE": "스웨덴",
    "ARG": "아르헨티나", "IRL": "아일랜드", "AUT": "오스트리아", "THA": "태국", "ARE": "아랍에미리트",
    "NOR": "노르웨이", "ISR": "이스라엘", "SGP": "싱가포르", "PHL": "필리핀", "VNM": "베트남",
    "MYS": "말레이시아", "BGD": "방글라데시", "EGY": "이집트", "ZAF": "남아공", "UZB": "우즈베키스탄",
    "KAZ": "카자흐스탄", "IRQ": "이라크", "QAT": "카타르", "HUN": "헝가리", "CZE": "체코", "SRB": "세르비아",
    "KGZ": "키르기스스탄", "MNG": "몽골", "GHA": "가나", "JOR": "요르단", "LBY": "리비아", "PRT": "포르투갈",
    "GRC": "그리스", "COL": "콜롬비아", "CHL": "칠레", "PER": "페루", "PAK": "파키스탄", "NGA": "나이지리아",
    "DZA": "알제리", "KWT": "쿠웨이트", "LBN": "레바논", "VEN": "베네수엘라", "ECU": "에콰도르",
    "SVK": "슬로바키아", "ROU": "루마니아", "UKR": "우크라이나", "BLR": "벨라루스", "AZE": "아제르바이잔",
    "TJK": "타지키스탄", "MAR": "모로코", "DNK": "덴마크", "FIN": "핀란드", "NZL": "뉴질랜드", "HKG": "홍콩",
    "OMN": "오만", "BHR": "바레인", "CYP": "키프로스", "GTM": "과테말라", "KHM": "캄보디아", "LKA": "스리랑카",
}

_GOLD_SOURCE = "IMF International Liquidity (금 보유량, 순금 트로이온스 → 톤)"
_GOLD_UP = ("달러 자산 의존을 줄이려는 외환보유액 다변화, 제재·자산동결 위험 회피(2022년 러시아 외환보유액 동결 이후 특히 부각), "
            "자국 통화 신뢰 보강 목적으로 중앙은행이 금을 사들일 때 늘어납니다. 꾸준한 매입은 금 가격의 하방을 받쳐주는 수요로 해석됩니다.")
_GOLD_DOWN = ("외환보유액이 부족해 금을 팔아 달러를 마련하거나(환율 방어), 금 가격 급등기에 차익을 실현할 때 줄어듭니다. "
              "우즈베키스탄·카자흐스탄처럼 국내 금광 생산분을 사들였다가 시세에 따라 되파는 나라는 매달 출렁임이 큽니다.")
_CUM_DESC = ("IMF에 각국 중앙은행이 신고한 금 보유량을 2009년 말과 비교한 값입니다. 2010년부터 쌓아 온 순매입량이라 보면 됩니다. "
             "어느 나라가 들어갈지는 매 실행마다 최신 데이터로 순매입이 많은 순서대로 다시 뽑습니다(국제기구 제외). "
             "세계금협회(WGC)는 신고되지 않은 매입까지 추정해 합치지만, 이 차트는 신고된 수치만 씁니다. "
             "그래서 미보고 매입이 많은 2022년 이후 국가 합계는 WGC 집계보다 작게 나옵니다. "
             "러시아는 2025년 말 이후 신고가 끊겨 선이 멈춰 있고, 튀르키예는 시중은행이 맡긴 금과 스와프 물량이 섞여 있어 출렁임이 큽니다. "
             "각국 신고가 IMF에 반영되기까지 보통 1~2개월이 걸립니다.")
_GDP_DESC = ("IMF 세계경제전망(WEO)의 지난해 명목 GDP 기준 상위 10개국의 중앙은행 금 보유량입니다. 2010년 이후 구간만 그렸고, "
             "GDP 순위도 매 실행마다 다시 확인합니다. "
             "미국·독일·이탈리아·프랑스는 1950~60년대 브레턴우즈 체제 때 쌓은 금을 그대로 가진 채 거의 평평하고, "
             "중국·러시아·인도는 아래에서 올라오며 격차를 좁혀 왔습니다. "
             "독일이 조금씩 줄어든 건 기념주화 제작용 금을 정부에 넘겨 온 몫입니다. "
             "신고된 수치 기준이라 미보고 매입은 빠져 있습니다.")


def _gold(prefix: str, desc: str, what: str, rank: int) -> IndicatorMeta:
    """순위 자리 하나. 어느 나라인지(name_ko)는 실행할 때 data/imf_gold.py가 채움."""
    first = rank == 1   # 설명은 차트당 한 번만 (나머지 자리는 같은 설명이라 비워 둠)
    return IndicatorMeta(
        key=f"{prefix}{rank}", name_ko=f"{rank}위", name_en=f"By Country Central Bank Gold — {what}",
        description=desc if first else "", direction="↑금 보유 확대", source=_GOLD_SOURCE, category=4,
        unit="t", chg_as_point=True, when_up=_GOLD_UP, when_down=_GOLD_DOWN,
    )


_RANKS = range(1, CB_GOLD_TOP_N + 1)
CB_GOLD: list[IndicatorMeta] = (
    [_gold("CB_GOLD_CUM_", _CUM_DESC, "Cumulative Net Purchases since 2010", r) for r in _RANKS]
    + [_gold("CB_GOLD_GDP_", _GDP_DESC, "Gold Reserves of the 10 Largest Economies", r) for r in _RANKS]
)


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
    4: (
        "④ 중앙은행 금 매입",
        "각국 중앙은행이 IMF에 신고한 금 보유량 기준 2010년 이후 순매입 상위 10개국의 누적 순매입과 GDP 상위 10개국의 보유량 (톤, 월간 · 순위는 매 실행마다 갱신)",
        CB_GOLD,
    ),
}

ALL_INDICATORS: list[IndicatorMeta] = VALUATION + SENTIMENT + GOV_DEBT + CB_GOLD
