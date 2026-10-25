#!/usr/bin/env python3
"""
config_korea.py ─ 한국 증시 수급 모니터 전역 설정 & 지표 메타데이터
=====================================================================
KOSPI에서 누가(외국인·기관·개인·기타법인) 사고 누가 파는지, 기관 안에서는
어느 주체(연기금·금융투자·투신 등)가 움직이는지를 누적 순매수로 봅니다.
데이터는 main_combined.py에서 한국투자증권 Open API로 가져옵니다
(KIS_APP_KEY/KIS_APP_SECRET 필요 — KRX 사이트 자동 수집은 약관상 금지).
"""

from config import IndicatorMeta

KR_FLOW_START = "20200101"   # 누적 순매수의 기준 시작일 (이날부터 0에서 누적)
_SOURCE = "한국투자증권 Open API (시장별 투자자매매동향)"


def _net(key, name, name_en, category, description, when_up, when_down) -> IndicatorMeta:
    """누적 순매수 지표 공통 설정 (단위 조원, 0을 오가므로 % 대신 포인트 변화)."""
    return IndicatorMeta(
        key=key, name_ko=f"{name} 누적 순매수", name_en=f"{name_en} Cumulative Net Buying",
        description=description, direction=f"↑{name} 자금 유입", source=_SOURCE, category=category,
        unit="조원", chg_as_point=True, when_up=when_up, when_down=when_down,
    )


# ══════════════════════════════════════════════════════════════════════
#  외국인 증권투자자금 유출입 (달러) — 한국은행 ECOS 국제수지(301Y013) 금융계정
#  증권투자(부채) 월별, 2020년 1월부터 누적 (ECOS_API_KEY 필요)
# ══════════════════════════════════════════════════════════════════════
ECOS_BOP_TABLE = "301Y013"
ECOS_START_MONTH = "202001"

_BOP_SOURCE = "한국은행 ECOS 국제수지 (금융계정 · 증권투자 부채)"
_BOP_UP = ("외국인이 달러를 원화로 바꿔 한국 주식·채권을 사들일 때 상승합니다. 원화 강세 기대, 한국 기업 이익 개선, "
           "글로벌 위험선호 국면, 세계국채지수(WGBI) 편입 같은 채권 수요 요인이 대표적입니다.")
_BOP_DOWN = ("외국인이 한국 주식·채권을 팔고 받은 원화를 달러로 바꿔 나갈 때 하락합니다. 이 과정 자체가 달러 수요를 늘려 "
             "원/달러 환율을 끌어올리고(원화 약세), 원화 약세가 다시 외국인 매도를 부르는 악순환이 생기기도 합니다.")


def _bop(key, name, name_en, description) -> IndicatorMeta:
    return IndicatorMeta(
        key=key, name_ko=name, name_en=f"Foreign Portfolio Investment Flows — {name_en} (Cumulative)",
        description=description, direction="↑외국인 자금 유입", source=_BOP_SOURCE, category=1,
        unit="억달러", chg_as_point=True, when_up=_BOP_UP, when_down=_BOP_DOWN,
    )


# 지표 키 → ECOS 국제수지 항목 코드 (백만달러, 월별 순유출입)
ECOS_BOP_ITEMS: dict[str, str] = {
    "KR_BOP_TOTAL_NET":  "BOPF22000000",   # 증권투자(부채) 합계
    "KR_BOP_EQUITY_NET": "BOPF22100000",   # 주식(부채)
    "KR_BOP_DEBT_NET":   "BOPF22200000",   # 부채성증권(부채) = 채권 등
}

FOREIGN_USD: list[IndicatorMeta] = [
    _bop("KR_BOP_TOTAL_NET", "합계", "Total",
         "한국은행 국제수지 통계의 '증권투자(부채)', 즉 외국인이 국내 주식·채권에 투자한 자금의 월별 순유출입(달러)을 "
         "2020년 1월부터 누적한 값입니다. 위로 가면 외국인 달러 자금이 들어오는 중, 아래로 가면 원화를 달러로 바꿔 빠져나가는 중입니다. "
         "한국은행이 매달 보도자료(국제금융·외환시장 동향)로 내는 '외국인 증권투자자금' 속보치와는 집계 기준이 달라 금액이 다를 수 있으며, "
         "국제수지 발표 일정상 약 2개월 늦게 반영됩니다."),
    _bop("KR_BOP_EQUITY_NET", "주식", "Equity",
         "외국인의 국내 주식 투자 자금 순유출입 누적입니다. 원화 기준 외국인 순매수와 방향은 비슷하지만, 환율 변동을 반영한 실제 달러 규모입니다."),
    _bop("KR_BOP_DEBT_NET", "채권", "Debt Securities",
         "외국인의 국내 채권(부채성증권) 투자 자금 순유출입 누적입니다. 주식과 달리 각국 중앙은행·국부펀드 등 장기 투자자 비중이 커서, "
         "주식에서 돈이 빠질 때도 채권으로는 들어오는 경우가 많습니다."),
]


# ══════════════════════════════════════════════════════════════════════
#  ① 투자 주체별 수급
# ══════════════════════════════════════════════════════════════════════
INVESTOR_FLOW: list[IndicatorMeta] = [
    _net("KR_FOREIGN_NET", "외국인", "Foreign", 1,
         "외국인이 순매수(매수금액−매도금액)한 금액을 매일 더해 나간 값입니다. 선의 절대 높이보다 기울기가 중요해, 오른쪽 위로 가면 외국인 자금이 들어오는 중, 아래로 가면 빠져나가는 중이라는 뜻입니다. 같은 차트의 기관·개인 선과 비교하면 누가 누구에게 물량을 넘기고 있는지 한눈에 보입니다.",
         "원화 강세 기대, 반도체 등 한국 주력 업종의 이익 개선, 글로벌 위험선호(리스크온) 국면에서 외국인이 한국 주식을 사들일 때 상승합니다.",
         "달러 강세·원화 약세, 글로벌 위험회피(리스크오프), 신흥국 자금 이탈 시 외국인이 차익실현·비중축소에 나설 때 하락합니다. 급락 구간은 대개 지수 조정과 함께 나타납니다."),
    _net("KR_INST_NET", "기관", "Institutional", 1,
         "기관(연기금·금융투자·투신·사모·보험·은행·기타금융 합계)이 순매수한 금액의 누적값입니다. 어느 기관이 움직였는지는 아래 기관 세부 수급 차트에서 볼 수 있습니다.",
         "연기금의 국내주식 비중 확대, 펀드 자금 유입, 지수 급락 시 저가매수 등으로 기관이 순매수할 때 상승합니다.",
         "연기금 리밸런싱(주가 상승 후 비중 축소), 펀드 환매 등으로 기관이 순매도할 때 하락합니다."),
    _net("KR_INDIV_NET", "개인", "Retail", 1,
         "개인투자자가 순매수한 금액의 누적값입니다. 외국인이 팔 때 개인이 받아내는 경우가 많아 외국인 선과 거울처럼 움직이는 경향이 있습니다.",
         "지수 하락 시 저가매수, 2020~2021년 '동학개미'처럼 신규 개인자금이 증시로 몰릴 때 상승합니다.",
         "개인이 차익실현하거나 해외주식·예금 등으로 자금을 옮길 때 하락합니다."),
    _net("KR_CORP_NET", "기타법인", "Other Corporations", 1,
         "기관이 아닌 일반 법인(상장사·비상장 기업 등)의 누적 순매수입니다. 자사주 매입·처분, 계열사 지분 거래, 기업의 여유자금 운용이 주로 잡힙니다.",
         "기업들이 자사주를 사거나 계열사 지분을 늘릴 때 상승합니다. 주가가 저평가됐다고 보는 기업이 많다는 신호로 읽히기도 합니다.",
         "자사주 처분, 지분 매각, 블록딜 등으로 법인이 주식을 내다 팔 때 하락합니다."),
]


# ══════════════════════════════════════════════════════════════════════
#  기관 세부 수급 (합치면 위 기관 합계)
# ══════════════════════════════════════════════════════════════════════
INST_DETAIL: list[IndicatorMeta] = [
    _net("KR_PENSION_NET", "연기금", "Pension Funds", 1,
         "국민연금 등 연기금의 누적 순매수입니다. 자산배분 목표 비중에 맞춰 기계적으로 리밸런싱하기 때문에, 지수가 급락하면 사고 급등하면 파는 '역방향' 수급의 대표 주체입니다.",
         "주가 급락으로 국내주식 비중이 목표보다 낮아졌을 때 비중을 채우려 매수하면서 상승합니다. 하락장의 하방을 받쳐주는 역할로 자주 언급됩니다.",
         "주가 상승으로 비중이 목표를 넘었거나, 해외투자 확대로 국내주식 목표 비중 자체를 줄일 때 하락합니다."),
    _net("KR_SECURITIES_NET", "금융투자", "Financial Investment", 1,
         "증권사 자기매매(금융투자)의 누적 순매수입니다. 가장 큰 몫은 ETF입니다: 개인·연기금 등이 KODEX 200 같은 ETF를 사면, 증권사(지정참가회사·유동성공급자)가 실제 주식 바스켓을 사서 ETF를 새로 만들어 주는데, 이 주식 매수가 통계상 '금융투자 순매수'로 잡힙니다. 즉 주식의 실질 주인은 ETF를 산 투자자이고, 2020년 이후 국내 ETF 시장이 크게 커지면서 이 선이 꾸준히 올라온 것입니다. 나머지는 선물·현물 차익거래, 레버리지·인버스 ETF와 ELS 헤지 매매 등으로, 선물 매도로 위험이 상쇄된 경우가 많습니다. 따라서 증권사의 방향성 베팅이라기보다 ETF를 통한 간접 매수 + 기계적 수급으로 읽는 것이 정확합니다.",
         "ETF로 자금이 들어와 증권사가 바스켓 주식을 사들이거나, 선물이 비싸 현물 매수 차익거래가 늘 때 상승합니다.",
         "ETF 환매, 선물 대비 현물이 비쌀 때의 매도 차익거래, 증권사 자기자본 축소 등으로 하락합니다."),
    _net("KR_TRUST_NET", "투신", "Investment Trusts", 1,
         "자산운용사 공모펀드(투자신탁)의 누적 순매수입니다. 개인이 펀드에 넣고 빼는 자금 흐름이 그대로 반영됩니다.",
         "주식형 공모펀드로 자금이 유입될 때 상승합니다.",
         "펀드 환매가 이어질 때 하락합니다. 최근 몇 년간 공모펀드에서 ETF·직접투자로 자금이 옮겨가며 장기적으로 하락 추세를 보여 왔습니다."),
    _net("KR_PE_NET", "사모펀드", "Private Funds", 1,
         "사모펀드(헤지펀드 포함)의 누적 순매수입니다. 기관 중 상대적으로 단기·이벤트 성격의 매매가 많습니다.",
         "사모펀드로 자금이 몰리거나 이벤트 드리븐 전략으로 주식을 사들일 때 상승합니다.",
         "환매 요청이나 차익실현으로 주식을 팔 때 하락합니다."),
    _net("KR_INSURANCE_NET", "보험", "Insurance", 1,
         "보험사의 누적 순매수입니다. 보험사는 자본규제(K-ICS)상 주식 위험부담이 커서 대체로 주식 비중을 조금씩 줄여 왔습니다.",
         "주가가 크게 싸졌다고 판단하거나 운용수익을 높이려 주식 비중을 늘릴 때 상승합니다.",
         "자본규제 강화나 금리 상승으로 채권 쪽 매력이 커져 주식 비중을 줄일 때 하락합니다."),
    _net("KR_BANK_NET", "은행", "Banks", 1,
         "은행 고유계정의 누적 순매수입니다. 금액이 작고 변동도 적어 보조적으로만 봅니다.",
         "은행이 여유자금 일부를 주식으로 운용할 때 상승합니다.",
         "은행이 보유 주식을 정리할 때 하락합니다."),
    _net("KR_OTHER_FIN_NET", "기타금융", "Other Financials", 1,
         "종금사·저축은행·여신금융사 등 기타 금융기관의 누적 순매수입니다. 금액이 작아 보조적으로만 봅니다.",
         "기타 금융기관이 주식 운용을 늘릴 때 상승합니다.",
         "기타 금융기관이 주식을 처분할 때 하락합니다."),
]


# ══════════════════════════════════════════════════════════════════════
#  선물 차익거래 (프로그램매매 중 차익 부분)
# ══════════════════════════════════════════════════════════════════════
PROGRAM_ARB: list[IndicatorMeta] = [
    IndicatorMeta(
        key="KR_ARB_NET", name_ko="선물 차익거래 누적 순매수", name_en="Arbitrage Program Cumulative Net Buying",
        description="KOSPI200 선물과 현물 주식 바스켓의 가격 차이(베이시스)를 노리는 차익거래 프로그램매매의 누적 순매수입니다. 선물이 현물보다 비싸면 '현물 매수 + 선물 매도', 반대면 '현물 매도 + 선물 매수'로 위험 없이 가격 차이만 챙기므로 방향성 베팅이 아닙니다. 위로 쌓인 만큼은 선물 매도와 짝지어진 현물 매수 잔고로, 선물 만기일(3·6·9·12월 둘째 목요일) 전후에 한꺼번에 풀리며 지수를 흔들 수 있는 '대기 매물'로 봅니다. 금융투자 순매수 중 이 몫이 작다면, 금융투자 순매수는 대부분 ETF 설정 등 다른 요인이라는 뜻입니다.",
        direction="↑차익 매수잔고 증가", source="한국투자증권 Open API (프로그램매매 종합현황)", category=1,
        unit="조원", chg_as_point=True,
        when_up="선물이 현물보다 비싸게 거래될 때(콘탱고 확대) 현물을 사고 선물을 파는 매수 차익거래가 늘어나며 상승합니다. 지수 상승기대가 강해 선물에 웃돈이 붙을 때 자주 나타납니다.",
        when_down="선물이 현물보다 싸지거나(백워데이션) 선물 만기가 다가와 기존 차익 포지션을 청산할 때 현물이 쏟아지며 하락합니다. 만기일 급락은 대개 이 매물 출회 때문입니다.",
    ),
]


# 지표 키 → KIS 응답 필드 (순매수 거래대금, 백만원)
KIS_FIELDS: dict[str, str] = {
    "KR_FOREIGN_NET":    "frgn_ntby_tr_pbmn",
    "KR_INST_NET":       "orgn_ntby_tr_pbmn",
    "KR_INDIV_NET":      "prsn_ntby_tr_pbmn",
    "KR_CORP_NET":       "etc_corp_ntby_tr_pbmn",
    "KR_PENSION_NET":    "fund_ntby_tr_pbmn",
    "KR_SECURITIES_NET": "scrt_ntby_tr_pbmn",
    "KR_TRUST_NET":      "ivtr_ntby_tr_pbmn",
    "KR_PE_NET":         "pe_fund_ntby_tr_pbmn",
    "KR_INSURANCE_NET":  "insu_ntby_tr_pbmn",
    "KR_BANK_NET":       "bank_ntby_tr_pbmn",
    "KR_OTHER_FIN_NET":  "mrbn_ntby_tr_pbmn",
}
# 프로그램매매 종합현황(일별) 응답 필드 (백만원)
KIS_PROGRAM_FIELDS: dict[str, str] = {
    "KR_ARB_NET": "arbt_smtn_ntby_tr_pbmn",
}


# ══════════════════════════════════════════════════════════════════════
#  ② 부동산 — 국토교통부 아파트 매매 실거래가 → 84㎡ 환산 중위가격 (MOLIT_API_KEY 필요)
#  수집·계산은 data/molit_client.py
# ══════════════════════════════════════════════════════════════════════
MOLIT_START = "2020-01-01"

_MOLIT_SOURCE = "국토교통부 실거래가 공개시스템 (아파트 매매)"


def _apt(key, region, region_en, description) -> IndicatorMeta:
    return IndicatorMeta(
        key=key, name_ko=region, name_en=f"{region_en} Apartment Median Price (84㎡)",
        description=description, direction="↑집값 상승", source=_MOLIT_SOURCE, category=2,
        unit="억원", compare_yoy=True,
        when_up="매수 수요가 공급보다 많을 때 오릅니다. 금리 인하·대출 규제 완화, 공급 부족, 전세가 상승(전세→매매 전환) 등이 주된 원인입니다.",
        when_down="고금리·대출 규제 강화로 매수 여력이 줄거나, 입주 물량이 몰리거나, 경기 둔화로 매수 심리가 위축될 때 내립니다.",
    )


_HOW = ("그 달 신고된 아파트 매매 실거래(해제 건 제외)의 전용 ㎡당 가격 중위값에 84를 곱해, "
        "'국민평형(전용 84㎡) 한 채'로 환산한 가격입니다. 실제 84㎡ 거래만의 평균과는 다를 수 있고, "
        "거래 신고 기한(30일) 때문에 최근 1~2개월 값은 이후 신고가 들어오며 조금씩 바뀝니다. "
        "거래가 적은 달은 어떤 단지가 거래됐느냐에 따라 값이 크게 출렁일 수 있습니다.")

# 지표 키 → 법정동 시군구 코드(LAWD_CD)
MOLIT_REGIONS: dict[str, str] = {
    "KR_APT_GANGNAM":  "11680",
    "KR_APT_SONGPA":   "11710",
    "KR_APT_YONGSAN":  "11170",
    "KR_APT_MAPO":     "11440",
    "KR_APT_SEONGDONG": "11200",
    "KR_APT_GANGDONG": "11740",
    "KR_APT_NOWON":    "11350",
    "KR_APT_DOBONG":   "11320",
    "KR_APT_BUNDANG":  "41135",
    "KR_APT_GWACHEON": "41290",
    "KR_APT_HANAM":    "41450",
    "KR_APT_GWANGMYEONG": "41210",
    "KR_APT_YEONGTONG": "41117",
    "KR_APT_SUJI":     "41465",
    "KR_APT_DONGTAN":  "41597",   # 화성시 동탄구 (2026년 구 신설 — 과거 거래도 이 코드로 조회됨)
    "KR_APT_ILSANDONG": "41285",
}

SEOUL_APT: list[IndicatorMeta] = [
    _apt("KR_APT_GANGNAM", "강남", "Gangnam", "서울 강남구. " + _HOW),
    _apt("KR_APT_SONGPA", "송파", "Songpa", "서울 송파구."),
    _apt("KR_APT_YONGSAN", "용산", "Yongsan", "서울 용산구. 거래량이 적어 월별 변동이 큰 편입니다."),
    _apt("KR_APT_MAPO", "마포", "Mapo", "서울 마포구."),
    _apt("KR_APT_SEONGDONG", "성동", "Seongdong", "서울 성동구."),
    _apt("KR_APT_GANGDONG", "강동", "Gangdong", "서울 강동구."),
    _apt("KR_APT_NOWON", "노원", "Nowon", "서울 노원구. 중저가 아파트가 밀집한 지역으로, 금리·대출 규제에 민감합니다."),
    _apt("KR_APT_DOBONG", "도봉", "Dobong", "서울 도봉구."),
]

GYEONGGI_APT: list[IndicatorMeta] = [
    _apt("KR_APT_BUNDANG", "분당", "Bundang", "성남시 분당구. " + _HOW),
    _apt("KR_APT_GWACHEON", "과천", "Gwacheon", "과천시. 거래량이 매우 적어 월별 변동이 큰 편입니다."),
    _apt("KR_APT_HANAM", "하남", "Hanam", "하남시."),
    _apt("KR_APT_GWANGMYEONG", "광명", "Gwangmyeong", "광명시."),
    _apt("KR_APT_YEONGTONG", "수원 영통", "Yeongtong", "수원시 영통구."),
    _apt("KR_APT_SUJI", "용인 수지", "Suji", "용인시 수지구."),
    _apt("KR_APT_DONGTAN", "동탄", "Dongtan", "화성시 동탄구."),
    _apt("KR_APT_ILSANDONG", "일산동구", "Ilsandong", "고양시 일산동구."),
]


CATEGORY_META: dict[int, tuple[str, str, list[IndicatorMeta]]] = {
    1: (
        "① 주식 수급",
        "외국인 달러 기준 자금 유출입, 투자 주체별·기관 세부 누적 순매수와 선물 차익거래",
        FOREIGN_USD + INVESTOR_FLOW + INST_DETAIL + PROGRAM_ARB,
    ),
    2: (
        "② 부동산 가격",
        "서울 주요 구·경기 주요 지역 아파트 실거래 중위가격 (84㎡ 환산, 월간)",
        SEOUL_APT + GYEONGGI_APT,
    ),
}

ALL_INDICATORS: list[IndicatorMeta] = (
    FOREIGN_USD + INVESTOR_FLOW + INST_DETAIL + PROGRAM_ARB + SEOUL_APT + GYEONGGI_APT
)
