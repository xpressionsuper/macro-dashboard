#!/usr/bin/env python3
"""
매크로 모니터 v3 ─ 전역 설정 & 지표 메타데이터
=====================================================================
모든 FRED 시리즈 ID, 표시명, 의미, 방향성을 한 곳에서 관리합니다.
새 지표 추가 시 이 파일만 수정하면 됩니다.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ── 환경변수 (.env → os.environ, 이미 설정된 값이 우선) ────────────────
_ENV_FILE = Path(__file__).with_name(".env")
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text(encoding="utf-8-sig").splitlines():
        _k, _sep, _v = _line.partition("=")
        if _sep and _k.strip() and not _k.lstrip().startswith("#"):
            os.environ.setdefault(_k.strip(), _v.strip().strip("'\""))

FRED_API_KEY     = os.environ.get("FRED_API_KEY", "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ── 수집·출력 설정 ────────────────────────────────────────────────────
LOOKBACK_DAYS = 365 * 10     # 10년치 데이터 수집
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
    description: str                # 의미 및 해석 (짧은 요약)
    direction: str                  # 방향성 설명 ("↑유동성 증가" 등)
    source: str                     # 데이터 출처
    category: int                   # 카테고리 번호 1~5
    fred_id: Optional[str] = None   # FRED 시리즈 ID (없으면 파생/외부)
    unit: str = ""                  # 표시 단위 ("$B", "%", "bps" 등)
    divisor: float = 1.0            # 단위 환산 제수 (million → billion: 1000)
    available: bool = True          # 데이터 수집 가능 여부
    when_up: str = ""               # 이 지표가 "증가"할 때 실제로 벌어지는 일과 의미
    when_down: str = ""             # 이 지표가 "감소"할 때 실제로 벌어지는 일과 의미
    compare_yoy: bool = False       # True면 전월대비 대신 전년대비를 주 비교로 씀
    chg_as_point: Optional[bool] = None   # True=포인트(±X) 변화, False=% 변화, None=unit으로 자동판단


# ══════════════════════════════════════════════════════════════════════
#  ① 연준(Fed) 직접 공급 지표
#  Fed가 직접 조절하는 유동성 채널
# ══════════════════════════════════════════════════════════════════════
FED_SUPPLY: list[IndicatorMeta] = [
    IndicatorMeta(
        key="BS", name_ko="Fed 대차대조표", name_en="Balance Sheet",
        description="연준이 보유한 국채·MBS 등 총자산 규모입니다. 연준이 자산을 사들이면(양적완화·QE) 그 대금이 은행 시스템에 새 돈으로 꽂히고, 반대로 만기 채권을 재투자하지 않고 흘려보내면(양적긴축·QT) 그만큼 돈이 시중에서 빠져나갑니다. 유동성 대시보드에서 가장 근본이 되는 '수도꼭지' 지표로, 이후 나오는 대부분의 파생 지표(넷 유동성 등)의 출발점입니다.",
        direction="↑유동성 증가", source="FRED: WALCL", category=1,
        fred_id="WALCL", unit="$B", divisor=1_000,
        when_up="연준이 국채·MBS를 사들이는 QE(양적완화)를 하거나, 위기 시 은행에 긴급대출을 내줄 때 늘어납니다. 시중에 새 돈이 풀렸다는 뜻이라 주식·채권 등 위험자산에 우호적입니다.",
        when_down="QT(양적긴축)로 만기 도래한 채권을 재투자하지 않고 시중 유동성을 회수할 때 줄어듭니다. 유동성이 마르는 신호라 시장 변동성이 커지는 경향이 있습니다.",
    ),
    IndicatorMeta(
        key="UST", name_ko="국채 보유량", name_en="UST Holdings",
        description="Fed 대차대조표 자산 중 미국 국채가 차지하는 부분만 따로 뗀 값입니다. 연준이 QT를 진행할 때 국채와 MBS 중 어느 쪽을 더 빨리 줄이는지, QT의 실제 속도와 구성을 가장 직접적으로 보여줍니다. 국채 축소가 빠르면 그만큼 민간(투자자)이 신규 발행 국채를 더 많이 소화해야 해서 장기금리에 상승 압력을 줍니다.",
        direction="↑유동성 증가", source="FRED: TREAST", category=1,
        fred_id="TREAST", unit="$B", divisor=1_000,
        when_up="연준이 만기 도래 국채를 재투자하거나 신규 매입할 때 늘어납니다. 국채 금리 상승을 누르는 효과가 있습니다.",
        when_down="만기 국채를 재투자하지 않고 그대로 소멸시키는(롤오프) QT 국면에서 줄어듭니다. 국채 공급 부담이 민간으로 넘어가 금리 상승 압력이 됩니다.",
    ),
    IndicatorMeta(
        key="MBS", name_ko="MBS 보유량", name_en="MBS Holdings",
        description="Fed 대차대조표 안에서 주택저당증권(MBS)이 차지하는 부분입니다. 2008년 금융위기 이후 연준이 주택시장을 떠받치기 위해 대규모로 사들이기 시작했습니다. 이 보유량이 줄어드는 속도는 곧 모기지(주택담보대출) 금리와 주택시장 유동성에 영향을 주므로, 국채와는 별도로 부동산 경기 판단에 참고할 수 있습니다.",
        direction="↑유동성 증가", source="FRED: WSHOMCB", category=1,
        fred_id="WSHOMCB", unit="$B", divisor=1_000,
        when_up="연준이 모기지 채권을 사들여 주택담보대출 금리를 낮추려 할 때 늘어납니다. 부동산 시장 부양 신호입니다.",
        when_down="MBS도 QT 대상에 포함되어 상환분을 재투자하지 않을 때 줄어듭니다. 모기지 금리 상승 압력으로 이어질 수 있습니다.",
    ),
    IndicatorMeta(
        key="FED_COUPONS", name_ko="연준 중장기 국채 보유 (Notes·Bonds)", name_en="Fed Notes & Bonds Holdings",
        description="연준이 보유한 만기 2년 이상 명목 국채입니다. '연준의 항복' 판정의 핵심 지표로, 단기채(Bills) 매입은 준비금 관리용 기술적 조치지만 중장기채를 사들이는 것은 장기금리를 직접 누르는 행위(QE·수익률 곡선 통제)이기 때문입니다. MBS 상환분을 단기채로 재투자하는 지금 구조에서는 이 잔고가 늘어나는 것 자체가 정책 전환의 신호입니다.",
        direction="↑금리 억제 개입", source="FRED: WSHONBNL", category=1,
        fred_id="WSHONBNL", unit="$B", divisor=1_000,
        when_up="연준이 장기금리 급등·국채시장 기능 장애에 대응해 중장기채를 직접 사들일 때 늘어납니다. 13주 +$50B 이상이면 항복 초기, +$150B 이상이면 QE급 본격 개입으로 봅니다.",
        when_down="QT로 만기 도래분을 재투자하지 않거나 단기채로만 재투자할 때 줄어들거나 횡보합니다.",
    ),
    IndicatorMeta(
        key="FED_BILLS", name_ko="연준 단기채 보유 (Bills)", name_en="Fed T-Bills Holdings",
        description="연준이 보유한 만기 1년 이하 단기 국채입니다. 연준은 QT 종료 후 지급준비금이 부족해지지 않도록 단기채를 꾸준히 사들이는데(준비금 관리 매입, RMP), 이는 장기금리를 누르려는 목적이 아니라서 '항복'으로 보지 않습니다. 다만 대차대조표가 다시 커지기 시작했다는 점에서 항복 전 단계의 유동성 공급으로 읽습니다.",
        direction="↑유동성 증가", source="FRED: WSHOBL", category=1,
        fred_id="WSHOBL", unit="$B", divisor=1_000,
        when_up="준비금 관리 매입이나 MBS 상환분 재투자로 단기채를 사들일 때 늘어납니다. 시중 지급준비금을 보충하는 효과가 있습니다.",
        when_down="QT로 단기채 만기분을 재투자하지 않을 때 줄어듭니다.",
    ),
    IndicatorMeta(
        key="SRF", name_ko="상설레포(SRF) 이용액", name_en="Standing Repo Facility",
        description="은행·딜러가 국채를 담보로 연준에서 하룻밤 자금을 빌리는 상설 창구의 이용액입니다. 평소엔 0에 가깝고 분기말에만 잠깐 튀는데, 평시에도 이용이 이어지면 국채를 들고 있는 딜러들이 자금 조달에 어려움을 겪는다는 뜻이라 국채시장 기능 장애의 직접 신호가 됩니다.",
        direction="↑시장 기능 경고", source="FRED: RPONTSYD", category=1,
        fred_id="RPONTSYD", unit="$B",
        when_up="딜러가 쌓인 국채 재고를 민간 레포시장에서 소화하지 못해 연준 창구로 몰릴 때 늘어납니다. 5일 평균 $10B 이상이면 경고로 봅니다.",
        when_down="민간 단기자금 시장이 정상적으로 작동할 때 0 근처를 유지합니다.",
    ),
    IndicatorMeta(
        key="RRP", name_ko="역레포 잔고", name_en="RRP / ON RRP",
        description="머니마켓펀드(MMF) 등 금융기관이 하루짜리로 연준에 맡기는 초단기 예치금입니다. 마땅한 투자처가 없을 때 연준의 역레포 창구에 현금을 세워두는 일종의 '대기 자금 주차장'으로, 이 잔고가 크다는 건 그만큼 많은 돈이 실물경제로 돌지 않고 잠겨 있다는 뜻입니다. 2021~2022년 한때 2조 달러를 넘었다가 이후 꾸준히 줄어든 것이 최근 유동성 확대의 숨은 원천이었습니다.",
        direction="↓유동성 증가", source="FRED: RRPONTSYD", category=1,
        fred_id="RRPONTSYD", unit="$B",
        when_up="MMF(머니마켓펀드)들이 마땅한 투자처를 못 찾아 남는 현금을 연준에 예치할 때 늘어납니다. 즉 돈이 시중에 안 돌고 연준 창고에 잠자고 있다는 뜻입니다.",
        when_down="MMF가 국채 등 다른 자산에 투자하려고 예치금을 인출할 때 줄어듭니다. 잠겨있던 자금이 시중으로 풀려나온다는 뜻이라, 다른 조건이 같다면 유동성엔 긍정적입니다.",
    ),
    IndicatorMeta(
        key="RESERVES", name_ko="지급준비금 잔고", name_en="Reserve Balances",
        description="시중은행들이 결제·규제 목적으로 연준 계좌에 의무·자율적으로 예치해 두는 현금입니다. 은행 시스템 전체의 '실탄' 재고라고 볼 수 있으며, 이 잔고가 특정 임계치 아래로 떨어지면 은행 간 하루짜리 자금 조달(레포) 시장에서 갑자기 금리가 튀는 등 배관 이상이 나타납니다. 2019년 9월 레포 발작이 대표 사례입니다.",
        direction="↑유동성 증가", source="FRED: WRBWFRBL", category=1,
        fred_id="WRBWFRBL", unit="$B", divisor=1_000,
        when_up="연준 자산 매입(QE)이나 TGA·RRP에서 자금이 빠져나와 은행 시스템으로 흘러들 때 늘어납니다. 은행이 여유자금을 많이 쥐고 있다는 뜻입니다.",
        when_down="QT나 TGA 재충전(국채 발행 대금 흡수)으로 은행 시스템에서 자금이 빠질 때 줄어듭니다. 특정 수준(약 3조 달러 안팎으로 추정) 아래로 급감하면 2019년 9월처럼 레포시장 발작 위험이 커집니다.",
    ),
    IndicatorMeta(
        key="DISC_WIN", name_ko="할인창구 대출", name_en="Discount Window",
        description="은행이 민간시장에서 자금을 못 구할 때 최후 수단으로 연준에서 직접 빌리는 대출 총잔고입니다('최종 대부자' 창구). 평소엔 낙인 효과(이용 사실이 알려지면 부실로 낙인찍힘) 때문에 거의 쓰이지 않다가, 개별 은행 또는 시스템 전반에 위기가 닥치면 급격히 치솟습니다. 2023년 3월 SVB·시그니처은행 파산 당시 역대 최고치를 기록했습니다.",
        direction="↑스트레스 증가", source="FRED: BORROW", category=1,
        fred_id="BORROW", unit="$B", divisor=1_000,
        when_up="은행이 민간시장에서 자금을 조달하지 못해 최후의 보루인 연준 창구로 몰릴 때 급증합니다. 2023년 SVB 사태처럼 개별 은행 또는 시스템 전반의 유동성 위기 신호입니다.",
        when_down="은행들이 민간 자금조달(예금, 레포시장 등)에 문제가 없어 연준 대출에 의존할 필요가 없을 때 낮은 수준을 유지합니다. 평상시엔 거의 0에 가깝습니다.",
    ),
]


# ══════════════════════════════════════════════════════════════════════
#  ② 재무부(Treasury) 채널 지표
#  TGA와 발행 전략으로 시중 유동성을 흡수/방출
# ══════════════════════════════════════════════════════════════════════
TREASURY_CHANNEL: list[IndicatorMeta] = [
    IndicatorMeta(
        key="TGA", name_ko="TGA 잔고", name_en="Treasury General Account",
        description="미국 정부가 연준 안에 갖고 있는 당좌예금 계좌입니다. 정부가 세금·국채발행으로 돈을 걷으면 이 계좌에 쌓이고(시중 자금 흡수), 국방비·복지 등으로 지출하면 이 계좌에서 빠져나가 시중에 뿌려집니다(시중 자금 방출). 특히 부채한도 협상이 타결된 직후에는 그동안 못 갚았던 청구서를 갚기 위해 국채를 대량 발행해 TGA를 급격히 채우는데, 이때 시중 유동성이 단기간에 크게 빨려나갑니다.",
        direction="↓유동성 증가", source="TreasuryDirect (매일)", category=2,
        fred_id=None, unit="$B",   # Treasury Fiscal Data API로 별도 수집
        when_up="정부가 국채를 발행해 시중 자금을 걷어 재무부 계좌에 쌓을 때(부채한도 협상 타결 직후 자주 발생) 늘어납니다. 시중에서 돈을 빨아들이는 효과라 유동성엔 부정적입니다.",
        when_down="정부가 재정지출(국방비, 사회보장 등)을 집행해 계좌에서 돈을 풀 때 줄어듭니다. 시중에 현금이 유입되는 효과라 유동성엔 긍정적입니다.",
    ),
]


# ══════════════════════════════════════════════════════════════════════
#  ③ 합성(Net) 유동성 지표
#  연준·재무부 데이터를 결합한 실질 유동성 계량
# ══════════════════════════════════════════════════════════════════════
NET_LIQUIDITY: list[IndicatorMeta] = [
    IndicatorMeta(
        key="NET_LIQ", name_ko="넷 유동성", name_en="Net Liquidity (BS−RRP−TGA)",
        description="Fed 대차대조표에서 역레포(RRP)와 재무부 계좌(TGA)를 뺀 값으로, '실제로 시중 은행 시스템에 풀려 굴러다니는 순수 유동성'을 근사합니다. 개별 지표(대차대조표, RRP, TGA)를 따로 보면 방향이 엇갈릴 때가 많은데, 이 셋을 합쳐야 진짜 유동성 사이클이 보인다는 게 월가 트레이더들이 이 지표를 즐겨 쓰는 이유입니다. 역사적으로 S&P500 지수와 상당히 높은 상관관계를 보여왔습니다.",
        direction="↑유동성 증가", source="파생 계산 (BS−RRP−TGA)", category=3,
        fred_id=None, unit="$B",
        when_up="대차대조표가 늘거나(QE) RRP·TGA가 줄어들 때(자금이 시중으로 방출) 늘어납니다. 실질적으로 시중에 풀린 순수 유동성이 커진다는 뜻이며, 통상 위험자산 랠리와 같이 움직입니다.",
        when_down="QT로 대차대조표가 줄거나 TGA 재충전·RRP 증가로 자금이 흡수될 때 줄어듭니다. 위험자산에 역풍으로 작용하는 경우가 많습니다.",
    ),
    IndicatorMeta(
        key="SOFR_SPR", name_ko="SOFR−IORB 스프레드", name_en="SOFR−IORB Spread",
        description="하룻밤 국채 담보대출 금리(SOFR)에서 연준이 은행에 지급하는 지급준비금 이자율(IORB)을 뺀 값입니다. 이론상 은행은 IORB보다 비싸게 돈을 빌릴 이유가 없어 스프레드는 0 근처에 머물러야 정상인데, 지급준비금이 부족해지면 은행들이 웃돈을 주고서라도 하룻밤 자금을 구하려 해 스프레드가 벌어집니다. 배관(자금 조달 시스템) 이상을 가장 먼저 포착하는 '탄광 속 카나리아' 같은 지표입니다.",
        direction="↑스트레스 증가", source="파생 계산 (SOFR−IORB)", category=3,
        fred_id=None, unit="bps",
        when_up="은행 시스템에 담보로 쓸 현금(지급준비금)이 부족해 하룻밤 자금을 빌리려는 수요가 몰릴 때 급등합니다. 지급준비금이 필요 이상으로 줄었다는 배관 이상 신호입니다.",
        when_down="지급준비금이 충분해 단기자금 조달에 여유가 있을 때 0 근처(음수 포함)로 안정됩니다. 평상시엔 이 상태가 정상입니다.",
    ),
    IndicatorMeta(
        key="M2", name_ko="M2 통화량", name_en="M2 Money Stock",
        description="현금, 요구불예금, 저축성예금, MMF 등을 모두 합친 광의통화 총량입니다. 연준의 QE·QT가 은행 대출을 거쳐 실물경제에 파급된 '최종 결과물'이라 할 수 있어, 연준 대차대조표보다 한 박자 늦게, 하지만 더 넓은 범위에서 유동성 상황을 보여줍니다. 코로나19 기간 사상 최대 폭으로 늘었다가 2022년부터 사상 처음으로 감소세를 보였습니다.",
        direction="↑유동성 증가", source="FRED: M2SL", category=3,
        fred_id="M2SL", unit="$B", compare_yoy=True,
        when_up="은행 대출이 늘거나 연준·재정 부양으로 시중에 현금·예금이 늘어날 때 증가합니다. 장기적으로는 인플레이션 압력과도 연결됩니다.",
        when_down="은행 신용이 위축되거나(대출 회수), 연준이 자금을 흡수할 때 감소합니다. 2022년처럼 사상 최초의 M2 감소는 강한 긴축 신호였습니다.",
    ),
    IndicatorMeta(
        key="M2_YOY", name_ko="M2 전년비 증가율", name_en="M2 YoY Growth",
        description="M2 통화량이 1년 전 대비 몇 % 늘었는지를 보여줍니다. 실물경제에 영향을 미치기까지 통상 3~6개월의 시차가 있어 '지금의 M2_YOY는 몇 달 뒤 경기·물가를 예고한다'는 식으로 해석됩니다. 마이너스로 전환하는 것은 극히 이례적인 사건으로, 실제로 2022~2023년 마이너스 전환 이후 여러 지역은행 위기와 대출 위축이 뒤따랐습니다.",
        direction="↑유동성 증가", source="파생 계산 (M2SL YoY)", category=3,
        fred_id=None, unit="%",
        when_up="통화량 증가 속도가 빨라질 때 상승합니다. 팬데믹 시기처럼 급격한 상승은 이후 인플레이션으로 이어질 위험 신호이기도 합니다.",
        when_down="통화 공급 확대 속도가 둔화되거나 실제로 줄어들 때 하락·마이너스 전환합니다. 마이너스는 극히 이례적인 긴축 국면(2022~2023년)을 의미합니다.",
    ),
]


# ══════════════════════════════════════════════════════════════════════
#  ④ 금융시장 유동성 지표
#  시장에서 관측되는 유동성 가격·스트레스 신호
# ══════════════════════════════════════════════════════════════════════
FINANCIAL_MARKET: list[IndicatorMeta] = [
    IndicatorMeta(
        key="FEDFUNDS", name_ko="연준 기준금리", name_en="Federal Funds Rate",
        description="연준이 FOMC 회의에서 직접 결정하는 정책금리(실효 기준)입니다. SOFR·2년물 금리 등 시중 모든 단기금리의 출발점이자, '지금 통화정책이 얼마나 긴축적인지'를 보여주는 가장 기본적인 숫자입니다.",
        direction="↑긴축", source="FRED: FEDFUNDS", category=4,
        fred_id="FEDFUNDS", unit="bps", divisor=0.01,
        when_up="연준이 물가를 잡거나 경기 과열을 억제하기 위해 금리를 인상할 때 오릅니다. 오르는 속도와 최종 수준(터미널 레이트)이 시장 전체의 할인율·자산가격에 영향을 줍니다.",
        when_down="연준이 경기 둔화·침체 우려에 대응해 금리를 인하할 때 내려갑니다. 인하 시작 시점과 속도가 통상 주식시장의 가장 큰 변곡점 중 하나입니다.",
    ),
    IndicatorMeta(
        key="DGS3MO", name_ko="3개월물 국채 수익률", name_en="3-Month Treasury Yield",
        description="만기 3개월 미국 국채의 시장 유통수익률입니다. 연준 기준금리에 대한 시장의 실시간 기대를 가장 짧은 만기에서 반영하며, 통상 기준금리와 거의 동일하게 움직이지만 금리 인하/인상 전환 국면에서는 기준금리보다 먼저 반응하는 경우가 있습니다.",
        direction="", source="FRED: DGS3MO", category=4,
        fred_id="DGS3MO", unit="bps", divisor=0.01,
        when_up="연준의 추가 인상 기대가 강해지거나 기준금리 인상이 실제로 반영될 때 상승합니다.",
        when_down="연준의 인하 기대가 강해지거나 기준금리 인하가 반영될 때 하락합니다. 침체 국면 진입 시 기준금리보다 먼저 꺾이는 경향이 있습니다.",
    ),
    IndicatorMeta(
        key="SOFR", name_ko="SOFR", name_en="Secured Overnight Financing Rate",
        description="국채를 담보로 하룻밤 동안 자금을 빌리고 빌려줄 때 실제로 거래되는 금리로, 2022년부터 LIBOR를 대체한 미국의 기준 단기금리입니다. 은행·MMF·헤지펀드 등이 실시간으로 자금을 조달하는 가격이라, 이 금리가 튀는 것 자체가 단기자금시장 스트레스의 1차 신호입니다.",
        direction="", source="FRED: SOFR", category=4,
        fred_id="SOFR", unit="bps", divisor=0.01,
        when_up="연준 기준금리 인상이나 담보(국채) 확보 경쟁이 치열해질 때 상승합니다. 분기말·월말 등 결제 수요가 몰리는 시점에 일시적으로 튀기도 합니다.",
        when_down="연준 금리 인하 또는 단기자금 시장에 자금이 넉넉할 때 하락합니다.",
    ),
    IndicatorMeta(
        key="IORB", name_ko="IORB", name_en="Interest on Reserve Balances",
        description="연준이 은행들의 지급준비금 예치에 대해 지급하는 이자율로, 연준이 회의에서 직접 결정하는 정책금리입니다. SOFR이 이 금리보다 얼마나 위로 튀는지가 지급준비금 희소도(SOFR−IORB 스프레드)를 재는 기준선이 됩니다.",
        direction="", source="FRED: IORB", category=4,
        fred_id="IORB", unit="bps", divisor=0.01,
        when_up="연준이 기준금리를 인상할 때 함께 오릅니다 (연준이 직접 결정하는 정책 변수).",
        when_down="연준이 기준금리를 인하할 때 함께 내려갑니다.",
    ),
    IndicatorMeta(
        key="HY_OAS", name_ko="HY 크레딧 스프레드", name_en="HY OAS",
        description="신용등급이 낮은 기업(하이일드/정크본드)이 국채 대비 얼마나 높은 금리를 얹어 돈을 빌리는지를 나타냅니다. 이 스프레드는 '시장이 매기는 부실 위험의 가격표'로, 실물경제 지표보다 앞서 신용시장의 불안을 감지하는 경우가 많아 위기 국면의 조기경보로 널리 쓰입니다.",
        direction="↑유동성 악화", source="FRED: BAMLH0A0HYM2", category=4,
        fred_id="BAMLH0A0HYM2", unit="bps", divisor=0.01,
        when_up="경기 둔화 우려로 투자자들이 부실 위험이 큰 하이일드 채권을 팔고 국채로 도망갈 때(flight to quality) 확대됩니다. 600bps 이상은 통상 위기 국면으로 해석됩니다.",
        when_down="경기·기업실적에 대한 낙관, 위험자산 선호 심리가 강할 때 축소됩니다. 지나치게 낮으면(과열) 오히려 리스크가 과소평가됐다는 신호로 보기도 합니다.",
    ),
    IndicatorMeta(
        key="IG_OAS", name_ko="IG 크레딧 스프레드", name_en="IG OAS",
        description="신용등급이 우량한(투자등급) 기업이 국채 대비 얼마나 높은 금리를 부담하는지를 보여줍니다. HY 스프레드보다 움직임은 둔하지만, 우량기업 발행시장을 포함하기 때문에 더 넓은 신용시장 전체의 체온계 역할을 합니다. IG 스프레드까지 확대되면 신용경색이 부실기업을 넘어 우량기업까지 번졌다는 뜻이라 더 심각한 신호로 봅니다.",
        direction="↑유동성 악화", source="FRED: BAMLC0A0CM", category=4,
        fred_id="BAMLC0A0CM", unit="bps", divisor=0.01,
        when_up="우량기업조차 신용 리스크 프리미엄을 더 요구받을 때 확대됩니다. HY보다 뒤늦게 반응하지만 확대되면 신용경색이 우량기업까지 번졌다는 뜻이라 더 심각합니다.",
        when_down="전반적인 신용시장 여건이 안정적일 때 축소됩니다.",
    ),
    IndicatorMeta(
        key="NFCI", name_ko="금융조건지수 (시카고 Fed)", name_en="NFCI",
        description="시카고 연은이 금리, 신용스프레드, 주가 변동성, 환율 등 100개 이상의 변수를 하나로 압축한 종합 금융여건 지수입니다. 0을 기준으로 평균적인 금융여건을 나타내며, 개별 지표를 하나하나 보지 않고 '지금 돈 빌리기가 평소보다 쉬운지 어려운지'를 한눈에 파악할 때 유용합니다.",
        direction="↓조건 완화", source="FRED: NFCI", category=4,
        fred_id="NFCI", unit="", chg_as_point=True,
        when_up="금리·신용스프레드·변동성이 동시에 악화되며 금융여건이 평균보다 조여질 때 양수로 올라갑니다. 경기 하방 압력 신호입니다.",
        when_down="금융여건이 평균보다 완화적(돈 빌리기 쉬운 상태)일 때 음수로 내려갑니다.",
    ),
    IndicatorMeta(
        key="VIX", name_ko="VIX 변동성 지수", name_en="VIX",
        description="S&P500 옵션 가격에서 역산한, 향후 30일간 시장이 예상하는 변동성입니다. '공포지수'라는 별명대로 투자자들이 앞으로 큰 등락을 걱정할수록 옵션 프리미엄이 비싸지며 값이 올라갑니다. 대체로 20 미만이면 안정, 20~30이면 경계, 30을 넘으면 패닉에 가까운 위기 국면으로 봅니다.",
        direction="↑스트레스 증가", source="FRED: VIXCLS", category=4,
        fred_id="VIXCLS", unit="",
        when_up="예상치 못한 악재(지정학 리스크, 실적 쇼크, 금융시스템 불안 등)로 옵션 시장에서 향후 변동성에 대한 프리미엄이 커질 때 급등합니다. 통상 주가 급락과 동시에 발생합니다.",
        when_down="시장이 안정되고 방향성에 대한 확신이 커질 때 하락합니다. 지나치게 낮은 수준(10 이하)이 장기간 지속되면 시장이 리스크를 과소평가하고 있다는 경고로도 읽힙니다.",
    ),
    IndicatorMeta(
        key="DGS2", name_ko="2년물 국채금리", name_en="2Y Treasury Yield",
        description="만기 2년짜리 미국 국채의 유통 수익률입니다. 향후 1~2년간 연준이 금리를 얼마나 올리거나 내릴지에 대한 시장의 예측을 가장 잘 반영하는 만기로, 수익률 곡선(10Y−2Y)을 계산할 때 '단기 쪽 기준점' 역할을 합니다.",
        direction="", source="FRED: DGS2", category=4,
        fred_id="DGS2", unit="%",
        when_up="시장이 향후 연준의 금리 인상(또는 인하 지연)을 예상할 때 상승합니다.",
        when_down="시장이 향후 연준의 금리 인하를 예상할 때 하락합니다. 경기침체 우려가 커지면 인하 기대가 앞당겨지며 빠르게 떨어지기도 합니다.",
    ),
    IndicatorMeta(
        key="DGS10", name_ko="10년물 국채금리", name_en="10Y Treasury Yield",
        description="만기 10년짜리 미국 국채의 유통 수익률로, 전 세계 자산가격의 '무위험 할인율' 기준으로 쓰입니다. 주식(특히 성장주), 부동산, 모기지 금리 등 거의 모든 자산의 밸류에이션에 직접 영향을 미치는, 금융시장에서 가장 중요한 단일 금리라고 해도 과언이 아닙니다.",
        direction="", source="FRED: DGS10", category=4,
        fred_id="DGS10", unit="%",
        when_up="장기 성장·인플레이션 기대가 높아지거나, 국채 공급 부담(재정적자 확대)이 커질 때 상승합니다. 주식 밸류에이션의 할인율이 높아져 성장주에 부담이 됩니다.",
        when_down="경기침체·디플레이션 우려로 안전자산 수요가 몰리거나, 인플레이션 기대가 낮아질 때 하락합니다.",
    ),
    IndicatorMeta(
        key="DFII10", name_ko="10년물 실질금리 (TIPS)", name_en="10Y Real Yield",
        description="물가연동국채(TIPS) 수익률로, 10년물 국채금리(DGS10)에서 시장이 예상하는 인플레이션분(기대인플레이션)을 이미 제거한 '진짜' 금리입니다. 명목금리가 높아도 기대인플레이션이 그만큼 높으면 실질적인 자금조달 부담은 크지 않을 수 있는데, 이 지표는 그 착시를 걷어내고 실물경제·주식 밸류에이션에 실제로 가해지는 할인율 압박을 보여줍니다.",
        direction="↑할인율 부담 증가", source="FRED: DFII10", category=4,
        fred_id="DFII10", unit="%",
        when_up="연준이 실질적으로 긴축적인 정책을 펴거나(명목금리 상승이 기대인플레이션 하락보다 클 때), 강한 경제성장 기대로 실질 자금수요가 늘 때 상승합니다. 성장주·고밸류에이션 자산에 특히 부담스러운 신호입니다.",
        when_down="경기 둔화 우려로 연준의 완화 기대가 커지거나, 안전자산으로서 TIPS 수요가 몰릴 때 하락합니다. 마이너스로 전환하는 건 극도로 완화적인 국면(2020~2021년)에서만 나타났던 이례적인 상태입니다.",
    ),
    IndicatorMeta(
        key="TERM_PREMIUM", name_ko="10년물 기간 프리미엄", name_en="10Y Term Premium (Kim-Wright)",
        description="10년물 금리 중에서 '앞으로의 단기금리 경로 예상'으로 설명되지 않는 부분, 즉 장기채를 오래 들고 있는 위험에 대해 투자자가 요구하는 웃돈입니다. 재정적자·국채 공급 부담·물가 불확실성이 커지면 이 값이 오르는데, 연준이 장기채 매입으로 직접 눌러야 하는 대상이 바로 이 부분이라 '연준 항복' 압력의 핵심 계기판입니다.",
        direction="↑장기채 기피", source="FRED: THREEFYTP10", category=4,
        fred_id="THREEFYTP10", unit="%",
        when_up="국채 공급 과잉, 재정 신뢰 하락, 해외 수요 이탈로 투자자들이 장기채 보유에 더 큰 보상을 요구할 때 상승합니다. 1%p를 넘으면 압박 구간으로 봅니다.",
        when_down="안전자산 수요가 몰리거나 연준이 장기채를 매입(QE)해 공급 부담을 흡수할 때 하락하며, 2010년대에는 마이너스까지 내려갔습니다.",
    ),
]

# 요약 카드에는 표시하지 않지만(다른 지표와 겹치거나 차트에 종속) 원시 데이터는
# 파생 계산·오버레이 차트를 위해 계속 수집해야 하는 지표 키
HIDDEN_FROM_TABLE: set[str] = {"IORB", "DGS2", "MBS"}


# ── 카테고리 통합 딕셔너리 ────────────────────────────────────────────
CATEGORY_META: dict[int, tuple[str, str, list[IndicatorMeta]]] = {
    1: ("① 연준(Fed) 직접 공급 지표",    "Fed가 직접 조절하는 유동성 채널",               FED_SUPPLY),
    2: ("② 재무부(Treasury) 채널 지표",  "TGA와 발행 전략으로 시중 유동성을 흡수/방출",   TREASURY_CHANNEL),
    3: ("③ 합성(Net) 유동성 지표",       "연준·재무부 데이터를 결합한 실질 유동성 계량",  NET_LIQUIDITY),
    4: ("④ 금융시장 유동성 지표",        "시장에서 관측되는 유동성 가격·스트레스 신호",    FINANCIAL_MARKET),
}

ALL_INDICATORS: list[IndicatorMeta] = (
    FED_SUPPLY + TREASURY_CHANNEL + NET_LIQUIDITY + FINANCIAL_MARKET
)
