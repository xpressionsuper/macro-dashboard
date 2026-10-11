"""
indicators/capitulation.py ─ 연준 항복(Fed Capitulation) 판정 v2
=====================================================================
항복은 세 트랙으로 판정합니다 (reports/2026-10-11_fed_capitulation_checklist_v2.md).

  A. 대차대조표 항복  새 돈으로 중장기 국채 매입 → 장기금리 방어
       0 평시 · 1 압박 누적 · 2 시장 기능 경고 · 3 대차대조표 재확장(단기채)
       4 항복 초기 · 5 본격 항복(QE급)        현재 단계 = 충족된 가장 높은 단계
       4단계 이상이면 QE형 / 트위스트형 / 재투자형 / 항복 실패형으로 유형 판별
  B. 금리정책 항복 (재정지배)  물가 경보인데 동결·인하, Bad Cut, 인상+중장기채 매입
  C. 제도적 항복  직접 인수·YCC 등 → 데이터 없음, 정책 발표 수동 확인

보조: 위기형(긴급 빅컷) 임박 신호, 달러 신뢰 균열, 유동성 시차 시계.
ponytail: 임계값은 경험칙(강의·[보완])이라 아래 상수에서 조정. 과거 국면 검증 안 됨.
"""

from __future__ import annotations

import html
from dataclasses import dataclass

import pandas as pd

# ── 트랙 A ──
TEN_Y_MIN        = 5.0    # %
TERM_PREMIUM_MIN = 1.0    # %p
INTEREST_GDP_MIN = 3.5    # %
BILL_SHARE_MIN   = 22.0   # %
SOFR_SPR_MIN     = 5.0    # bps
SRF_5D_MIN       = 10.0   # $B
TEN_Y_JUMP_MIN   = 0.50   # %p, 1개월
HY_OAS_MIN       = 400.0  # bps (강의 4.0%p)
VIX_MIN          = 30.0
BILLS_13W_MIN    = 50.0   # $B
COUPONS_13W_MIN  = 50.0   # $B
COUPONS_QE_MIN   = 150.0  # $B
# ── 트랙 B ──
CPI_ALERT        = 3.5    # % (강의 2026 경계수위)
UNRATE_STAG      = 4.5    # % (스태그플레이션 기준선)
# ── 위기 신호 ──
DOLLAR_SPIKE_PCT = 3.0    # % 1개월 [보완]
SPIRAL_10Y_BP    = 0.25   # %p 1개월 [보완]
SPIRAL_USD_PCT   = -2.0   # % 1개월 [보완]
# ── 유동성 시차 시계 ──
CLOCK_BASE       = pd.Timestamp("2025-12-01")
CLOCK_FROM       = pd.Timestamp("2026-12-01")
CLOCK_TO         = pd.Timestamp("2027-06-30")

STAGE_NAMES = ["평시", "압박 누적", "시장 기능 경고", "대차대조표 재확장", "항복 초기", "본격 항복"]
STAGE_HINTS = [
    "금리·재정·시장 모두 정상 범위",
    "장기금리·기간 프리미엄·재정 부담이 쌓이는 중 (2개 이상)",
    "국채·자금·신용시장에서 기능 이상 신호 (1개 이상)",
    "단기채 매입으로 대차대조표 재확장 — 아직 항복 아님",
    "중장기채 매입 시작 — 장기금리 직접 개입",
    "QE급 중장기채 매입 (13주 +$150B 이상)",
]
TRACK_C_ITEMS = [
    "FOMC 성명에 '시장 기능 회복을 위한 국채 매입' 표현",
    "뉴욕연은 매입 일정표에 중장기채(coupon) 매입 등장",
    "YCC 또는 오퍼레이션 트위스트 언급",
    "SLR에서 국채 제외 (은행이 대신 사게 하는 우회 수단)",
    "MBS 상환분 재투자 대상을 단기채 → 중장기채로 변경",
    "연준의 국채 직접 인수(발행시장 매입) 논의 — 현행법상 금지",
    "개인 대상 국채 직접 판매 확대 (기관 수요 감소 방증)",
]


@dataclass
class Check:
    group: str
    label: str
    value: float | None
    fmt: str
    met: bool

    @property
    def value_text(self) -> str:
        if self.value is None:
            return "데이터 없음"
        return self.fmt.format(self.value).replace("$+", "+$").replace("$-", "-$")


# ── 시계열 헬퍼 ──────────────────────────────────────────────────────

def _clean(s: pd.Series | None) -> pd.Series | None:
    if s is None:
        return None
    s = s.dropna().sort_index()
    return None if s.empty else s


def _last(s: pd.Series | None) -> float | None:
    s = _clean(s)
    return None if s is None else float(s.iloc[-1])


def _past(s: pd.Series | None, days: int) -> float | None:
    s = _clean(s)
    if s is None:
        return None
    v = s.asof(s.index[-1] - pd.Timedelta(days=days))
    return None if pd.isna(v) else float(v)


def _change(s: pd.Series | None, days: int) -> float | None:
    """최신값 − days일 전 값 (주간 시리즈도 asof로 맞춤)."""
    last, past = _last(s), _past(s, days)
    return None if last is None or past is None else last - past


def _pct_change(s: pd.Series | None, days: int) -> float | None:
    last, past = _last(s), _past(s, days)
    return None if last is None or not past else (last / past - 1) * 100


def _mean_tail(s: pd.Series | None, n: int) -> float | None:
    s = _clean(s)
    return None if s is None else float(s.iloc[-n:].mean())


def _ge(v: float | None, th: float) -> bool:
    return v is not None and v >= th


def _le(v: float | None, th: float) -> bool:
    return v is not None and v <= th


# ── 판정 ─────────────────────────────────────────────────────────────

def assess(raw: dict[str, pd.Series]) -> dict:
    g = raw.get
    ten_y    = _last(g("DGS10"))
    ten_1m   = _change(g("DGS10"), 28)
    ten_90d  = _change(g("DGS10"), 90)
    hy       = _last(g("HY_OAS"))
    hy_1m    = _change(g("HY_OAS"), 28)
    vix      = _last(g("VIX"))
    usd_1m   = _pct_change(g("DXY"), 28)
    unrate   = _last(g("UNRATE"))
    cpi      = _last(g("CPI_ALL"))
    iorb_90d = _change(g("IORB"), 90)
    coupons  = _change(g("FED_COUPONS"), 91)
    bills    = _change(g("FED_BILLS"), 91)
    mbs      = _change(g("MBS"), 91)
    bs       = _change(g("BS"), 91)

    # 트랙 A
    a = [
        Check("1", f"10년물 금리 ≥ {TEN_Y_MIN:.1f}%",              ten_y, "{:.2f}%", _ge(ten_y, TEN_Y_MIN)),
        Check("1", f"10년물 기간 프리미엄 ≥ {TERM_PREMIUM_MIN:.1f}%p", _last(g("TERM_PREMIUM")), "{:.2f}%p",
              _ge(_last(g("TERM_PREMIUM")), TERM_PREMIUM_MIN)),
        Check("1", f"이자비용/GDP ≥ {INTEREST_GDP_MIN:.1f}%",       _last(g("INTEREST_GDP")), "{:.2f}%",
              _ge(_last(g("INTEREST_GDP")), INTEREST_GDP_MIN)),
        Check("1", f"국채 단기채 비중 ≥ {BILL_SHARE_MIN:.0f}%",     _last(g("DEBT_SHORT_SHARE")), "{:.1f}%",
              _ge(_last(g("DEBT_SHORT_SHARE")), BILL_SHARE_MIN)),
        Check("2", f"SOFR−IORB ≥ +{SOFR_SPR_MIN:.0f}bp",           _last(g("SOFR_SPR")), "{:+.0f}bp",
              _ge(_last(g("SOFR_SPR")), SOFR_SPR_MIN)),
        Check("2", f"상설레포(SRF) 5일 평균 ≥ ${SRF_5D_MIN:.0f}B",   _mean_tail(g("SRF"), 5), "${:,.1f}B",
              _ge(_mean_tail(g("SRF"), 5), SRF_5D_MIN)),
        Check("2", f"10년물 1개월 상승폭 ≥ +{TEN_Y_JUMP_MIN*100:.0f}bp", ten_1m, "{:+.2f}%p", _ge(ten_1m, TEN_Y_JUMP_MIN)),
        Check("2", f"하이일드 스프레드 ≥ {HY_OAS_MIN:.0f}bp + 1개월 확대", hy, "{:.0f}bp",
              _ge(hy, HY_OAS_MIN) and _ge(hy_1m, 0.01)),
        Check("2", f"VIX ≥ {VIX_MIN:.0f}",                         vix, "{:.1f}", _ge(vix, VIX_MIN)),
        Check("3", f"연준 단기채 13주 증가 ≥ ${BILLS_13W_MIN:.0f}B",   bills, "${:+,.0f}B", _ge(bills, BILLS_13W_MIN)),
        Check("4", f"연준 중장기채 13주 증가 ≥ ${COUPONS_13W_MIN:.0f}B", coupons, "${:+,.0f}B", _ge(coupons, COUPONS_13W_MIN)),
        Check("5", f"연준 중장기채 13주 증가 ≥ ${COUPONS_QE_MIN:.0f}B",  coupons, "${:+,.0f}B", _ge(coupons, COUPONS_QE_MIN)),
    ]
    met = {
        1: sum(c.met for c in a if c.group == "1") >= 2,
        **{s: any(c.met for c in a if c.group == str(s)) for s in (2, 3, 4, 5)},
    }
    stage = max((s for s, ok in met.items() if ok), default=0)

    # 4단계 이상일 때 유형 판별
    a_type = None
    if stage >= 4 and coupons:
        if bills is not None and bills < 0 and (bs is None or bs < 0.5 * coupons):
            a_type = ("트위스트형", "단기채를 팔아 중장기채를 삼 — 금리 방어지만 돈은 안 찍음")
        elif mbs is not None and coupons + mbs <= 0.5 * coupons and (bs is None or bs < 0.5 * coupons):
            a_type = ("재투자형", "MBS 상환분을 중장기채로 갈아탐 — 자산 구성만 변경")
        else:
            a_type = ("QE형 항복", "새 돈(지준)으로 중장기채 매입 — 장기금리 직접 방어")
        if _ge(_change(g("DGS10"), 28), 0.01) and _le(usd_1m, -0.01):
            a_type = ("항복 실패형", "매입에도 10년물 상승 + 달러 약세 — 신뢰 붕괴 신호 (금 최강 구간)")

    # 트랙 B
    hold_or_cut = _le(iorb_90d, 0)
    b = [
        Check("B", f"물가 경보: CPI 전년비 ≥ {CPI_ALERT:.1f}%", cpi, "{:.2f}%", _ge(cpi, CPI_ALERT)),
        Check("B", "연준 반응: 최근 90일 기준금리(IORB) 동결·인하", iorb_90d, "{:+.0f}bp", hold_or_cut),
        Check("B", "Bad Cut: 90일 내 인하 + 10년물 상승", ten_90d, "10Y {:+.2f}%p",
              _le(iorb_90d, -1) and _ge(ten_90d, 0.01)),
        Check("B", "모순 조합: 인상 + 중장기채 13주 ≥ $50B", coupons, "${:+,.0f}B",
              _ge(iorb_90d, 1) and _ge(coupons, COUPONS_13W_MIN)),
        Check("B", f"(참고) 스태그플레이션: CPI ≥ {CPI_ALERT}% + 실업률 ≥ {UNRATE_STAG}%", unrate, "실업률 {:.1f}%",
              _ge(cpi, CPI_ALERT) and _ge(unrate, UNRATE_STAG)),
    ]
    b_capitulated = (b[0].met and b[1].met) or b[2].met or b[3].met
    if b_capitulated:
        b_verdict = "항복 — 물가 경보 속 동결·인하 또는 재정 우위 신호"
    elif _ge(iorb_90d, 1):
        b_verdict = "항복 아님 (반대 방향: 최근 인상)"
    else:
        b_verdict = "항복 아님"

    # 위기형(긴급 빅컷) 임박 신호 + 달러 신뢰 균열
    crisis = [
        Check("X", f"실업률 ≥ {UNRATE_STAG}%",               unrate, "{:.1f}%", _ge(unrate, UNRATE_STAG)),
        Check("X", f"VIX ≥ {VIX_MIN:.0f}",                   vix, "{:.1f}", _ge(vix, VIX_MIN)),
        Check("X", f"하이일드 스프레드 ≥ {HY_OAS_MIN:.0f}bp", hy, "{:.0f}bp", _ge(hy, HY_OAS_MIN)),
        Check("X", f"달러지수(Broad) 1개월 ≥ +{DOLLAR_SPIKE_PCT:.0f}%", usd_1m, "{:+.2f}%", _ge(usd_1m, DOLLAR_SPIKE_PCT)),
    ]
    spiral = Check("S", f"달러 신뢰 균열: 10년물 1개월 ≥ +{SPIRAL_10Y_BP*100:.0f}bp & 달러 1개월 ≤ {SPIRAL_USD_PCT:.0f}%",
                   usd_1m, "달러 {:+.2f}%", _ge(ten_1m, SPIRAL_10Y_BP) and _le(usd_1m, SPIRAL_USD_PCT))

    return {
        "stage": stage, "a": a, "a_type": a_type,
        "b": b, "b_capitulated": b_capitulated, "b_verdict": b_verdict,
        "crisis": crisis, "crisis_n": sum(c.met for c in crisis), "spiral": spiral,
        "m2_yoy": _last(g("M2_YOY")), "m2_3m": _change(g("M2_YOY"), 92),
    }


# ── HTML 패널 ─────────────────────────────────────────────────────────

_PANEL_CSS = """
<style>
.cap-panel { background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
             padding: 18px 20px; margin-bottom: 36px; }
.cap-track { margin-top: 22px; }
.cap-track h3 { font-size: 17px; margin-bottom: 6px; color: var(--text); }
.cap-summary { width: 100%; border-collapse: collapse; margin: 8px 0 4px; font-size: 16px; }
.cap-summary td { padding: 6px 10px; border-bottom: 1px solid var(--border); }
.cap-summary .bad { color: var(--down); font-weight: 700; }
.cap-summary .ok { color: var(--up); font-weight: 700; }
.cap-summary .mid { color: var(--warn); font-weight: 700; }
.cap-ladder { display: grid; grid-template-columns: repeat(6, 1fr); gap: 8px; margin: 10px 0 14px; }
.cap-step { border: 1px solid var(--border); border-radius: 8px; padding: 10px; text-align: center;
            color: var(--muted); font-size: 14px; }
.cap-step .n { font-size: 20px; font-weight: 700; display: block; }
.cap-step.passed { border-color: var(--warn); color: var(--warn); }
.cap-step.current { border-color: var(--down); color: var(--text); background: rgba(248,81,73,.12);
                    box-shadow: 0 0 0 1px var(--down); }
.cap-step.current .n { color: var(--down); }
.cap-checks { width: 100%; border-collapse: collapse; font-size: 15px; }
.cap-checks td, .cap-checks th { padding: 6px 10px; border-bottom: 1px solid var(--border); text-align: left; }
.cap-checks th { color: var(--muted); font-weight: 600; }
.cap-checks .met { color: var(--down); font-weight: 700; }
.cap-checks .unmet { color: var(--muted); }
.cap-checks .v { font-family: 'JetBrains Mono', monospace; }
.cap-list { margin: 6px 0 0 20px; color: var(--muted); font-size: 15px; }
.cap-note { color: var(--muted); font-size: 13px; margin-top: 10px; }
@media (max-width: 760px) { .cap-ladder { grid-template-columns: repeat(3, 1fr); } }
</style>
"""


def _status(c: Check) -> str:
    return f'<td class="{"met" if c.met else "unmet"}">{"● 충족" if c.met else "○ 미충족"}</td>'


def _rows(checks: list[Check], group_label=None, rule=None) -> str:
    out, prev = [], None
    for c in checks:
        first = ""
        if group_label:
            if c.group != prev:
                n = sum(x.group == c.group for x in checks)
                first = (f'<td rowspan="{n}">{group_label(c.group)}<br>'
                         f'<span class="unmet">({rule.get(c.group, "충족 시")})</span></td>')
            prev = c.group
        out.append(f'<tr>{first}<td>{html.escape(c.label)}</td>'
                   f'<td class="v">{html.escape(c.value_text)}</td>{_status(c)}</tr>')
    return "".join(out)


def render_panel(raw: dict[str, pd.Series]) -> str:
    r = assess(raw)
    stage = r["stage"]
    today = pd.Timestamp.today().normalize()

    # 요약
    a_cls = "bad" if stage >= 4 else "mid" if stage >= 1 else "ok"
    a_txt = f"{stage}단계 · {STAGE_NAMES[stage]}" + (f" ({r['a_type'][0]})" if r["a_type"] else "")
    b_cls = "bad" if r["b_capitulated"] else "ok"
    x_cls = "bad" if r["crisis_n"] >= 2 else "mid" if r["crisis_n"] else "ok"
    summary = f"""
  <table class="cap-summary">
    <tr><td>A. 대차대조표 항복</td><td class="{a_cls}">{a_txt}</td><td>{STAGE_HINTS[stage]}</td></tr>
    <tr><td>B. 금리정책 항복 (재정지배)</td><td class="{b_cls}">{r['b_verdict']}</td><td>물가 경보인데 동결·인하하면 항복</td></tr>
    <tr><td>C. 제도적 항복</td><td class="unmet">수동 확인</td><td>직접 인수·YCC 등 정책 발표 — 아래 체크리스트</td></tr>
    <tr><td>위기형 빅컷 임박 신호</td><td class="{x_cls}">{r['crisis_n']} / 4</td><td>2개 이상이면 긴급 인하·QE 경로 경계</td></tr>
  </table>"""

    # 트랙 A
    steps = "".join(
        f'<div class="cap-step {"current" if i == stage else "passed" if i < stage else ""}">'
        f'<span class="n">{i}</span>{STAGE_NAMES[i]}</div>'
        for i in range(len(STAGE_NAMES))
    )
    a_rows = _rows(r["a"], lambda gr: f"{gr}단계 {STAGE_NAMES[int(gr)]}",
                   {"1": "4개 중 2개 이상", "2": "1개 이상"})
    a_type = (f'<div class="cap-note">유형 판별: <b>{r["a_type"][0]}</b> — {r["a_type"][1]}</div>'
              if r["a_type"] else
              '<div class="cap-note">유형 판별(QE형·트위스트형·재투자형·항복 실패형)은 4단계 이상에서 작동합니다.</div>')

    # 유동성 시차 시계
    if today < CLOCK_FROM:
        clock = f"효과 예상 구간까지 {(CLOCK_FROM - today).days}일"
    elif today <= CLOCK_TO:
        clock = "효과 예상 구간 진입 — CPI 3.5% 돌파 시 트랙 B 경계"
    else:
        clock = "효과 예상 구간 경과"
    m2 = ("M2 전년비 데이터 없음" if r["m2_yoy"] is None else
          f"M2 전년비 {r['m2_yoy']:.2f}%" + ("" if r["m2_3m"] is None else f" (3개월 {r['m2_3m']:+.2f}%p)"))

    c_items = "".join(f"<li>☐ {html.escape(t)}</li>" for t in TRACK_C_ITEMS)

    return f"""{_PANEL_CSS}
<section class="category-section cap-panel" id="fed-capitulation">
  <div class="category-header">
    <span class="category-name">🏳️ 연준 항복 판정</span>
    <span class="category-desc">대차대조표 · 금리정책(재정지배) · 제도 세 트랙으로 판정</span>
  </div>
  {summary}

  <div class="cap-track">
    <h3>A. 대차대조표 항복 — 새 돈으로 중장기 국채를 사는가</h3>
    <div class="cap-ladder">{steps}</div>
    <table class="cap-checks">
      <tr><th>단계</th><th>조건</th><th>현재값</th><th>상태</th></tr>
      {a_rows}
    </table>
    {a_type}
  </div>

  <div class="cap-track">
    <h3>B. 금리정책 항복 — 물가 경보 속에서도 금리를 못 올리는가</h3>
    <table class="cap-checks">
      <tr><th>조건</th><th>현재값</th><th>상태</th></tr>
      {_rows(r["b"])}
    </table>
    <div class="cap-note">판정: (물가 경보 AND 동결·인하) OR Bad Cut OR 모순 조합. 스태그플레이션 행은 참고용.</div>
  </div>

  <div class="cap-track">
    <h3>위기형 항복 — 긴급 빅컷·QE 임박 신호</h3>
    <table class="cap-checks">
      <tr><th>조건</th><th>현재값</th><th>상태</th></tr>
      {_rows(r["crisis"] + [r["spiral"]])}
    </table>
    <div class="cap-note">달러 신뢰 균열(10년물↑ + 달러↓)은 강제 매도에 따른 개입 압력과 금 강세를 함께 시사. 달러는 DXY 대신 Broad 지수 사용.</div>
  </div>

  <div class="cap-track">
    <h3>C. 제도적 항복 — 정책 신호 (수동 확인)</h3>
    <ul class="cap-list">{c_items}</ul>
  </div>

  <div class="cap-track">
    <h3>유동성 시차 시계 (강의 관점, 판정과 별도)</h3>
    <div class="cap-note" style="font-size:15px">기준점 2025-12 (단기채 매입 개시) → 효과 예상 2026-12 ~ 2027-06 · {clock} · {m2}</div>
  </div>

  <div class="cap-note">임계값은 경험칙이며 과거 국면으로 검증하지 않았습니다. 미수집: 국채 입찰 결과, 딜러 포지션, 해외 공적 보유, TIC, 일본 10년물.</div>
</section>"""


if __name__ == "__main__":
    idx_w = pd.date_range("2026-01-07", periods=40, freq="W-WED")
    idx_d = pd.bdate_range("2026-01-01", periods=200)
    ramp = pd.Series(range(40), idx_w, dtype=float)
    base = {
        "DGS10": pd.Series(4.0, idx_d), "TERM_PREMIUM": pd.Series(0.5, idx_d),
        "INTEREST_GDP": pd.Series(3.0, idx_w), "DEBT_SHORT_SHARE": pd.Series(20.0, idx_w),
        "SOFR_SPR": pd.Series(-3.0, idx_d), "SRF": pd.Series(0.0, idx_d),
        "HY_OAS": pd.Series(300.0, idx_d), "VIX": pd.Series(15.0, idx_d), "DXY": pd.Series(120.0, idx_d),
        "UNRATE": pd.Series(4.0, idx_w), "CPI_ALL": pd.Series(2.5, idx_w), "IORB": pd.Series(390.0, idx_d),
        "FED_BILLS": pd.Series(200.0, idx_w), "FED_COUPONS": pd.Series(3600.0, idx_w),
        "MBS": pd.Series(2000.0, idx_w), "BS": pd.Series(6700.0, idx_w),
    }
    A = lambda **kw: assess({**base, **kw})
    # 트랙 A 단계
    assert A()["stage"] == 0
    assert A(DGS10=pd.Series(5.2, idx_d))["stage"] == 0
    assert A(DGS10=pd.Series(5.2, idx_d), INTEREST_GDP=pd.Series(3.9, idx_w))["stage"] == 1
    assert A(SOFR_SPR=pd.Series(8.0, idx_d))["stage"] == 2
    assert A(VIX=pd.Series(35.0, idx_d))["stage"] == 2
    assert A(HY_OAS=pd.Series(450.0, idx_d))["stage"] == 0                      # 수준만 높고 확대 아님
    assert A(HY_OAS=pd.Series(300.0, idx_d) + pd.Series(range(200), idx_d))["stage"] == 2
    assert A(FED_BILLS=200 + ramp * 9)["stage"] == 3
    assert A(FED_COUPONS=3600 + ramp * 5)["stage"] == 4
    assert A(FED_COUPONS=3600 + ramp * 15)["stage"] == 5
    assert assess({})["stage"] == 0
    # 유형 판별
    qe = dict(FED_COUPONS=3600 + ramp * 15, BS=6700 + ramp * 15)
    assert A(**qe)["a_type"][0] == "QE형 항복"
    assert A(FED_COUPONS=3600 + ramp * 15, FED_BILLS=600 - ramp * 15)["a_type"][0] == "트위스트형"
    assert A(FED_COUPONS=3600 + ramp * 15, MBS=2000 - ramp * 15)["a_type"][0] == "재투자형"
    up = pd.Series([4.0 + i * 0.01 for i in range(200)], idx_d)
    down = pd.Series([120.0 - i * 0.05 for i in range(200)], idx_d)
    assert A(**qe, DGS10=up, DXY=down)["a_type"][0] == "항복 실패형"
    # 트랙 B
    hike = pd.Series([365.0] * 150 + [390.0] * 50, idx_d)
    cut = pd.Series([390.0] * 150 + [365.0] * 50, idx_d)
    assert A(CPI_ALL=pd.Series(3.7, idx_w), IORB=hike)["b_capitulated"] is False
    assert A(CPI_ALL=pd.Series(3.7, idx_w))["b_capitulated"] is True             # 물가 경보 + 동결
    assert A(CPI_ALL=pd.Series(2.5, idx_w))["b_capitulated"] is False
    assert A(IORB=cut, DGS10=up)["b_capitulated"] is True                         # Bad Cut
    assert A(IORB=hike, FED_COUPONS=3600 + ramp * 5)["b_capitulated"] is True     # 모순 조합
    # 위기 신호
    assert A(UNRATE=pd.Series(4.6, idx_w), VIX=pd.Series(31.0, idx_d))["crisis_n"] == 2
    steep_up = pd.Series([4.0 + i * 0.02 for i in range(200)], idx_d)
    steep_down = pd.Series([120.0 - i * 0.2 for i in range(200)], idx_d)
    assert A(DGS10=steep_up, DXY=steep_down)["spiral"].met
    assert not A(DGS10=up, DXY=down)["spiral"].met                                # 완만하면 미충족
    assert "fed-capitulation" in render_panel(base) and "fed-capitulation" in render_panel({})
    print("capitulation self-check OK")
