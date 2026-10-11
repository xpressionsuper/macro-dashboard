"""
indicators/capitulation.py ─ 연준 항복(Fed Capitulation) 단계 판정
=====================================================================
'연준의 항복' = 연준이 장기금리를 누르기 위해 스스로 중장기 국채를 사들이는 것.
단기채(Bills) 매입은 준비금 관리용이라 항복이 아니고, 중장기채(Notes·Bonds)
매입부터 항복으로 봅니다.

  0  평시               아무 조건도 충족 안 됨
  1  압박 누적          금리·재정 압박 4개 중 2개 이상
  2  시장 기능 경고     국채·단기자금 시장 이상 1개 이상
  3  대차대조표 재확장  단기채 13주 +$50B 이상 (준비금 관리 매입)
  4  항복 초기          중장기채 13주 +$50B 이상
  5  본격 항복 (QE급)   중장기채 13주 +$150B 이상

현재 단계 = 조건이 충족된 가장 높은 단계.
ponytail: 임계값은 경험적 기준이라 아래 상수에서 조정.
"""

from __future__ import annotations

import html
from dataclasses import dataclass

import pandas as pd

TEN_Y_MIN        = 5.0    # %
TERM_PREMIUM_MIN = 1.0    # %p
INTEREST_GDP_MIN = 3.5    # %
BILL_SHARE_MIN   = 22.0   # %
SOFR_SPR_MIN     = 5.0    # bps
SRF_5D_MIN       = 10.0   # $B
TEN_Y_JUMP_MIN   = 0.50   # %p, 약 1개월(20영업일)
BILLS_13W_MIN    = 50.0   # $B
COUPONS_13W_MIN  = 50.0   # $B
COUPONS_QE_MIN   = 150.0  # $B

STAGE_NAMES = ["평시", "압박 누적", "시장 기능 경고", "대차대조표 재확장", "항복 초기", "본격 항복"]
STAGE_HINTS = [
    "금리·재정·시장 모두 정상 범위",
    "장기금리·기간 프리미엄·재정 부담이 쌓이는 중 (2개 이상)",
    "국채·단기자금 시장에서 기능 이상 신호 (1개 이상)",
    "단기채 매입으로 대차대조표 재확장 — 아직 항복 아님",
    "중장기채 매입 시작 — 장기금리 직접 개입",
    "QE급 중장기채 매입 (13주 +$150B 이상)",
]


@dataclass
class Check:
    stage: int
    label: str
    value: float | None
    fmt: str
    met: bool

    @property
    def value_text(self) -> str:
        if self.value is None:
            return "데이터 없음"
        return self.fmt.format(self.value).replace("$+", "+$").replace("$-", "-$")


def _last(s: pd.Series | None) -> float | None:
    if s is None or s.dropna().empty:
        return None
    return float(s.dropna().iloc[-1])


def _change(s: pd.Series | None, days: int) -> float | None:
    """최신값 − days일 전 값 (주간 시리즈도 asof로 맞춤)."""
    if s is None or s.dropna().empty:
        return None
    s = s.dropna().sort_index()
    past = s.asof(s.index[-1] - pd.Timedelta(days=days))
    return None if pd.isna(past) else float(s.iloc[-1] - past)


def _mean_tail(s: pd.Series | None, n: int) -> float | None:
    if s is None or s.dropna().empty:
        return None
    return float(s.dropna().iloc[-n:].mean())


def _ge(v: float | None, th: float) -> bool:
    return v is not None and v >= th


def assess(raw: dict[str, pd.Series]) -> tuple[int, list[Check]]:
    """raw 시계열에서 (현재 단계, 조건 목록)을 계산합니다."""
    ten_y   = _last(raw.get("DGS10"))
    tp      = _last(raw.get("TERM_PREMIUM"))
    int_gdp = _last(raw.get("INTEREST_GDP"))
    bshare  = _last(raw.get("DEBT_SHORT_SHARE"))
    spr     = _last(raw.get("SOFR_SPR"))
    srf5    = _mean_tail(raw.get("SRF"), 5)
    jump    = _change(raw.get("DGS10"), 28)
    bills   = _change(raw.get("FED_BILLS"), 91)
    coupons = _change(raw.get("FED_COUPONS"), 91)

    checks = [
        Check(1, f"10년물 금리 ≥ {TEN_Y_MIN:.1f}%",            ten_y,   "{:.2f}%",    _ge(ten_y, TEN_Y_MIN)),
        Check(1, f"기간 프리미엄 ≥ {TERM_PREMIUM_MIN:.1f}%p",   tp,      "{:.2f}%p",   _ge(tp, TERM_PREMIUM_MIN)),
        Check(1, f"이자비용/GDP ≥ {INTEREST_GDP_MIN:.1f}%",     int_gdp, "{:.2f}%",    _ge(int_gdp, INTEREST_GDP_MIN)),
        Check(1, f"국채 단기채 비중 ≥ {BILL_SHARE_MIN:.0f}%",   bshare,  "{:.1f}%",    _ge(bshare, BILL_SHARE_MIN)),
        Check(2, f"SOFR−IORB ≥ +{SOFR_SPR_MIN:.0f}bp",         spr,     "{:+.0f}bp",  _ge(spr, SOFR_SPR_MIN)),
        Check(2, f"상설레포(SRF) 5일 평균 ≥ ${SRF_5D_MIN:.0f}B", srf5,    "${:,.1f}B",  _ge(srf5, SRF_5D_MIN)),
        Check(2, f"10년물 1개월 급등 ≥ +{TEN_Y_JUMP_MIN*100:.0f}bp", jump, "{:+.2f}%p", _ge(jump, TEN_Y_JUMP_MIN)),
        Check(3, f"연준 단기채 13주 증가 ≥ ${BILLS_13W_MIN:.0f}B",   bills,   "${:+,.0f}B", _ge(bills, BILLS_13W_MIN)),
        Check(4, f"연준 중장기채 13주 증가 ≥ ${COUPONS_13W_MIN:.0f}B", coupons, "${:+,.0f}B", _ge(coupons, COUPONS_13W_MIN)),
        Check(5, f"연준 중장기채 13주 증가 ≥ ${COUPONS_QE_MIN:.0f}B",  coupons, "${:+,.0f}B", _ge(coupons, COUPONS_QE_MIN)),
    ]

    met = {
        1: sum(c.met for c in checks if c.stage == 1) >= 2,
        2: any(c.met for c in checks if c.stage == 2),
        3: any(c.met for c in checks if c.stage == 3),
        4: any(c.met for c in checks if c.stage == 4),
        5: any(c.met for c in checks if c.stage == 5),
    }
    stage = max((s for s, ok in met.items() if ok), default=0)
    return stage, checks


# ── HTML 패널 ─────────────────────────────────────────────────────────

_PANEL_CSS = """
<style>
.cap-panel { background: var(--surface); border: 1px solid var(--border); border-radius: 10px;
             padding: 18px 20px; margin-bottom: 36px; }
.cap-verdict { font-size: 18px; margin: 6px 0 16px; }
.cap-verdict b { color: var(--accent); }
.cap-ladder { display: grid; grid-template-columns: repeat(6, 1fr); gap: 8px; margin-bottom: 18px; }
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
.cap-note { color: var(--muted); font-size: 13px; margin-top: 10px; }
@media (max-width: 760px) { .cap-ladder { grid-template-columns: repeat(3, 1fr); } }
</style>
"""


def render_panel(raw: dict[str, pd.Series]) -> str:
    stage, checks = assess(raw)

    steps = "".join(
        f'<div class="cap-step {"current" if i == stage else "passed" if i < stage else ""}">'
        f'<span class="n">{i}</span>{STAGE_NAMES[i]}</div>'
        for i in range(len(STAGE_NAMES))
    )

    rule = {1: "4개 중 2개 이상", 2: "1개 이상", 3: "충족 시", 4: "충족 시", 5: "충족 시"}
    rows, prev = [], None
    for c in checks:
        stage_cell = (f'<td rowspan="{sum(x.stage == c.stage for x in checks)}">'
                      f'{c.stage}단계 {STAGE_NAMES[c.stage]}<br>'
                      f'<span class="unmet">({rule[c.stage]})</span></td>') if c.stage != prev else ""
        prev = c.stage
        cls = "met" if c.met else "unmet"
        rows.append(
            f'<tr>{stage_cell}<td>{html.escape(c.label)}</td>'
            f'<td class="v">{html.escape(c.value_text)}</td>'
            f'<td class="{cls}">{"● 충족" if c.met else "○ 미충족"}</td></tr>'
        )

    return f"""{_PANEL_CSS}
<section class="category-section cap-panel" id="fed-capitulation">
  <div class="category-header">
    <span class="category-name">🏳️ 연준 항복 판정</span>
    <span class="category-desc">연준이 장기금리를 누르려고 중장기 국채를 직접 사들이는지 단계별로 판정</span>
  </div>
  <div class="cap-verdict">현재 <b>{stage}단계 · {STAGE_NAMES[stage]}</b> — {STAGE_HINTS[stage]}</div>
  <div class="cap-ladder">{steps}</div>
  <table class="cap-checks">
    <tr><th>단계</th><th>조건</th><th>현재값</th><th>상태</th></tr>
    {''.join(rows)}
  </table>
  <div class="cap-note">현재 단계 = 조건이 충족된 가장 높은 단계. 4단계부터가 항복이며,
    3단계(단기채 매입)는 준비금 관리용 기술적 매입입니다. 13주 변화는 연준 H.4.1 주간 잔고 기준.</div>
</section>"""


if __name__ == "__main__":
    idx_w = pd.date_range("2026-01-07", periods=40, freq="W-WED")
    idx_d = pd.bdate_range("2026-01-01", periods=200)
    base = {
        "DGS10": pd.Series(4.0, idx_d), "TERM_PREMIUM": pd.Series(0.5, idx_d),
        "INTEREST_GDP": pd.Series(3.0, idx_w), "DEBT_SHORT_SHARE": pd.Series(20.0, idx_w),
        "SOFR_SPR": pd.Series(-3.0, idx_d), "SRF": pd.Series(0.0, idx_d),
        "FED_BILLS": pd.Series(200.0, idx_w), "FED_COUPONS": pd.Series(3600.0, idx_w),
    }
    assert assess(base)[0] == 0
    assert assess({**base, "DGS10": pd.Series(5.2, idx_d)})[0] == 0          # 압박 1개로는 부족
    assert assess({**base, "DGS10": pd.Series(5.2, idx_d), "INTEREST_GDP": pd.Series(3.9, idx_w)})[0] == 1
    assert assess({**base, "SOFR_SPR": pd.Series(8.0, idx_d)})[0] == 2
    ramp = pd.Series(range(40), idx_w, dtype=float)
    assert assess({**base, "FED_BILLS": 200 + ramp * 9})[0] == 3             # 13주 +$117B
    assert assess({**base, "FED_COUPONS": 3600 + ramp * 5})[0] == 4          # 13주 +$65B
    assert assess({**base, "FED_COUPONS": 3600 + ramp * 15})[0] == 5         # 13주 +$195B
    assert assess({})[0] == 0                                                 # 데이터 없음 → 평시
    assert "fed-capitulation" in render_panel(base)
    print("capitulation self-check OK")
