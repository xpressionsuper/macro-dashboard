"""
indicators/fed_supply.py ─ ① 연준(Fed) 직접 공급 지표
=====================================================================
Fed가 직접 조절하는 유동성 채널 6개 지표를 진단합니다.
  BS / UST / MBS / RRP / RESERVES / DISC_WIN
"""

import pandas as pd

from config import FED_SUPPLY
from indicators.base import BaseIndicatorGroup, IndicatorResult


class FedSupplyGroup(BaseIndicatorGroup):
    """① 연준(Fed) 직접 공급 지표 그룹."""

    category_num  = 1
    category_name = "① 연준(Fed) 직접 공급 지표"

    def diagnose(self) -> list[IndicatorResult]:
        """FED_SUPPLY 목록의 모든 지표를 순서대로 진단합니다."""
        results: list[IndicatorResult] = []

        for meta in FED_SUPPLY:
            series = self.raw.get(meta.key, pd.Series(dtype=float))
            result = self._compute_result(meta, series)

            # ── 지표별 추가 컨텍스트 보강 ─────────────────────────────
            result.context = self._enrich_context(meta.key, result, series)
            results.append(result)

        return results

    # ── 지표별 추가 컨텍스트 ─────────────────────────────────────────

    def _enrich_context(
        self, key: str, r: IndicatorResult, s: pd.Series
    ) -> str:
        """기본 컨텍스트에 지표 특유의 해석을 추가합니다."""
        base = r.context

        if key == "BS" and not s.empty:
            peak = float(s.max())
            diff = float(s.iloc[-1]) - peak
            return f"{base} | 피크(${peak:,.0f}B) 대비 ${diff:,.0f}B"

        if key == "RRP" and not s.empty:
            # RRP가 매우 낮으면 시중 유동성 방출 거의 완료
            if r.latest_value < 100:
                return f"{base} | RRP 잔고 $100B 미만 → 방출 여력 소진 단계"

        if key == "DISC_WIN" and not s.empty:
            # 할인창구 급증 여부
            if r.chg_1m is not None and r.chg_1m > 10:
                return f"{base} | ⚠ 1개월 $10B+ 급증 — 스트레스 신호 확인 필요"

        return base
