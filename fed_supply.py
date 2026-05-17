"""
indicators/global_dollar.py ─ ⑤ 글로벌 달러 유동성 지표
=====================================================================
역외 달러 공급과 환율 메커니즘 3개 지표를 진단합니다.
SWPT / DXY / JPY
"""

import numpy as np
import pandas as pd

from config import GLOBAL_DOLLAR
from indicators.base import BaseIndicatorGroup, IndicatorResult


class GlobalDollarGroup(BaseIndicatorGroup):
    """⑤ 글로벌 달러 유동성 지표 그룹."""

    category_num  = 5
    category_name = "⑤ 글로벌 달러 유동성 지표"

    def diagnose(self) -> list[IndicatorResult]:
        """GLOBAL_DOLLAR 목록의 지표를 진단합니다."""
        results: list[IndicatorResult] = []

        for meta in GLOBAL_DOLLAR:
            series = self.raw.get(meta.key, pd.Series(dtype=float))
            result = self._compute_result(meta, series)

            enrichers = {
                "SWPT": self._swpt_context,
                "DXY":  self._dxy_context,
                "JPY":  self._jpy_context,
            }
            if meta.key in enrichers:
                result.context = enrichers[meta.key](result, series)

            results.append(result)

        return results

    # ── 개별 지표 컨텍스트 ────────────────────────────────────────────

    @staticmethod
    def _swpt_context(r: IndicatorResult, s: pd.Series) -> str:
        parts = []
        val = r.latest_value
        if not np.isnan(val):
            if val > 200:
                parts.append(f"${val:,.0f}B — 위기 수준 달러 공급 (2020형 급증)")
            elif val > 50:
                parts.append(f"${val:,.0f}B — 일부 역외 달러 부족 진행 중")
            else:
                parts.append(f"${val:,.0f}B — 낮은 수준 (정상)")
        if r.chg_1m is not None and not np.isnan(r.chg_1m) and r.chg_1m > 10:
            parts.append(f"⚠ 1개월 ${r.chg_1m:+,.0f}B 급증")
        return " | ".join(parts)

    @staticmethod
    def _dxy_context(r: IndicatorResult, s: pd.Series) -> str:
        parts = []
        if r.pct_rank is not None:
            parts.append(f"5년 기간 중 {r.pct_rank:.0f}% 수준")
        if r.chg_1m is not None and not np.isnan(r.chg_1m):
            dir_s = "강세" if r.chg_1m >= 0 else "약세"
            parts.append(f"1개월 달러 {dir_s} {r.chg_1m:+.1f}pt")
        if r.chg_1y is not None and not np.isnan(r.chg_1y):
            dir_s = "강세" if r.chg_1y >= 0 else "약세"
            parts.append(f"1년 {dir_s} {r.chg_1y:+.1f}pt")
        return " | ".join(parts)

    @staticmethod
    def _jpy_context(r: IndicatorResult, s: pd.Series) -> str:
        """JPY/USD: 수치 하락 = 엔 강세 = 캐리 청산 리스크."""
        parts = []
        val = r.latest_value
        if not np.isnan(val):
            if val < 130:
                parts.append(f"{val:.1f} — 엔 강세 (캐리 청산 위험 구간)")
            elif val > 150:
                parts.append(f"{val:.1f} — 엔 약세 (캐리 누적 구간)")
            else:
                parts.append(f"{val:.1f}")
        if r.chg_1m is not None and not np.isnan(r.chg_1m):
            dir_s = "엔 약세" if r.chg_1m >= 0 else "엔 강세"
            parts.append(f"1개월 {dir_s} {r.chg_1m:+.1f}")
        return " | ".join(parts)
