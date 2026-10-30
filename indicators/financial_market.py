"""
indicators/financial_market.py ─ ④ 금융시장 유동성 지표
=====================================================================
시장에서 관측되는 유동성 가격·스트레스 신호 10개 지표를 진단합니다.
SOFR / IORB / HY_OAS / BBB_OAS / IG_OAS / NFCI / VIX / DGS2 / DGS10 / T10Y2Y
"""

import numpy as np
import pandas as pd

from config import FINANCIAL_MARKET
from indicators.base import BaseIndicatorGroup, IndicatorResult


class FinancialMarketGroup(BaseIndicatorGroup):
    """④ 금융시장 유동성 지표 그룹."""

    category_num  = 4
    category_name = "④ 금융시장 유동성 지표"

    def diagnose(self) -> list[IndicatorResult]:
        """FINANCIAL_MARKET 목록의 지표를 진단합니다."""
        results: list[IndicatorResult] = []

        for meta in FINANCIAL_MARKET:
            series = self.raw.get(meta.key, pd.Series(dtype=float))
            result = self._compute_result(meta, series)

            # ── 지표별 보강 ────────────────────────────────────────────
            enrichers = {
                "VIX":    self._vix_context,
                "NFCI":   self._nfci_context,
                "HY_OAS": self._hy_context,
                "T10Y2Y": self._yield_curve_context,
            }
            if meta.key in enrichers:
                result.context = enrichers[meta.key](result)

            results.append(result)

        return results

    # ── 개별 지표 컨텍스트 ────────────────────────────────────────────

    @staticmethod
    def _vix_context(r: IndicatorResult) -> str:
        val = r.latest_value
        parts = []
        if not np.isnan(val):
            if val >= 30:
                parts.append(f"{val:.1f} — 고공포 구간 (30+)")
            elif val >= 20:
                parts.append(f"{val:.1f} — 경계 구간 (20~30)")
            else:
                parts.append(f"{val:.1f} — 안정 구간 (20 미만)")
        if r.pct_rank is not None:
            parts.append(f"5년 기간 중 {r.pct_rank:.0f}% 수준")
        if r.chg_1m is not None and not np.isnan(r.chg_1m):
            dir_s = "급등" if r.chg_1m > 5 else ("상승" if r.chg_1m > 0 else "하락")
            parts.append(f"1개월 {dir_s} {r.chg_1m:+.1f}pt")
        return " | ".join(parts)

    @staticmethod
    def _nfci_context(r: IndicatorResult) -> str:
        val = r.latest_value
        parts = []
        if not np.isnan(val):
            if val > 0.5:
                parts.append(f"{val:+.3f} — 금융 긴축 상태")
            elif val > 0:
                parts.append(f"{val:+.3f} — 소폭 긴축")
            elif val > -0.5:
                parts.append(f"{val:+.3f} — 소폭 완화")
            else:
                parts.append(f"{val:+.3f} — 금융 완화 상태")
        if r.pct_rank is not None:
            parts.append(f"5년 기간 중 {r.pct_rank:.0f}% 수준")
        return " | ".join(parts)

    @staticmethod
    def _hy_context(r: IndicatorResult) -> str:
        val = r.latest_value
        parts = []
        if not np.isnan(val):
            if val > 600:
                parts.append(f"{val:.0f}bps — 위기 수준 (600bps+)")
            elif val > 400:
                parts.append(f"{val:.0f}bps — 경계 확대 (400~600bps)")
            else:
                parts.append(f"{val:.0f}bps — 안정 구간 (400bps 미만)")
        if r.pct_rank is not None:
            parts.append(f"5년 기간 중 {r.pct_rank:.0f}% 수준")
        if r.chg_3m is not None and not np.isnan(r.chg_3m):
            dir_s = "확대" if r.chg_3m >= 0 else "축소"
            parts.append(f"3개월 {dir_s} {abs(r.chg_3m):.0f}bps")
        return " | ".join(parts)

    @staticmethod
    def _yield_curve_context(r: IndicatorResult) -> str:
        val = r.latest_value
        parts = []
        if not np.isnan(val):
            if val < -0.5:
                parts.append(f"{val:.2f}% — 역전 심화 (경기침체 선행 신호)")
            elif val < 0:
                parts.append(f"{val:.2f}% — 역전 상태")
            elif val < 0.5:
                parts.append(f"{val:.2f}% — 정상화 초입")
            else:
                parts.append(f"{val:.2f}% — 양의 기울기 정상화")
        if r.chg_3m is not None and not np.isnan(r.chg_3m):
            dir_s = "가팔라짐" if r.chg_3m >= 0 else "평탄화"
            parts.append(f"3개월 {dir_s} {r.chg_3m:+.2f}%")
        return " | ".join(parts)
