"""
indicators/net_liquidity.py ─ ③ 합성(Net) 유동성 지표
=====================================================================
파생 계산 지표(넷 유동성, SOFR 스프레드, M2 증가율)를 진단합니다.
"""

import numpy as np
import pandas as pd

from config import NET_LIQUIDITY
from indicators.base import BaseIndicatorGroup, IndicatorResult


class NetLiquidityGroup(BaseIndicatorGroup):
    """③ 합성(Net) 유동성 지표 그룹."""

    category_num  = 3
    category_name = "③ 합성(Net) 유동성 지표"

    def diagnose(self) -> list[IndicatorResult]:
        """NET_LIQUIDITY 목록의 지표를 진단합니다."""
        results: list[IndicatorResult] = []

        for meta in NET_LIQUIDITY:
            series = self.raw.get(meta.key, pd.Series(dtype=float))
            result = self._compute_result(meta, series)

            # ── 지표별 보강 ────────────────────────────────────────────
            if meta.key == "NET_LIQ":
                result.context = self._net_liq_context(result, series)
            elif meta.key == "SOFR_SPR":
                result.context = self._sofr_spr_context(result)
            elif meta.key == "M2_YOY":
                result.context = self._m2_yoy_context(result)

            results.append(result)

        return results

    # ── 개별 지표 컨텍스트 ────────────────────────────────────────────

    @staticmethod
    def _net_liq_context(r: IndicatorResult, s: pd.Series) -> str:
        parts = []
        if r.pct_rank is not None:
            parts.append(f"5년 기간 중 {r.pct_rank:.0f}% 수준")
        if not s.empty and not np.isnan(r.latest_value):
            peak = float(s.max())
            trough = float(s.min())
            parts.append(f"5년 범위 ${trough:,.0f}B ~ ${peak:,.0f}B")
        if r.chg_3m is not None and not np.isnan(r.chg_3m):
            dir_s = "확대" if r.chg_3m >= 0 else "축소"
            parts.append(f"3개월 {dir_s} ${abs(r.chg_3m):,.0f}B")
        return " | ".join(parts)

    @staticmethod
    def _sofr_spr_context(r: IndicatorResult) -> str:
        """SOFR-IORB 스프레드 해석. 양수=SOFR이 IORB 위에 있음."""
        parts = []
        val = r.latest_value
        if not np.isnan(val):
            if val > 10:
                parts.append(f"{val:.1f}bps — SOFR가 IORB 상회 중 (지급준비금 희소 신호)")
            elif val > 0:
                parts.append(f"{val:.1f}bps — 정상 범위 (소폭 양수)")
            else:
                parts.append(f"{val:.1f}bps — SOFR가 IORB 하회 (지급준비금 충분)")
        if r.pct_rank is not None:
            parts.append(f"5년 기간 중 {r.pct_rank:.0f}% 수준")
        return " | ".join(parts)

    @staticmethod
    def _m2_yoy_context(r: IndicatorResult) -> str:
        """M2 전년비 증가율 해석."""
        parts = []
        val = r.latest_value
        if not np.isnan(val):
            if val > 5:
                parts.append(f"{val:.1f}% — 팽창적 통화 공급")
            elif val > 0:
                parts.append(f"{val:.1f}% — 완만한 증가")
            else:
                parts.append(f"{val:.1f}% — 마이너스 성장 (긴축 효과 확인)")
        if r.chg_1y is not None:
            trend = "개선" if r.chg_1y >= 0 else "둔화"
            parts.append(f"전년 대비 추세 {trend}")
        return " | ".join(parts)
