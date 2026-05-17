"""
indicators/treasury_channel.py ─ ② 재무부(Treasury) 채널 지표
=====================================================================
TGA 잔고를 진단합니다.
부채한도 협상 국면에서 TGA 급감(방출) / 급증(흡수) 패턴을 포착합니다.
"""

import numpy as np
import pandas as pd

from config import TREASURY_CHANNEL
from indicators.base import BaseIndicatorGroup, IndicatorResult


class TreasuryChannelGroup(BaseIndicatorGroup):
    """② 재무부(Treasury) 채널 지표 그룹."""

    category_num  = 2
    category_name = "② 재무부(Treasury) 채널 지표"

    def diagnose(self) -> list[IndicatorResult]:
        """TREASURY_CHANNEL 목록의 지표를 진단합니다."""
        results: list[IndicatorResult] = []

        for meta in TREASURY_CHANNEL:
            series = self.raw.get(meta.key, pd.Series(dtype=float))
            result = self._compute_result(meta, series)

            # ── TGA 특화 컨텍스트 ──────────────────────────────────────
            if meta.key == "TGA" and not series.empty:
                result.context = self._tga_context(result, series)

            results.append(result)

        return results

    # ── TGA 특화 해석 ─────────────────────────────────────────────────

    @staticmethod
    def _tga_context(r: IndicatorResult, s: pd.Series) -> str:
        """
        TGA 잔고 수준에 따른 팩트 기반 해석.
        - $500B 이상: 유동성 흡수 상태
        - $200B 이하: 부채한도 접근 또는 의도적 방출
        """
        parts = []

        if r.pct_rank is not None:
            parts.append(f"5년 기간 중 {r.pct_rank:.0f}% 수준")

        val = r.latest_value
        if not np.isnan(val):
            if val > 500:
                parts.append(f"${val:,.0f}B — 유동성 흡수 상태")
            elif val < 200:
                parts.append(f"${val:,.0f}B — 부채한도 접근 또는 의도적 방출 구간")
            else:
                parts.append(f"${val:,.0f}B")

        if r.chg_1m is not None and not np.isnan(r.chg_1m):
            sign = "+" if r.chg_1m >= 0 else ""
            parts.append(f"1개월 변화 {sign}${r.chg_1m:,.0f}B")

        return " | ".join(parts)
