"""
indicators/base.py ─ 지표 기반 클래스 & 진단 결과 구조체
=====================================================================
모든 지표 그룹 클래스가 상속하는 추상 기반 클래스.
공통 진단 계산 로직(변화량, 퍼센타일, 추세 라벨)을 제공합니다.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional

import numpy as np
import pandas as pd


# ══════════════════════════════════════════════════════════════════════
#  진단 결과 구조체
# ══════════════════════════════════════════════════════════════════════
@dataclass
class IndicatorResult:
    """단일 지표의 진단 결과를 보유합니다 (점수 없음, 사실 기반)."""

    # ── 메타 ──────────────────────────────────────────────────────────
    key:         str
    name_ko:     str
    name_en:     str
    description: str
    direction:   str
    source:      str
    category:    int
    unit:        str

    # ── 현재값 ────────────────────────────────────────────────────────
    latest_value: float
    latest_date:  str

    # ── 변화량 ────────────────────────────────────────────────────────
    chg_1w:  Optional[float] = None    # 1주(7일) 변화
    chg_1m:  Optional[float] = None    # 1개월(30일) 변화
    chg_3m:  Optional[float] = None    # 3개월(90일) 변화
    chg_1y:  Optional[float] = None    # 1년(365일) 변화

    # ── 컨텍스트 ──────────────────────────────────────────────────────
    pct_rank:    Optional[float] = None   # 5년 내 퍼센타일 순위 (0~100)
    trend_label: str = ""                 # "5년 최고", "역대 최저" 등
    context:     str = ""                 # 현 수준 요약 문장

    # ── 차트용 원시 시계열 ────────────────────────────────────────────
    series: pd.Series = field(default_factory=lambda: pd.Series(dtype=float))


# ══════════════════════════════════════════════════════════════════════
#  추상 기반 클래스
# ══════════════════════════════════════════════════════════════════════
class BaseIndicatorGroup(abc.ABC):
    """
    지표 그룹(카테고리)의 공통 인터페이스.

    서브클래스는 `diagnose()`만 구현하면 되며,
    나머지 진단 계산은 `_compute_result()` 헬퍼가 처리합니다.
    """

    category_num:  int = 0
    category_name: str = ""

    def __init__(self, raw: dict[str, pd.Series]):
        """
        Args:
            raw: {key: Series} 딕셔너리 (FRED 원시 + 파생 지표 포함).
        """
        self.raw = raw

    # ── 추상 메서드 ──────────────────────────────────────────────────

    @abc.abstractmethod
    def diagnose(self) -> list[IndicatorResult]:
        """이 그룹의 모든 지표 진단 결과를 반환합니다."""
        ...

    # ── 공통 헬퍼 ────────────────────────────────────────────────────

    def _compute_result(
        self,
        meta,                       # IndicatorMeta
        series: pd.Series,
    ) -> IndicatorResult:
        """
        단일 지표에 대한 IndicatorResult를 계산합니다.

        Args:
            meta:   config.py 의 IndicatorMeta
            series: 시계열 데이터
        """
        if series.empty:
            return IndicatorResult(
                key=meta.key, name_ko=meta.name_ko, name_en=meta.name_en,
                description=meta.description, direction=meta.direction,
                source=meta.source, category=meta.category, unit=meta.unit,
                latest_value=float("nan"), latest_date="N/A",
                context="데이터 수집 실패",
            )

        s = series.dropna().sort_index()

        latest_value = float(s.iloc[-1])
        latest_date  = s.index[-1].strftime("%Y-%m-%d")

        chg_1w = self._change(s, 7)
        chg_1m = self._change(s, 30)
        chg_3m = self._change(s, 90)
        chg_1y = self._change(s, 365)

        pct_rank    = self._percentile_rank(s)
        trend_label = self._trend_label(s, pct_rank)
        context     = self._context_text(meta, latest_value, chg_1m, chg_1y, pct_rank, s)

        return IndicatorResult(
            key=meta.key, name_ko=meta.name_ko, name_en=meta.name_en,
            description=meta.description, direction=meta.direction,
            source=meta.source, category=meta.category, unit=meta.unit,
            latest_value=latest_value, latest_date=latest_date,
            chg_1w=chg_1w, chg_1m=chg_1m, chg_3m=chg_3m, chg_1y=chg_1y,
            pct_rank=pct_rank, trend_label=trend_label, context=context,
            series=s,
        )

    # ── 내부 계산 유틸 ───────────────────────────────────────────────

    @staticmethod
    def _change(s: pd.Series, days: int) -> Optional[float]:
        """n일 전 값 대비 절대 변화량."""
        if s.empty:
            return None
        target = s.index[-1] - timedelta(days=days)
        past   = s[s.index <= target]
        if past.empty:
            return None
        return float(s.iloc[-1] - past.iloc[-1])

    @staticmethod
    def _percentile_rank(s: pd.Series) -> Optional[float]:
        """현재값의 전체 기간 내 퍼센타일 순위 (0=최저, 100=최고)."""
        if len(s) < 10:
            return None
        rank = float(np.sum(s.values <= s.iloc[-1])) / len(s) * 100
        return round(rank, 1)

    @staticmethod
    def _trend_label(s: pd.Series, pct_rank: Optional[float]) -> str:
        """퍼센타일 기반 간단 위치 라벨."""
        if pct_rank is None:
            return ""
        if pct_rank >= 90:
            return "5년 최고 수준"
        if pct_rank >= 75:
            return "5년 상위 25%"
        if pct_rank >= 50:
            return "5년 중상위"
        if pct_rank >= 25:
            return "5년 중하위"
        if pct_rank >= 10:
            return "5년 하위 25%"
        return "5년 최저 수준"

    @staticmethod
    def _fmt(val: float, unit: str) -> str:
        """단위에 맞는 숫자 포매팅."""
        if np.isnan(val):
            return "N/A"
        if unit == "$B":
            return f"${val:,.0f}B"
        if unit == "%":
            return f"{val:.2f}%"
        if unit == "bps":
            return f"{val:.1f}bps"
        return f"{val:.2f}"

    @classmethod
    def _context_text(
        cls,
        meta,
        latest: float,
        chg_1m: Optional[float],
        chg_1y: Optional[float],
        pct_rank: Optional[float],
        s: pd.Series,
    ) -> str:
        """
        현재 수준을 사실 기반으로 설명하는 한 문장을 생성합니다.
        (점수·판정 없음, 변화 사실만 기재)
        """
        parts: list[str] = []

        # ① 퍼센타일 위치
        if pct_rank is not None:
            parts.append(f"5년 기간 중 {pct_rank:.0f}% 수준")

        # ② 1개월 변화
        if chg_1m is not None and not np.isnan(chg_1m):
            sign  = "+" if chg_1m >= 0 else ""
            val_s = cls._fmt(abs(chg_1m), meta.unit)
            dir_s = "증가" if chg_1m >= 0 else "감소"
            parts.append(f"1개월 {dir_s} {val_s}")

        # ③ 1년 변화
        if chg_1y is not None and not np.isnan(chg_1y):
            val_s = cls._fmt(abs(chg_1y), meta.unit)
            dir_s = "증가" if chg_1y >= 0 else "감소"
            parts.append(f"1년 {dir_s} {val_s}")

        return " | ".join(parts) if parts else "데이터 부족"
