"""
dashboard/chart_builder.py ─ Plotly 차트 생성 엔진
=====================================================================
IndicatorResult 목록을 받아 카테고리별 Plotly Figure를 생성합니다.

차트 조합 규칙:
  ① BS + UST + MBS 를 하나의 차트에 겹쳐서 표시
  ③ NET_LIQ 차트에 BS/RRP/TGA 구성 요소도 함께 표시
  ④ SOFR + IORB 같은 차트 | DGS2 + DGS10 같은 차트 | T10Y2Y 별도
  나머지는 지표별 단독 차트
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from indicators.base import IndicatorResult

# ── 차트 스타일 ────────────────────────────────────────────────────────
THEME        = "plotly_dark"
COLORS       = ["#00d4ff", "#ff6b6b", "#ffd166", "#06d6a0", "#a8dadc",
                "#e9c46a", "#f4a261", "#e76f51", "#8ecae6", "#219ebc"]
BG_COLOR     = "#0d1117"
PAPER_COLOR  = "#161b22"
GRID_COLOR   = "#21262d"
FONT_COLOR   = "#c9d1d9"
FONT_FAMILY  = "Inter, 'Nanum Gothic', sans-serif"

# ── 같은 차트에 겹칠 지표 그룹 정의 ──────────────────────────────────────
# key: 대표 지표(y축 왼쪽), values: 같이 그릴 지표들
COMBINED_CHARTS: dict[str, list[str]] = {
    "BS":      ["UST", "MBS"],   # ① BS 차트에 UST, MBS 함께
    "SOFR":    ["IORB"],          # ④ SOFR 차트에 IORB 함께
    "DGS10":   ["DGS2"],          # ④ 10Y + 2Y 함께
    "NET_LIQ": [],                # ③ 별도 처리
}
# 단독으로 그리지 않을 (합친 차트에 종속될) 지표
SKIP_SOLO: set[str] = {"UST", "MBS", "IORB", "DGS2"}


class ChartBuilder:
    """IndicatorResult를 입력받아 카테고리별 Figure를 생성합니다."""

    def __init__(
        self,
        results: list[IndicatorResult],
        raw_all: dict[str, pd.Series],   # NET_LIQ 구성 요소 등 원시 시계열
    ):
        self._results   = {r.key: r for r in results}
        self._raw       = raw_all
        self._color_idx = 0

    # ── Public API ───────────────────────────────────────────────────

    def build_all(self) -> dict[int, go.Figure]:
        """
        카테고리 번호 → Figure 딕셔너리를 반환합니다.
        """
        categories: dict[int, list[IndicatorResult]] = {}
        for r in self._results.values():
            categories.setdefault(r.category, []).append(r)

        return {
            cat: self._build_category_figure(cat_results)
            for cat, cat_results in sorted(categories.items())
        }

    # ── 카테고리 Figure 조립 ─────────────────────────────────────────

    def _build_category_figure(
        self, results: list[IndicatorResult]
    ) -> go.Figure:
        """
        한 카테고리 내 지표들의 차트 배치를 결정하고 Figure를 생성합니다.
        """
        # 그릴 차트 단위 목록 구성
        chart_slots: list[list[IndicatorResult]] = []
        visited: set[str] = set()

        for r in results:
            if r.key in visited:
                continue

            if r.key in COMBINED_CHARTS:
                # 대표 + 종속 지표를 한 슬롯에
                companions = [
                    self._results[k]
                    for k in COMBINED_CHARTS[r.key]
                    if k in self._results
                ]
                chart_slots.append([r] + companions)
                visited.add(r.key)
                visited.update(COMBINED_CHARTS[r.key])

            elif r.key in SKIP_SOLO:
                # 다른 차트에 종속된 지표는 건너뜀
                visited.add(r.key)

            else:
                chart_slots.append([r])
                visited.add(r.key)

        # NET_LIQ 슬롯은 구성 요소(BS/RRP/TGA) 오버레이 처리
        chart_slots = self._inject_net_liq_components(chart_slots)

        n_rows = len(chart_slots)
        if n_rows == 0:
            return go.Figure()

        fig = make_subplots(
            rows=n_rows, cols=1,
            subplot_titles=[
                " & ".join(r.name_ko for r in slot)
                for slot in chart_slots
            ],
            vertical_spacing=0.08,
            shared_xaxes=False,
        )

        self._color_idx = 0

        for row_idx, slot in enumerate(chart_slots, start=1):
            self._add_slot_traces(fig, slot, row_idx)

        height = max(380 * n_rows, 400)
        self._apply_layout(fig, height)

        return fig

    # ── 슬롯별 트레이스 추가 ─────────────────────────────────────────

    def _add_slot_traces(
        self,
        fig: go.Figure,
        slot: list[IndicatorResult],
        row: int,
    ) -> None:
        """슬롯(1개 차트)에 속한 지표들의 트레이스를 추가합니다."""

        for i, r in enumerate(slot):
            if r.series.empty:
                continue

            color  = self._next_color()
            series = r.series
            unit   = r.unit

            # 채우기 영역 (첫 번째 지표에만)
            fill = "tozeroy" if i == 0 and len(slot) == 1 else "none"

            fig.add_trace(
                go.Scatter(
                    x=series.index,
                    y=series.values,
                    name=r.name_ko,
                    line=dict(color=color, width=1.8),
                    fill=fill,
                    fillcolor=color.replace(")", ", 0.08)").replace("rgb(", "rgba(")
                              if fill != "none" else "rgba(0,0,0,0)",
                    hovertemplate=(
                        f"<b>{r.name_ko}</b><br>"
                        f"%{{x|%Y-%m-%d}}<br>"
                        f"%{{y:.2f}} {unit}<extra></extra>"
                    ),
                ),
                row=row, col=1,
            )

        # y축 단위 라벨 (슬롯의 첫 번째 지표 기준)
        if slot:
            fig.update_yaxes(
                title_text=slot[0].unit,
                row=row, col=1,
                title_font=dict(size=10),
                gridcolor=GRID_COLOR,
            )
            fig.update_xaxes(
                row=row, col=1,
                gridcolor=GRID_COLOR,
                showgrid=True,
            )

            # T10Y2Y: 0선 강조
            if slot[0].key == "T10Y2Y":
                fig.add_hline(
                    y=0,
                    line_dash="dot",
                    line_color="#ff6b6b",
                    line_width=1,
                    row=row, col=1,
                )

            # NFCI: 0선
            if slot[0].key == "NFCI":
                fig.add_hline(
                    y=0,
                    line_dash="dot",
                    line_color="#ffd166",
                    line_width=1,
                    row=row, col=1,
                )

    # ── NET_LIQ 구성 요소 오버레이 주입 ─────────────────────────────

    def _inject_net_liq_components(
        self, slots: list[list[IndicatorResult]]
    ) -> list[list[IndicatorResult]]:
        """
        NET_LIQ 슬롯을 두 줄로 분리합니다:
          Row A: BS / RRP / TGA 구성 요소 오버레이
          Row B: 넷 유동성(NET_LIQ) 단독
        """
        new_slots = []
        for slot in slots:
            if len(slot) == 1 and slot[0].key == "NET_LIQ":
                # 구성 요소 더미 IndicatorResult 생성
                component_results = self._make_component_slot()
                if component_results:
                    new_slots.append(component_results)
                new_slots.append(slot)
            else:
                new_slots.append(slot)
        return new_slots

    def _make_component_slot(self) -> list[IndicatorResult]:
        """BS / RRP / TGA 를 같은 차트에 그릴 임시 슬롯을 만듭니다."""
        keys = ["BS", "RRP", "TGA"]
        results = []
        for k in keys:
            if k in self._results and not self._results[k].series.empty:
                results.append(self._results[k])
        return results

    # ── 색상 순환 ─────────────────────────────────────────────────────

    def _next_color(self) -> str:
        color = COLORS[self._color_idx % len(COLORS)]
        self._color_idx += 1
        return color

    # ── 레이아웃 공통 설정 ────────────────────────────────────────────

    @staticmethod
    def _apply_layout(fig: go.Figure, height: int) -> None:
        fig.update_layout(
            height=height,
            template=THEME,
            paper_bgcolor=PAPER_COLOR,
            plot_bgcolor=BG_COLOR,
            font=dict(family=FONT_FAMILY, color=FONT_COLOR, size=11),
            legend=dict(
                orientation="h",
                yanchor="bottom", y=1.02,
                xanchor="right",  x=1,
                font=dict(size=10),
            ),
            margin=dict(l=60, r=30, t=60, b=40),
            hovermode="x unified",
        )
        fig.update_annotations(font_size=11, font_color=FONT_COLOR)
