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

import numpy as np
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
    "BS":       ["UST", "MBS"],   # ① BS 차트에 UST, MBS 함께
    "FEDFUNDS": ["DGS3MO"],       # ④ 기준금리 + 3개월물 국채 수익률 함께
    "SOFR":     ["IORB"],          # ④ SOFR 차트에 IORB 함께
    "DGS10":    ["DGS2"],          # ④ 10Y + 2Y 함께
    "NET_LIQ":  [],                # ③ 별도 처리
    # 한국 ①: 외국인·기관·개인·기타법인 한 차트 / ②: 기관 세부 7개 한 차트
    "KR_FOREIGN_NET": ["KR_INST_NET", "KR_INDIV_NET", "KR_CORP_NET"],
    "KR_PENSION_NET": ["KR_SECURITIES_NET", "KR_TRUST_NET", "KR_PE_NET",
                       "KR_INSURANCE_NET", "KR_BANK_NET", "KR_OTHER_FIN_NET"],
    # 한국 외국인 증권투자자금: 합계·주식·채권 한 차트
    "KR_BOP_TOTAL_NET": ["KR_BOP_EQUITY_NET", "KR_BOP_DEBT_NET"],
    # 한국 부동산: 서울 8개 구 한 차트 / 경기 8개 지역 한 차트
    "KR_APT_GANGNAM": ["KR_APT_SONGPA", "KR_APT_YONGSAN", "KR_APT_MAPO", "KR_APT_SEONGDONG",
                       "KR_APT_GANGDONG", "KR_APT_NOWON", "KR_APT_DOBONG"],
    "KR_APT_BUNDANG": ["KR_APT_GWACHEON", "KR_APT_HANAM", "KR_APT_GWANGMYEONG", "KR_APT_YEONGTONG",
                       "KR_APT_SUJI", "KR_APT_DONGTAN", "KR_APT_ILSANDONG"],
}
# 지표가 많아 이름을 이어 붙이면 너무 긴 차트의 제목
SLOT_TITLES: dict[str, str] = {
    "KR_FOREIGN_NET": "외국인 누적 순매수",
    "KR_PENSION_NET": "기관 세부 수급",
    "KR_BOP_TOTAL_NET": "외국인 증권투자자금 유출입 (누적, 달러)",
    "KR_APT_GANGNAM": "서울 아파트 가격 (84㎡ 환산)",
    "KR_APT_BUNDANG": "경기 아파트 가격 (84㎡ 환산)",
}
# 단독으로 그리지 않을 (합친 차트에 종속될) 지표
SKIP_SOLO: set[str] = {"UST", "MBS", "DGS3MO", "IORB", "DGS2"}.union(
    *(COMBINED_CHARTS[k] for k in ("KR_FOREIGN_NET", "KR_PENSION_NET", "KR_APT_GANGNAM",
                                   "KR_APT_BUNDANG", "KR_BOP_TOTAL_NET"))
)

# y축 눈금 상한 고정 (0 ~ 지정값) — 없는 지표는 자동 스케일 유지
Y_MAX_OVERRIDE: dict[str, float] = {
    "ICSA_R":  1000,    # 신규 실업수당 청구건수 (천 건)
    "CCSA":    5000,    # 지속 실업수당 청구건수 (천 명)
}


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

    def build_all(self) -> dict[int, list[tuple[go.Figure, list[IndicatorResult]]]]:
        """
        카테고리 번호 → [(Figure, 슬롯에 포함된 지표들), ...] 리스트.
        슬롯에 포함된 지표 목록은 설명 박스를 만드는 데 사용됩니다.
        """
        categories: dict[int, list[IndicatorResult]] = {}
        for r in self._results.values():
            categories.setdefault(r.category, []).append(r)

        return {
            cat: self._build_category_figures(cat_results)
            for cat, cat_results in sorted(categories.items())
        }

    # ── 카테고리 Figure 조립 ─────────────────────────────────────────

    def _build_category_figures(
        self, results: list[IndicatorResult]
    ) -> list[tuple[go.Figure, list[IndicatorResult]]]:
        """
        한 카테고리 내 지표들을 슬롯으로 묶고, 슬롯마다 정사각형에 가까운
        (3:4) 개별 Figure를 만들어 (Figure, 슬롯 지표 목록) 리스트로 반환합니다.
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

        self._color_idx = 0
        figures = []
        for slot in chart_slots:
            fig = make_subplots(rows=1, cols=1, specs=[[{"secondary_y": True}]])
            self._add_slot_traces(fig, slot, 1)
            self._add_benchmark_overlays(fig, 1, self._benchmarks_for(slot[0].key))
            title = SLOT_TITLES.get(slot[0].key) or " & ".join(r.name_ko for r in slot)
            self._apply_layout(fig, height=330, title=title)
            if len(slot) > 4:   # 범례가 여러 줄이면 제목과 겹치므로 차트 아래로
                fig.update_layout(height=380, legend=dict(yanchor="top", y=-0.12),
                                  margin=dict(t=40, b=90))
            figures.append((fig, slot))

        return figures

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

            fig.add_trace(
                go.Scatter(
                    x=series.index,
                    y=series.values,
                    name=r.name_ko,
                    mode="lines",
                    line=dict(color=color, width=1.8),
                    fill="none",
                    hovertemplate=(
                        f"<b>{r.name_ko}</b><br>"
                        f"%{{x|{'%Y년 %m월' if r.key.startswith('KR_BOP_') else '%Y-%m-%d'}}}<br>"
                        f"%{{y:.2f}} {unit}<extra></extra>"
                    ),
                ),
                row=row, col=1,
            )

        # y축 단위 라벨 (슬롯의 첫 번째 지표 기준)
        if slot:
            y_max = Y_MAX_OVERRIDE.get(slot[0].key)
            fig.update_yaxes(
                title_text=slot[0].unit,
                row=row, col=1, secondary_y=False,
                title_font=dict(size=10),
                gridcolor=GRID_COLOR,
                range=[0, y_max] if y_max is not None else None,
            )
            fig.update_xaxes(
                row=row, col=1,
                gridcolor=GRID_COLOR,
                showgrid=True,
            )

            # T10Y2Y: 0선 강조
            if slot[0].key == "T10Y2Y":
                fig.add_hline(
                    y=0, line_dash="dot", line_color="#ff6b6b", line_width=1,
                    row=row, col=1,
                )
            # NFCI·누적 순매수: 0선
            if slot[0].key == "NFCI" or (slot[0].key.startswith("KR_") and slot[0].key.endswith("_NET")):
                fig.add_hline(
                    y=0, line_dash="dot", line_color="#ffd166", line_width=1,
                    row=row, col=1,
                )
            # 단기채 비중: 재무부가 통상 관리하는 15~20% 구간 음영
            if slot[0].key == "DEBT_SHORT_SHARE":
                fig.add_hrect(
                    y0=15, y1=20, fillcolor="#06d6a0", opacity=0.12, line_width=0,
                    row=row, col=1,
                )

    # ── 시장 벤치마크(S&P500·금 / 한국은 KOSPI) 오버레이 ──────────────

    _BENCHMARKS = [
        ("SPX",  "S&P500", "#f4a261", "dot"),
        ("GOLD", "금",     "#ffd700", "dash"),
    ]
    _BENCHMARKS_KR = [
        ("KOSPI", "KOSPI", "#e6edf3", "dot"),
    ]

    @classmethod
    def _benchmarks_for(cls, key: str) -> list:
        """미국 지표엔 S&P500·금, 한국 수급엔 KOSPI, 한국 부동산엔 겹치지 않음."""
        if key.startswith("KR_APT_"):
            return []
        return cls._BENCHMARKS_KR if key.startswith("KR_") else cls._BENCHMARKS

    def _add_benchmark_overlays(self, fig: go.Figure, row: int, benchmarks: list) -> None:
        """차트 오른쪽 보조축에 벤치마크 가격을 겹쳐 그립니다."""
        for key, label, color, dash in benchmarks:
            series = self._raw.get(key, pd.Series(dtype=float))
            if series.empty:
                continue
            fig.add_trace(
                go.Scatter(
                    x=series.index,
                    y=series.values,
                    name=label,
                    mode="lines",
                    line=dict(color=color, width=1.2, dash=dash),
                    opacity=0.6,
                    hovertemplate=f"<b>{label}</b><br>%{{x|%Y-%m-%d}}<br>%{{y:,.0f}}<extra></extra>",
                ),
                row=row, col=1, secondary_y=True,
            )
        fig.update_yaxes(
            secondary_y=True, row=row, col=1,
            showgrid=False, title_text="", tickfont=dict(size=9),
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
    def _apply_layout(fig: go.Figure, height: int, title: str = "") -> None:
        # 가로폭은 컨테이너에 맞춤(모바일), 터치 드래그로 확대·이동 금지 → 페이지 스크롤 우선
        fig.update_xaxes(fixedrange=True)
        fig.update_yaxes(fixedrange=True)
        fig.update_layout(
            dragmode=False,
            height=height,
            template=THEME,
            title=dict(text=title, font=dict(size=14), x=0.05, y=0.97),
            paper_bgcolor=PAPER_COLOR,
            plot_bgcolor=BG_COLOR,
            font=dict(family=FONT_FAMILY, color=FONT_COLOR, size=12),
            legend=dict(
                orientation="h",
                yanchor="bottom", y=1.0,
                xanchor="left",  x=0,
                font=dict(size=11),
            ),
            margin=dict(l=50, r=20, t=60, b=35),
            hovermode="x unified",
        )
