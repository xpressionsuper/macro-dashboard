"""
dashboard/html_exporter.py ─ HTML 대시보드 조립 & 저장
=====================================================================
카테고리별 Plotly Figure와 IndicatorResult를 조합하여
완전한 단일 HTML 파일을 생성합니다.

구조:
  - 헤더 & 생성 일시
  - 사이드 탭 내비게이션 (카테고리①~⑤)
  - 각 카테고리: 요약 테이블 + 인터랙티브 차트
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import plotly.graph_objects as go
import plotly.io as pio

from config import CATEGORY_META
from indicators.base import IndicatorResult


# ── HTML 템플릿 스타일 ────────────────────────────────────────────────
_CSS = """
:root {
  --bg:       #0d1117;
  --surface:  #161b22;
  --border:   #21262d;
  --accent:   #00d4ff;
  --text:     #c9d1d9;
  --muted:    #8b949e;
  --up:       #3fb950;
  --down:     #f85149;
  --warn:     #d29922;
  --font:     'Inter', 'Nanum Gothic', sans-serif;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: var(--bg); color: var(--text); font-family: var(--font);
       font-size: 13px; line-height: 1.6; }

/* ── 레이아웃 ── */
.layout { display: flex; min-height: 100vh; }
.sidebar { width: 220px; min-width: 220px; background: var(--surface);
           border-right: 1px solid var(--border);
           padding: 20px 0; position: sticky; top: 0; height: 100vh;
           overflow-y: auto; }
.main { flex: 1; padding: 24px; overflow-x: hidden; }

/* ── 사이드바 ── */
.sidebar-title { padding: 0 16px 16px; font-size: 14px; font-weight: 700;
                 color: var(--accent); border-bottom: 1px solid var(--border);
                 margin-bottom: 8px; }
.nav-item { display: block; padding: 10px 16px; cursor: pointer;
            color: var(--muted); font-size: 12px; border-radius: 0;
            transition: all .2s; border-left: 3px solid transparent; }
.nav-item:hover { color: var(--text); background: rgba(255,255,255,.03); }
.nav-item.active { color: var(--accent); border-left-color: var(--accent);
                   background: rgba(0,212,255,.06); }

/* ── 헤더 ── */
.page-header { margin-bottom: 28px; }
.page-title { font-size: 22px; font-weight: 700; color: var(--accent); }
.page-subtitle { color: var(--muted); font-size: 12px; margin-top: 4px; }

/* ── 카테고리 섹션 ── */
.category-section { margin-bottom: 48px; scroll-margin-top: 20px; }
.category-header { display: flex; align-items: baseline; gap: 12px;
                   margin-bottom: 6px; }
.category-name { font-size: 16px; font-weight: 700; color: var(--text); }
.category-desc { font-size: 11px; color: var(--muted); }

/* ── 요약 테이블 ── */
.summary-table { width: 100%; border-collapse: collapse;
                 margin: 14px 0 20px; font-size: 12px; }
.summary-table th { background: var(--surface); color: var(--muted);
                    font-weight: 600; text-align: left;
                    padding: 8px 12px; border-bottom: 1px solid var(--border); }
.summary-table td { padding: 8px 12px; border-bottom: 1px solid var(--border);
                    vertical-align: middle; }
.summary-table tr:hover td { background: rgba(255,255,255,.02); }
.val-cell  { font-family: 'JetBrains Mono', monospace; font-weight: 700;
             color: var(--accent); white-space: nowrap; }
.chg-pos   { color: var(--up);   font-weight: 600; }
.chg-neg   { color: var(--down); font-weight: 600; }
.chg-neu   { color: var(--muted); }
.pct-bar   { display: inline-block; width: 60px; height: 6px;
             background: var(--border); border-radius: 3px; vertical-align: middle;
             margin-right: 6px; }
.pct-fill  { height: 100%; border-radius: 3px; background: var(--accent); }
.desc-cell { color: var(--muted); font-size: 11px; max-width: 260px;
             white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.ctx-cell  { font-size: 11px; color: var(--muted); max-width: 320px; }

/* ── 차트 ── */
.chart-wrap { background: var(--surface); border: 1px solid var(--border);
              border-radius: 8px; padding: 4px; margin-bottom: 8px; }
"""

_JS_NAV = """
<script>
(function () {
  var sections = document.querySelectorAll('.category-section');
  var navItems = document.querySelectorAll('.nav-item');

  navItems.forEach(function (item) {
    item.addEventListener('click', function () {
      var target = document.getElementById(item.dataset.target);
      if (target) target.scrollIntoView({ behavior: 'smooth' });
    });
  });

  var observer = new IntersectionObserver(function (entries) {
    entries.forEach(function (e) {
      if (e.isIntersecting) {
        navItems.forEach(function (n) {
          n.classList.toggle('active', n.dataset.target === e.target.id);
        });
      }
    });
  }, { threshold: 0.2 });

  sections.forEach(function (s) { observer.observe(s); });
})();
</script>
"""


class HtmlExporter:
    """Plotly Figure + IndicatorResult → 단일 HTML 파일 생성."""

    def __init__(
        self,
        figures:   dict[int, go.Figure],
        results:   list[IndicatorResult],
        timestamp: str,
    ):
        self._figures   = figures
        self._results   = {r.key: r for r in results}
        self._timestamp = timestamp

    # ── Public API ───────────────────────────────────────────────────

    def export(self, output_path: str) -> None:
        """HTML 파일을 생성합니다."""
        html = self._build_html()
        Path(output_path).write_text(html, encoding="utf-8")
        print(f"  ✅  HTML 대시보드 저장: {output_path}")

    # ── HTML 조립 ────────────────────────────────────────────────────

    def _build_html(self) -> str:
        sidebar  = self._build_sidebar()
        main     = self._build_main()

        return f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>매크로 유동성 모니터 — {self._timestamp}</title>
  <style>{_CSS}</style>
</head>
<body>
  <div class="layout">
    {sidebar}
    <main class="main">
      {main}
    </main>
  </div>
  {_JS_NAV}
</body>
</html>"""

    # ── 사이드바 ──────────────────────────────────────────────────────

    def _build_sidebar(self) -> str:
        items = "\n".join(
            f'<div class="nav-item" data-target="cat-{cat}">{name}</div>'
            for cat, (name, _, _) in CATEGORY_META.items()
        )
        return f"""
<aside class="sidebar">
  <div class="sidebar-title">📊 매크로 모니터</div>
  {items}
</aside>"""

    # ── 메인 콘텐츠 ──────────────────────────────────────────────────

    def _build_main(self) -> str:
        header = f"""
<div class="page-header">
  <div class="page-title">🏦 미국 달러 유동성 대시보드</div>
  <div class="page-subtitle">생성: {self._timestamp} &nbsp;|&nbsp;
    데이터: FRED · TreasuryDirect · NY Fed</div>
</div>"""

        sections = "\n".join(
            self._build_category_section(cat)
            for cat in sorted(CATEGORY_META.keys())
        )

        return header + sections

    # ── 카테고리 섹션 ─────────────────────────────────────────────────

    def _build_category_section(self, cat: int) -> str:
        name, desc, indicators = CATEGORY_META[cat]

        # 이 카테고리에 해당하는 결과만
        cat_results = [
            self._results[m.key]
            for m in indicators
            if m.key in self._results
        ]

        table  = self._build_summary_table(cat_results)
        charts = self._build_chart_html(cat)

        return f"""
<section class="category-section" id="cat-{cat}">
  <div class="category-header">
    <span class="category-name">{name}</span>
    <span class="category-desc">{desc}</span>
  </div>
  {table}
  {charts}
</section>"""

    # ── 요약 테이블 ──────────────────────────────────────────────────

    def _build_summary_table(self, results: list[IndicatorResult]) -> str:
        if not results:
            return "<p style='color:var(--muted)'>데이터 없음</p>"

        rows = "\n".join(self._table_row(r) for r in results)
        return f"""
<table class="summary-table">
  <thead>
    <tr>
      <th>지표명</th>
      <th>현재값</th>
      <th>1주</th>
      <th>1개월</th>
      <th>1년</th>
      <th>5년 위치</th>
      <th>상황 요약</th>
    </tr>
  </thead>
  <tbody>
    {rows}
  </tbody>
</table>"""

    def _table_row(self, r: IndicatorResult) -> str:
        import math

        def fmt_chg(val, unit):
            if val is None or math.isnan(val):
                return '<span class="chg-neu">—</span>'
            sign = "+" if val >= 0 else ""
            cls  = "chg-pos" if val >= 0 else "chg-neg"
            if unit == "$B":
                s = f"{sign}${val:,.0f}B"
            elif unit in ("%", "bps"):
                s = f"{sign}{val:.2f}{unit}"
            else:
                s = f"{sign}{val:.2f}"
            return f'<span class="{cls}">{s}</span>'

        latest_str = (
            f"${r.latest_value:,.1f}B" if r.unit == "$B"
            else f"{r.latest_value:.2f} {r.unit}".strip()
        )

        pct_bar = ""
        if r.pct_rank is not None:
            pct_bar = (
                f'<span class="pct-bar">'
                f'<span class="pct-fill" style="width:{r.pct_rank:.0f}%"></span>'
                f'</span>{r.pct_rank:.0f}%'
            )

        return f"""
<tr>
  <td>
    <div style="font-weight:600">{r.name_ko}</div>
    <div style="font-size:10px;color:var(--muted)">{r.name_en}</div>
  </td>
  <td class="val-cell">{latest_str}<br>
    <span style="font-size:10px;color:var(--muted)">{r.latest_date}</span></td>
  <td>{fmt_chg(r.chg_1w, r.unit)}</td>
  <td>{fmt_chg(r.chg_1m, r.unit)}</td>
  <td>{fmt_chg(r.chg_1y, r.unit)}</td>
  <td>{pct_bar}</td>
  <td class="ctx-cell" title="{r.context}">{r.context[:80]}{'…' if len(r.context) > 80 else ''}</td>
</tr>"""

    # ── 차트 HTML ────────────────────────────────────────────────────

    def _build_chart_html(self, cat: int) -> str:
        fig = self._figures.get(cat)
        if fig is None:
            return ""

        chart_div = pio.to_html(
            fig,
            full_html=False,
            include_plotlyjs="cdn",
            config={"responsive": True, "displayModeBar": True},
        )
        return f'<div class="chart-wrap">{chart_div}</div>'
