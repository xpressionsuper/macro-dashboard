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
import math
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import plotly.graph_objects as go
import plotly.io as pio

from dashboard.chart_builder import SLOT_TITLES
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
       font-size: 17px; line-height: 1.6; }

/* ── 레이아웃 ── */
.layout { display: flex; min-height: 100vh; }
.sidebar { width: 220px; min-width: 220px; background: var(--surface);
           border-right: 1px solid var(--border);
           padding: 20px 0; position: sticky; top: 0; height: 100vh;
           overflow-y: auto; }
.main { flex: 1; padding: 24px; overflow-x: hidden; }

/* ── 사이드바 ── */
.sidebar-title { padding: 0 16px 16px; font-size: 18px; font-weight: 700;
                 color: var(--accent); border-bottom: 1px solid var(--border);
                 margin-bottom: 8px; }
.nav-item { display: block; padding: 10px 16px; cursor: pointer;
            color: var(--muted); font-size: 16px; border-radius: 0;
            transition: all .2s; border-left: 3px solid transparent; }
.nav-item:hover { color: var(--text); background: rgba(255,255,255,.03); }
.nav-item.active { color: var(--accent); border-left-color: var(--accent);
                   background: rgba(0,212,255,.06); }
.nav-group { padding: 14px 16px 4px; font-size: 12px; font-weight: 700;
             color: var(--muted); text-transform: uppercase; letter-spacing: .04em; }
.nav-group:first-child { padding-top: 0; }

/* ── 헤더 ── */
.page-header { margin-bottom: 28px; }
.page-title { font-size: 26px; font-weight: 700; color: var(--accent); }
.page-subtitle { color: var(--muted); font-size: 16px; margin-top: 4px; }

/* ── 카테고리 섹션 ── */
.category-section { margin-bottom: 48px; scroll-margin-top: 20px; }
.category-header { display: flex; align-items: baseline; gap: 12px;
                   margin-bottom: 6px; }
.category-name { font-size: 20px; font-weight: 700; color: var(--text); }
.category-desc { font-size: 15px; color: var(--muted); }

/* ── 요약 테이블 ── */
.summary-table { width: 100%; border-collapse: collapse;
                 margin: 14px 0 20px; font-size: 16px; }
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
.desc-cell { color: var(--muted); font-size: 15px; max-width: 260px;
             white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.ctx-cell  { font-size: 15px; color: var(--muted); max-width: 320px; }

/* ── 지표 행 (설명 → 차트 → 증가/감소) ── */
.indicator-rows { display: flex; flex-direction: column; gap: 12px; margin: 14px 0 24px; }
.indicator-row { display: flex; align-items: stretch; gap: 14px; flex-wrap: wrap;
                 background: var(--surface); border: 1px solid var(--border);
                 border-radius: 8px; padding: 12px 16px; }
.indicator-row.newest { border-color: var(--accent);
                         box-shadow: 0 0 0 1px var(--accent), 0 0 16px rgba(0,212,255,.25); }
.newest-badge { display: inline-block; font-size: 10px; font-weight: 700;
                color: var(--bg); background: var(--accent);
                padding: 1px 6px; border-radius: 999px; margin-left: 4px;
                vertical-align: middle; }
.row-date.newest { color: var(--accent); font-weight: 600; }
.row-desc  { flex: 1 1 240px; min-width: 200px; }
.row-head  { display: flex; align-items: baseline; gap: 8px; flex-wrap: wrap;
             margin-bottom: 6px; }
.row-name  { font-size: 17px; font-weight: 700; color: var(--accent); }
.row-en    { font-size: 12px; color: var(--muted); }
.row-val   { font-family: 'JetBrains Mono', monospace; font-weight: 700; font-size: 15px; }
.row-date  { font-size: 12px; color: var(--muted); }
.row-chg   { font-family: 'JetBrains Mono', monospace; font-weight: 700; font-size: 13px;
             padding: 2px 7px; border-radius: 999px; }
.row-chg.up   { color: var(--up);   background: rgba(63,185,80,.12); }
.row-chg.down { color: var(--down); background: rgba(248,81,73,.12); }
.row-text  { font-size: 15px; color: var(--text); line-height: 1.55; }
.copy-btn  { margin-left: auto; font-size: 12px; color: var(--muted); background: transparent;
             border: 1px solid var(--border); border-radius: 6px; padding: 3px 9px; cursor: pointer; }
.copy-btn:hover { color: var(--accent); border-color: var(--accent); }
.copy-btn.done  { color: var(--up); border-color: var(--up); }
.copy-btn.fail  { color: var(--down); border-color: var(--down); }
#img-overlay { display: none; position: fixed; inset: 0; z-index: 100; background: var(--bg);
               flex-direction: column; align-items: center; justify-content: center; gap: 14px; padding: 16px; }
#img-overlay.open { display: flex; }
#img-overlay img { max-width: 100%; max-height: 70vh; border-radius: 6px;
                   -webkit-touch-callout: default; user-select: auto; }
.ov-hint  { color: #fff; font-size: 15px; font-weight: 600; }
.ov-close { color: var(--muted); font-size: 14px; padding: 8px 16px; border: 1px solid var(--border);
            border-radius: 999px; }
.row-chart { flex: 0 1 580px; min-width: 0; }
.row-chart .chart-wrap { background: transparent; border: none; padding: 0; }

/* ── 모바일: 사이드바를 상단 가로 메뉴로, 여백 축소 ── */
@media (max-width: 760px) {
  .layout { display: block; }
  .sidebar { position: sticky; top: 0; z-index: 10; width: auto; min-width: 0; height: auto;
             padding: 0; display: flex; overflow-x: auto; white-space: nowrap;
             border-right: none; border-bottom: 1px solid var(--border); }
  .sidebar-title, .nav-group { display: none; }
  .nav-item { padding: 10px 12px; border-left: none; border-bottom: 3px solid transparent; }
  .nav-item.active { border-bottom-color: var(--accent); }
  .main { padding: 12px; }
  .indicator-row { padding: 10px; }
  .row-desc, .row-updown { min-width: 0; }
  .category-section { scroll-margin-top: 50px; }
}
.row-updown { flex: 1 1 240px; min-width: 220px; display: flex; flex-direction: column; gap: 8px; }
.detail-row { font-size: 16px; font-weight: 600; line-height: 1.5;
              padding: 10px 12px; border-radius: 6px; }
.detail-row.up   { background: rgba(63,185,80,.1);  border: 1px solid var(--up); }
.detail-row.down { background: rgba(248,81,73,.1); border: 1px solid var(--down); }
.detail-row b { display: block; margin-bottom: 4px; font-size: 15px; }
.detail-row.up b   { color: var(--up); }
.detail-row.down b { color: var(--down); }
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

  // 지표별 복사 버튼 — 차트를 PNG 이미지로.
  // PC: 클립보드에 바로 넣음 / 휴대폰: 모바일 브라우저는 이미지 클립보드 쓰기가 불안정해서
  //     이미지를 화면에 띄우고, 길게 눌러 브라우저 기본 메뉴('이미지 복사'·'저장')로 복사하게 함
  var isPhone = window.matchMedia('(pointer: coarse)').matches;
  var chartOf = function (btn) { return btn.closest('.indicator-row').querySelector('.js-plotly-plot'); };
  var overlay = document.getElementById('img-overlay');
  overlay.addEventListener('click', function (e) {
    if (e.target !== overlay.querySelector('img')) overlay.classList.remove('open');
  });

  document.addEventListener('click', function (e) {
    var btn = e.target.closest('.copy-btn');
    if (!btn) return;
    var flash = function (label, cls) {
      btn.textContent = label; btn.classList.add(cls);
      setTimeout(function () { btn.textContent = '📋 Copy'; btn.classList.remove(cls); }, 1500);
    };
    var chart = chartOf(btn);
    if (!chart) return flash('Not supported', 'fail');

    if (isPhone) {
      return Plotly.toImage(chart, { format: 'png', scale: 2 }).then(function (url) {
        overlay.querySelector('img').src = url;
        overlay.classList.add('open');
      }, function () { flash('Failed', 'fail'); });
    }

    if (!window.ClipboardItem || !navigator.clipboard || !navigator.clipboard.write) {
      return flash('Not supported', 'fail');
    }
    // Safari는 클릭 직후 동기적으로 write를 불러야 하므로 이미지는 Promise로 넘김
    var png = Plotly.toImage(chart, { format: 'png', scale: 2 })
      .then(function (url) { return fetch(url); })
      .then(function (r) { return r.blob(); });
    navigator.clipboard.write([new ClipboardItem({ 'image/png': png })]).then(
      function () { flash('Copied ✓', 'done'); },
      function () { flash('Failed', 'fail'); }
    );
  });
})();
</script>
"""


class HtmlExporter:
    """Plotly Figure + IndicatorResult → 단일 HTML 파일 생성."""

    def __init__(
        self,
        figures:      dict[int, list[tuple[go.Figure, list[IndicatorResult]]]],
        results:      list[IndicatorResult],
        timestamp:    str,
        category_meta: dict[int, tuple[str, str, list]],
        page_title:   str = "🏦 미국 달러 유동성 대시보드",
        sidebar_title: str = "📊 매크로 모니터",
        data_sources: str = "FRED · TreasuryDirect · NY Fed",
        sidebar_groups: Optional[list[tuple[str, list[int]]]] = None,
        top_panel:    str = "",          # 헤더 바로 아래, 모든 카테고리 위에 넣을 HTML (id 필요)
        top_nav:      Optional[tuple[str, str]] = None,  # (사이드바 표시명, 패널 id)
    ):
        self._figures       = figures
        self._results       = {r.key: r for r in results}
        self._timestamp     = timestamp
        self._category_meta = category_meta
        self._page_title    = page_title
        self._sidebar_title = sidebar_title
        self._data_sources  = data_sources
        self._sidebar_groups = sidebar_groups   # [(그룹명, [카테고리 번호...]), ...]
        self._top_panel     = top_panel
        self._top_nav       = top_nav

        # "최신" 배지는 딱 하나의 최고값이 아니라, 오늘로부터 1주일 이내에
        # 발표된 지표는 전부 하이라이트합니다. (기준일이 아니라 발표일 기준)
        self._newest_cutoff = datetime.now() - timedelta(days=7)

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
  <title>{self._page_title} — {self._timestamp}</title>
  <style>{_CSS}</style>
</head>
<body>
  <div class="layout">
    {sidebar}
    <main class="main">
      {main}
    </main>
  </div>
  <div id="img-overlay">
    <div class="ov-hint">이미지를 길게 눌러 복사·저장하세요</div>
    <img alt="차트 이미지">
    <div class="ov-close">✕ 닫기</div>
  </div>
  {_JS_NAV}
</body>
</html>"""

    # ── 사이드바 ──────────────────────────────────────────────────────

    def _build_sidebar(self) -> str:
        if self._sidebar_groups:
            items = "\n".join(self._build_sidebar_group(label, cats) for label, cats in self._sidebar_groups)
        else:
            items = "\n".join(
                f'<div class="nav-item" data-target="cat-{cat}">{name}</div>'
                for cat, (name, _, _) in self._category_meta.items()
            )
        if self._top_nav:
            label, target = self._top_nav
            items = f'<div class="nav-item" data-target="{target}">{label}</div>' + items
        return f"""
<aside class="sidebar">
  <div class="sidebar-title">{self._sidebar_title}</div>
  {items}
</aside>"""

    def _build_sidebar_group(self, label: str, cats: list[int]) -> str:
        rows = "\n".join(
            f'<div class="nav-item" data-target="cat-{cat}">{self._category_meta[cat][0]}</div>'
            for cat in cats if cat in self._category_meta
        )
        return f'<div class="nav-group">{label}</div>{rows}'

    # ── 메인 콘텐츠 ──────────────────────────────────────────────────

    def _build_main(self) -> str:
        header = f"""
<div class="page-header">
  <div class="page-title">{self._page_title}</div>
  <div class="page-subtitle">생성: {self._timestamp} &nbsp;|&nbsp;
    데이터: {self._data_sources}</div>
</div>"""

        sections = "\n".join(
            self._build_category_section(cat)
            for cat in sorted(self._category_meta.keys())
        )

        return header + self._top_panel + sections

    # ── 카테고리 섹션 ─────────────────────────────────────────────────

    def _build_category_section(self, cat: int) -> str:
        name, desc, _indicators = self._category_meta[cat]

        rows = self._build_indicator_rows(cat)

        return f"""
<section class="category-section" id="cat-{cat}">
  <div class="category-header">
    <span class="category-name">{name}</span>
    <span class="category-desc">{desc}</span>
  </div>
  {rows}
</section>"""

    # ── 지표 행 (설명 → 차트 → 증가/감소) ────────────────────────────

    def _build_indicator_rows(self, cat: int) -> str:
        slots = self._figures.get(cat)
        if not slots:
            return "<p style='color:var(--muted)'>데이터 없음</p>"

        rows = []
        for i, (fig, slot_results) in enumerate(slots):
            include_js = "cdn" if (cat == 1 and i == 0) else False
            chart_div = pio.to_html(
                fig, full_html=False, include_plotlyjs=include_js,
                config={"displayModeBar": False, "responsive": True, "scrollZoom": False},
            )
            primary = self._resolve_row_meta(slot_results)
            is_newest = self._is_within_a_week(primary.released_date)
            row_cls = "indicator-row newest" if is_newest else "indicator-row"
            rows.append(f"""
<div class="{row_cls}">
  {self._build_row_desc(primary, is_newest=is_newest)}
  <div class="row-chart"><div class="chart-wrap">{chart_div}</div></div>
  {self._build_row_updown(primary)}
</div>""")
        return f'<div class="indicator-rows">{"".join(rows)}</div>'

    def _is_within_a_week(self, released_date: str) -> bool:
        """발표일이 오늘로부터 1주일 이내인지 확인합니다."""
        if not released_date or released_date == "N/A":
            return False
        try:
            d = datetime.strptime(released_date, "%Y-%m-%d")
        except ValueError:
            return False
        return d >= self._newest_cutoff

    @staticmethod
    def _resolve_row_meta(slot_results: list[IndicatorResult]) -> IndicatorResult:
        """
        여러 지표를 겹쳐 그린 슬롯의 설명 카드는 기본적으로 첫 번째 지표
        설명을 쓰지만, 넷 유동성 구성요소(BS·RRP·TGA)처럼 조합 자체가
        의미 있는 경우엔 조합에 맞는 설명으로 바꿔 보여줍니다.
        """
        keys = {r.key for r in slot_results}
        if keys == {"BS", "RRP", "TGA"}:
            bs = next(r for r in slot_results if r.key == "BS")
            return replace(
                bs,
                name_ko="Fed 대차대조표 & 역레포 잔고 & TGA 잔고",
                name_en="Balance Sheet & RRP & TGA",
                description=(
                    "넷 유동성(BS−RRP−TGA)을 이루는 세 구성요소를 한 차트에 겹쳐서, "
                    "어느 쪽의 움직임이 넷 유동성 증감을 주도하고 있는지 한눈에 비교합니다. "
                    "Fed 대차대조표(파랑)는 연준이 직접 조절하는 유동성 총량, 역레포(RRP)는 시중에서 "
                    "빠져나가 연준에 잠긴 자금, TGA는 재무부 계좌에 묶인 자금입니다."
                ),
                when_up=(
                    "대차대조표는 늘고 RRP·TGA는 줄어드는 조합일 때 넷 유동성이 가장 크게 증가합니다. "
                    "즉 연준이 돈을 풀면서(QE) 동시에 시중에 잠겨있던 RRP·TGA 자금까지 풀려나오는 구간입니다."
                ),
                when_down=(
                    "대차대조표는 줄고(QT) RRP·TGA는 늘어나는 조합일 때 넷 유동성이 가장 크게 감소합니다. "
                    "특히 부채한도 협상 타결 직후 TGA가 급격히 채워지는 구간에서 이런 패턴이 잘 나타납니다."
                ),
            )
        # 한국 수급·크레딧 스프레드 묶음 차트: 첫 지표 설명만 보이면 나머지 설명이 사라지므로 전부 나열
        head = slot_results[0]
        if len(slot_results) > 1 and (head.key.startswith("KR_") or head.key == "HY_OAS"):
            described = [r for r in slot_results if r.description]
            text = described[0].description if len(described) == 1 else "<br><br>".join(
                f"<b>{r.name_ko.removesuffix(' 누적 순매수')}</b> — {r.description}" for r in described
            )
            return replace(head, name_ko=SLOT_TITLES.get(head.key, head.name_ko), description=text)
        return head

    @staticmethod
    def _fmt_value(r: IndicatorResult) -> str:
        return (
            f"${r.latest_value:,.1f}B" if r.unit == "$B"
            else f"{r.latest_value:,.2f} {r.unit}".strip()
        )

    @staticmethod
    def _fmt_chg(r: IndicatorResult) -> str:
        """대표 증감 문자열 ("전월대비 +1.23%"), 없으면 빈 문자열."""
        if r.chg_disp is None or math.isnan(r.chg_disp):
            return ""
        sign = "+" if r.chg_disp >= 0 else ""
        suffix = "%" if r.chg_disp_is_pct else (f" {r.unit}" if r.unit else "")
        return f"{r.chg_disp_label} {sign}{r.chg_disp:.2f}{suffix}"

    @classmethod
    def _build_row_desc(cls, r: IndicatorResult, is_newest: bool = False) -> str:
        latest_str = cls._fmt_value(r)
        # 월간 통계(국제수지)는 날짜가 매달 1일로 붙어 '8월 1일 하루치'로 오해되므로 월로 표시
        date_str = r.latest_date
        if r.key.startswith("KR_BOP_") and r.latest_date[:4].isdigit():
            date_str = f"{r.latest_date[:4]}년 {int(r.latest_date[5:7])}월"

        chg_badge = ""
        chg = cls._fmt_chg(r)
        if chg:
            chg_cls = "up" if r.chg_disp >= 0 else "down"
            chg_badge = f'<span class="row-chg {chg_cls}">{chg}</span>'
        copy_btn = '<button class="copy-btn" type="button">📋 Copy</button>'

        date_cls = "row-date newest" if is_newest else "row-date"
        newest_badge = ' <span class="newest-badge">최신</span>' if is_newest else ""
        if r.released_date and r.released_date != r.latest_date:
            released_str = f'<span class="{date_cls}">발표일: {r.released_date}{newest_badge}</span>'
        elif is_newest:
            released_str = f'<span class="{date_cls}">{newest_badge}</span>'
        else:
            released_str = ""

        return f"""
<div class="row-desc">
  <div class="row-head">
    <span class="row-name">{r.name_ko}</span>
    <span class="row-en">{r.name_en}</span>
    {copy_btn}
  </div>
  <div class="row-head">
    <span class="row-val">{latest_str}</span>
    <span class="row-date">기준일: {date_str}</span>
    {released_str}
    {chg_badge}
  </div>
  <div class="row-text">{r.description}</div>
</div>"""

    @staticmethod
    def _build_row_updown(r: IndicatorResult) -> str:
        up_row = (
            f'<div class="detail-row up"><b>↑ 증가할 때</b>{r.when_up}</div>'
            if r.when_up else ""
        )
        down_row = (
            f'<div class="detail-row down"><b>↓ 감소할 때</b>{r.when_down}</div>'
            if r.when_down else ""
        )
        return f'<div class="row-updown">{up_row}{down_row}</div>'
