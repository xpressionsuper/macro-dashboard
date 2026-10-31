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
  --bg:        #0e0e0d;
  --surface:   #181817;
  --surface-2: #201f1e;
  --border:    #2a2a28;
  --border-2:  #383835;
  --accent:    #3fc6ec;
  --accent-bg: rgba(63,198,236,.10);
  --text:      #ecebe6;
  --text-2:    #c3c2b7;
  --muted:     #908e87;
  --up:        #4bbf6b;
  --down:      #ea5a52;
  --warn:      #e0a530;
  --font:      'Pretendard Variable', Pretendard, -apple-system, 'Apple SD Gothic Neo',
               'Malgun Gothic', sans-serif;
  color-scheme: dark;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
html { scroll-behavior: smooth; scrollbar-color: var(--border-2) var(--bg); }
body { background: var(--bg); color: var(--text); font-family: var(--font);
       font-size: 16px; line-height: 1.6; letter-spacing: -0.01em;
       -webkit-font-smoothing: antialiased; }
::selection { background: rgba(63,198,236,.28); color: var(--text); }
:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; border-radius: 4px; }

/* ── 레이아웃 ── */
.layout { display: flex; min-height: 100vh; }
.sidebar { width: 232px; min-width: 232px; background: var(--bg);
           border-right: 1px solid var(--border);
           padding: 24px 12px; position: sticky; top: 0; height: 100vh;
           overflow-y: auto; scrollbar-width: thin; }
.main { flex: 1; min-width: 0; max-width: 1480px; padding: 40px 40px 96px; }

/* ── 사이드바 ── */
.sidebar-title { display: flex; align-items: center; gap: 10px;
                 padding: 0 10px 20px; font-size: 15px; font-weight: 700;
                 color: var(--text); letter-spacing: -0.02em; }
.sidebar-title::before { content: ""; width: 8px; height: 8px; border-radius: 2px;
                         background: var(--accent); }
.nav-item { display: block; padding: 7px 10px; margin: 1px 0; border-radius: 6px;
            color: var(--muted); font-size: 14px; line-height: 1.45; text-decoration: none;
            transition: color .15s, background-color .15s; }
.nav-item:hover { color: var(--text); background: var(--surface); }
.nav-item.active { color: var(--text); background: var(--surface-2);
                   box-shadow: inset 2px 0 0 var(--accent); }
.nav-group { padding: 22px 10px 6px; font-size: 12px; font-weight: 600;
             color: var(--muted); letter-spacing: .02em; }

/* ── 헤더 ── */
.page-header { margin-bottom: 40px; padding-bottom: 24px; border-bottom: 1px solid var(--border); }
.page-title { font-size: 28px; font-weight: 700; color: var(--text);
              letter-spacing: -0.03em; line-height: 1.3; text-wrap: balance; }
.page-subtitle { display: flex; flex-wrap: wrap; gap: 4px 20px; margin-top: 10px;
                 color: var(--muted); font-size: 14px; }
.page-subtitle b { color: var(--text-2); font-weight: 500; font-variant-numeric: tabular-nums; }

/* ── 카테고리 섹션 ── */
.category-section { margin-bottom: 64px; scroll-margin-top: 24px; }
.category-header { margin-bottom: 18px; }
.category-name { display: block; font-size: 21px; font-weight: 700; color: var(--text);
                 letter-spacing: -0.025em; line-height: 1.35; }
.category-desc { display: block; margin-top: 2px; font-size: 14px; color: var(--muted); }

/* ── 지표 행 (설명 → 차트 → 증가/감소) ── */
.indicator-rows { display: flex; flex-direction: column; gap: 16px; }
.indicator-row { display: flex; align-items: stretch; gap: 24px; flex-wrap: wrap;
                 background: var(--surface); border: 1px solid var(--border);
                 border-radius: 12px; padding: 22px 24px; }
.indicator-row.newest { border-color: rgba(63,198,236,.45); }
.newest-badge { display: inline-block; font-size: 11px; font-weight: 600; line-height: 1.6;
                color: var(--accent); background: var(--accent-bg);
                padding: 0 7px; border-radius: 999px; margin-left: 2px; }
.row-date.newest { color: var(--text-2); }
.row-desc  { flex: 1 1 260px; min-width: 220px; }
.row-head  { display: flex; align-items: center; gap: 6px 10px; flex-wrap: wrap;
             margin-bottom: 8px; }
.row-name  { font-size: 17px; font-weight: 700; color: var(--text); letter-spacing: -0.02em; }
.row-en    { font-size: 12px; color: var(--muted); letter-spacing: 0; }
.row-val   { font-size: 22px; font-weight: 700; color: var(--text); letter-spacing: -0.02em;
             font-variant-numeric: tabular-nums; margin-right: 4px; }
.row-date  { font-size: 12px; color: var(--muted); font-variant-numeric: tabular-nums; }
.row-chg   { font-weight: 600; font-size: 12px; font-variant-numeric: tabular-nums;
             padding: 1px 8px; border-radius: 999px; }
.row-chg.up   { color: var(--up);   background: rgba(75,191,107,.12); }
.row-chg.down { color: var(--down); background: rgba(234,90,82,.12); }
.row-text  { margin-top: 14px; font-size: 14.5px; color: var(--text-2); line-height: 1.7;
             max-width: 62ch; }
.row-text b { color: var(--text); font-weight: 600; }
.copy-btn  { margin-left: auto; display: inline-flex; align-items: center; gap: 5px;
             font: inherit; font-size: 12px; color: var(--muted); background: transparent;
             border: 1px solid var(--border-2); border-radius: 6px; padding: 3px 9px; cursor: pointer;
             transition: color .15s, border-color .15s; }
.copy-btn svg { width: 13px; height: 13px; }
.copy-btn:hover { color: var(--text); border-color: var(--muted); }
.copy-btn.done  { color: var(--up); border-color: var(--up); }
.copy-btn.fail  { color: var(--down); border-color: var(--down); }
#img-overlay { display: none; position: fixed; inset: 0; z-index: 100; background: rgba(14,14,13,.96);
               flex-direction: column; align-items: center; justify-content: center; gap: 14px; padding: 16px; }
#img-overlay.open { display: flex; }
#img-overlay img { max-width: 100%; max-height: 70vh; border-radius: 8px;
                   -webkit-touch-callout: default; user-select: auto; }
.ov-hint  { color: var(--text); font-size: 15px; font-weight: 600; }
.ov-close { color: var(--text-2); font-size: 14px; padding: 8px 16px; border: 1px solid var(--border-2);
            border-radius: 999px; }
.row-chart { flex: 0 1 540px; min-width: 0; }
.row-chart .chart-wrap { background: transparent; border: none; padding: 0; }

.row-updown { flex: 1 1 220px; min-width: 200px; display: flex; flex-direction: column; gap: 10px; }
.detail-row { font-size: 14px; line-height: 1.65; color: var(--text-2);
              padding: 12px 14px; border-radius: 8px; background: var(--surface-2); }
.detail-row b { display: block; margin-bottom: 4px; font-size: 13px; font-weight: 600; }
.detail-row.up b   { color: var(--up); }
.detail-row.down b { color: var(--down); }

/* ── 좁은 화면: 여백 축소 / 모바일: 사이드바를 상단 가로 메뉴로 ── */
@media (max-width: 1100px) {
  .main { padding: 32px 24px 80px; }
  .indicator-row { gap: 20px; padding: 18px; }
}
@media (max-width: 760px) {
  .layout { display: block; }
  .sidebar { position: sticky; top: 0; z-index: 10; width: auto; min-width: 0; height: auto;
             padding: 6px 8px; display: flex; gap: 2px; overflow-x: auto; white-space: nowrap;
             border-right: none; border-bottom: 1px solid var(--border);
             background: rgba(14,14,13,.92); backdrop-filter: blur(10px); scrollbar-width: none; }
  .sidebar-title, .nav-group { display: none; }
  .nav-item { padding: 7px 11px; font-size: 13.5px; }
  .nav-item.active { box-shadow: inset 0 -2px 0 var(--accent); }
  .main { padding: 24px 16px 64px; }
  .page-title { font-size: 22px; }
  .page-header { margin-bottom: 28px; padding-bottom: 18px; }
  .category-section { margin-bottom: 44px; scroll-margin-top: 56px; }
  .category-name { font-size: 19px; }
  .indicator-row { padding: 16px 14px; border-radius: 10px; }
  .row-desc, .row-updown { min-width: 0; }
}
"""

_JS_NAV = """
<script>
(function () {
  var sections = document.querySelectorAll('.category-section');
  var navItems = document.querySelectorAll('.nav-item');

  // 메뉴는 #앵커 링크라 이동은 브라우저가 처리 — 여기선 현재 위치 표시만
  var observer = new IntersectionObserver(function (entries) {
    entries.forEach(function (e) {
      if (e.isIntersecting) {
        navItems.forEach(function (n) {
          var on = n.getAttribute('href') === '#' + e.target.id;
          n.classList.toggle('active', on);
          // 모바일 가로 메뉴: 현재 항목이 화면 밖이면 보이도록 옆으로 밀어줌
          if (on && n.parentNode.scrollWidth > n.parentNode.clientWidth) {
            n.parentNode.scrollTo({ left: n.offsetLeft - 16, behavior: 'smooth' });
          }
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
    var text = btn.querySelector('span');
    var flash = function (label, cls) {
      text.textContent = label; btn.classList.add(cls);
      setTimeout(function () { text.textContent = 'Copy'; btn.classList.remove(cls); }, 1500);
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
        page_title:   str = "미국 달러 유동성 대시보드",
        sidebar_title: str = "매크로 모니터",
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
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/variable/pretendardvariable-dynamic-subset.min.css">
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
                f'<a class="nav-item" href="#cat-{cat}">{name}</a>'
                for cat, (name, _, _) in self._category_meta.items()
            )
        if self._top_nav:
            label, target = self._top_nav
            items = f'<a class="nav-item" href="#{target}">{label}</a>' + items
        return f"""
<nav class="sidebar" aria-label="카테고리">
  <div class="sidebar-title">{self._sidebar_title}</div>
  {items}
</nav>"""

    def _build_sidebar_group(self, label: str, cats: list[int]) -> str:
        rows = "\n".join(
            f'<a class="nav-item" href="#cat-{cat}">{self._category_meta[cat][0]}</a>'
            for cat in cats if cat in self._category_meta
        )
        return f'<div class="nav-group">{label}</div>{rows}'

    # ── 메인 콘텐츠 ──────────────────────────────────────────────────

    def _build_main(self) -> str:
        header = f"""
<div class="page-header">
  <div class="page-title">{self._page_title}</div>
  <div class="page-subtitle">
    <span>업데이트 <b>{self._timestamp}</b></span>
    <span>데이터 <b>{self._data_sources}</b></span>
  </div>
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
        # 제목을 따로 붙인 묶음 차트: 첫 지표 설명만 보이면 나머지 설명이 사라지므로 전부 나열
        head = slot_results[0]
        if len(slot_results) > 1 and head.key in SLOT_TITLES:
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
        copy_btn = (
            '<button class="copy-btn" type="button" aria-label="차트 이미지 복사">'
            '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">'
            '<rect x="5" y="5" width="8.5" height="8.5" rx="1.5"/><path d="M10.5 5V3.5A1.5 1.5 0 0 0 9 2H3.5A1.5 1.5 0 0 0 2 3.5V9a1.5 1.5 0 0 0 1.5 1.5H5"/>'
            '</svg><span>Copy</span></button>'
        )

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
  </div>
  <div class="row-head">
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
