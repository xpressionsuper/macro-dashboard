#!/usr/bin/env python3
"""
main_recession.py ─ 경기침체 모니터 진입점
=====================================================================
구조는 main.py(유동성 모니터)와 동일합니다. 데이터 소스는 FRED 하나뿐이라
Treasury API·파생 지표 계산 단계가 없고, 카테고리별 진단은 범용
GenericIndicatorGroup 하나로 처리합니다.

실행:
  python main_recession.py
"""

import sys
import warnings
from datetime import datetime

warnings.filterwarnings("ignore")

import importlib.util

_REQUIRED = ["fredapi", "plotly", "pandas", "numpy", "requests"]
_missing  = [p for p in _REQUIRED if not importlib.util.find_spec(p)]
if _missing:
    print(f"❌ 미설치 패키지: {', '.join(_missing)}")
    print(f"   실행: pip install {' '.join(_missing)}")
    sys.exit(1)

import pandas as pd

import config_recession as cfg
from data.fred_client        import FredClient
from indicators.base         import IndicatorResult, GenericIndicatorGroup
from dashboard.chart_builder import ChartBuilder
from dashboard.html_exporter import HtmlExporter
from notify.telegram_sender  import TelegramSender
from main import MacroDashboard as _LiquidityDashboard  # Yahoo 벤치마크 fetch 재사용


class RecessionDashboard:
    """경기침체 지표 파이프라인을 조율하는 최상위 클래스."""

    def run(self) -> None:
        self._print_banner()
        self._validate_env()

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M KST")

        print("\n[1/5]  데이터 수집")
        raw = self._collect_data()
        print(f"       수집 완료: {len(raw)}개 시리즈\n")

        print("[2/5]  지표 진단")
        results: list[IndicatorResult] = self._run_diagnosis(raw)
        print(f"       진단 완료: {len(results)}개 지표")

        print("\n[3/5]  차트 생성")
        builder = ChartBuilder(results, raw)
        figures = builder.build_all()
        print(f"       {len(figures)}개 카테고리 차트 완성")

        print("\n[4/5]  HTML 대시보드 생성")
        HtmlExporter(
            figures, results, timestamp, cfg.CATEGORY_META,
            page_title="미국 경기침체 지표 대시보드",
            sidebar_title="침체 모니터",
            data_sources="FRED",
        ).export(cfg.OUTPUT_HTML)

        print("\n[5/5]  텔레그램 전송")
        TelegramSender(cfg.TELEGRAM_TOKEN, cfg.TELEGRAM_CHAT_ID).send_report(
            results, cfg.OUTPUT_HTML, cfg.CATEGORY_META, title="미국 경기침체 지표 대시보드"
        )

        print("\n  🎉  모든 작업 완료!\n")

    # ── 데이터 수집 ──────────────────────────────────────────────────

    def _collect_data(self) -> dict[str, pd.Series]:
        raw: dict[str, pd.Series] = {}

        fred_client = FredClient(cfg.FRED_API_KEY)
        raw.update(fred_client.fetch_batch(cfg.ALL_INDICATORS))

        # 모든 차트에 겹쳐 그릴 시장 벤치마크 (S&P500, 금) — 유동성 모니터와 동일 소스
        raw["SPX"]  = fred_client.fetch_series("SP500", label="S&P500 (오버레이)")
        raw["GOLD"] = _LiquidityDashboard._fetch_yahoo_series("GC=F", label="금 선물 (오버레이)")

        return raw

    # ── 카테고리별 진단 ───────────────────────────────────────────────

    def _run_diagnosis(self, raw: dict[str, pd.Series]) -> list[IndicatorResult]:
        all_results: list[IndicatorResult] = []
        for cat, (name, _desc, indicators) in cfg.CATEGORY_META.items():
            group = GenericIndicatorGroup(raw, indicators, cat, name)
            cat_results = group.diagnose()
            all_results.extend(cat_results)
            print(f"  ✅  {name}  ({len(cat_results)}개)")
        return all_results

    # ── 유틸 ──────────────────────────────────────────────────────────

    @staticmethod
    def _validate_env() -> None:
        print(f"\n  환경변수:")
        print(f"    FRED_API_KEY     : {'✅' if cfg.FRED_API_KEY     else '❌ 없음 → 종료'}")
        print(f"    TELEGRAM_TOKEN   : {'✅' if cfg.TELEGRAM_TOKEN   else '⚠️  없음 (전송 건너뜀)'}")
        print(f"    TELEGRAM_CHAT_ID : {'✅' if cfg.TELEGRAM_CHAT_ID else '⚠️  없음 (전송 건너뜀)'}")

        if not cfg.FRED_API_KEY:
            print("\n❌  FRED_API_KEY 미설정. https://fred.stlouisfed.org 에서 무료 발급.")
            sys.exit(1)

    @staticmethod
    def _print_banner() -> None:
        print("""
╔══════════════════════════════════════════════════════════════════════╗
║         📉  미국 경기침체 지표 모니터                                 ║
║  ① 노동시장  ② 소비·생산  ③ 주택시장  ④ 종합 선행·모델 지표         ║
╚══════════════════════════════════════════════════════════════════════╝""")


if __name__ == "__main__":
    RecessionDashboard().run()
