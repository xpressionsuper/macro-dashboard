#!/usr/bin/env python3
"""
main.py ─ 매크로 유동성 모니터 v3 진입점
=====================================================================
실행 흐름:
  [1] 데이터 수집   FredClient + TreasuryClient
  [2] 파생 계산     DerivedCalculator
  [3] 지표 진단     5개 IndicatorGroup
  [4] 차트 생성     ChartBuilder
  [5] HTML 출력     HtmlExporter
  [6] 텔레그램      TelegramSender
=====================================================================
실행:
  python main.py

GitHub Actions 환경변수 (Secrets):
  FRED_API_KEY / TELEGRAM_TOKEN / TELEGRAM_CHAT_ID
"""

import sys
import warnings
from datetime import datetime

warnings.filterwarnings("ignore")

# ── 의존성 사전 확인 ──────────────────────────────────────────────────
import importlib.util  # 👈 [필수] 이 모듈을 임포트해야 NameError가 안 납니다!

_REQUIRED = ["fredapi", "plotly", "pandas", "numpy", "requests"]
_missing  = [p for p in _REQUIRED if not importlib.util.find_spec(p)] # 👈 깔끔하게 변경
if _missing:
    print(f"❌ 미설치 패키지: {', '.join(_missing)}")
    print(f"   실행: pip install {' '.join(_missing)}")
    sys.exit(1)

import pandas as pd

import config as cfg
from data.fred_client      import FredClient
from data.treasury_client  import TreasuryClient
from data.derived          import DerivedCalculator
from indicators.fed_supply       import FedSupplyGroup
from indicators.treasury_channel import TreasuryChannelGroup
from indicators.net_liquidity    import NetLiquidityGroup
from indicators.financial_market import FinancialMarketGroup
from indicators.base             import IndicatorResult
from dashboard.chart_builder     import ChartBuilder
from dashboard.html_exporter     import HtmlExporter
from notify.telegram_sender      import TelegramSender


# ══════════════════════════════════════════════════════════════════════
class MacroDashboard:
    """전체 파이프라인을 조율하는 최상위 클래스."""

    def run(self) -> None:
        self._print_banner()
        self._validate_env()

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M KST")

        # ── [1] 데이터 수집 ────────────────────────────────────────────
        print("\n[1/6]  데이터 수집")
        raw = self._collect_data()
        print(f"       수집 완료: {len(raw)}개 시리즈\n")

        # ── [2] 파생 지표 계산 ─────────────────────────────────────────
        print("[2/6]  파생 지표 계산")
        derived = DerivedCalculator(raw).compute()
        for k, s in derived.items():
            print(f"  ✅  {k:<20}  {len(s)} pts")
        raw.update(derived)   # 원시 + 파생을 하나의 dict로 합침

        # ── [3] 지표 진단 ──────────────────────────────────────────────
        print("\n[3/6]  지표 진단")
        results: list[IndicatorResult] = self._run_diagnosis(raw)
        print(f"       진단 완료: {len(results)}개 지표")

        # ── [4] 차트 생성 ──────────────────────────────────────────────
        print("\n[4/6]  차트 생성")
        builder = ChartBuilder(results, raw)
        figures = builder.build_all()
        print(f"       {len(figures)}개 카테고리 차트 완성")

        # ── [5] HTML 저장 ──────────────────────────────────────────────
        print("\n[5/6]  HTML 대시보드 생성")
        HtmlExporter(
            figures, results, timestamp, cfg.CATEGORY_META,
            page_title="미국 달러 유동성 대시보드",
            sidebar_title="매크로 모니터",
        ).export(cfg.OUTPUT_HTML)

        # ── [6] 텔레그램 전송 ─────────────────────────────────────────
        print("\n[6/6]  텔레그램 전송")
        TelegramSender(cfg.TELEGRAM_TOKEN, cfg.TELEGRAM_CHAT_ID).send_report(
            results, cfg.OUTPUT_HTML, cfg.CATEGORY_META, title="미국 달러 유동성 대시보드"
        )

        print("\n  🎉  모든 작업 완료!\n")

    # ── 데이터 수집 ──────────────────────────────────────────────────

    def _collect_data(self) -> dict[str, pd.Series]:
        raw: dict[str, pd.Series] = {}

        # FRED (fred_id가 있는 모든 지표 일괄 수집)
        fred_client = FredClient(cfg.FRED_API_KEY)
        raw.update(fred_client.fetch_batch(cfg.ALL_INDICATORS))

        # Treasury TGA (별도 API)
        tga = TreasuryClient().fetch_tga()
        if not tga.empty:
            raw["TGA"] = tga

        # 모든 차트에 겹쳐 그릴 시장 벤치마크 (S&P500, 금)
        raw["SPX"]  = fred_client.fetch_series("SP500", label="S&P500 (오버레이)")
        raw["GOLD"] = self._fetch_yahoo_series("GC=F", label="금 선물 (오버레이)")

        return raw

    @staticmethod
    def _fetch_yahoo_series(symbol: str, label: str) -> pd.Series:
        """Yahoo Finance 차트 API에서 일봉 종가를 가져옵니다 (키 불필요)."""
        import requests

        try:
            resp = requests.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                params={"range": "10y", "interval": "1d"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=15,
            )
            data   = resp.json()["chart"]["result"][0]
            ts     = data["timestamp"]
            closes = data["indicators"]["quote"][0]["close"]
            s = pd.Series(
                closes,
                index=pd.to_datetime(ts, unit="s").normalize(),
                name=symbol,
            ).dropna()
            print(f"  ✅  {label:<36} [{symbol}]  {len(s)} pts")
            return s
        except Exception as exc:
            print(f"  ❌  {label:<36} [{symbol}]  최종 실패 → {exc}")
            return pd.Series(dtype=float, name=symbol)

    # ── 카테고리별 진단 ───────────────────────────────────────────────

    def _run_diagnosis(
        self, raw: dict[str, pd.Series]
    ) -> list[IndicatorResult]:
        groups = [
            FedSupplyGroup(raw),
            TreasuryChannelGroup(raw),
            NetLiquidityGroup(raw),
            FinancialMarketGroup(raw),
        ]

        all_results: list[IndicatorResult] = []
        for group in groups:
            cat_results = group.diagnose()
            all_results.extend(cat_results)
            print(f"  ✅  {group.category_name}  ({len(cat_results)}개)")

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
║         📊  미국 달러 유동성 종합 모니터  v3.0                        ║
║  ① Fed BS·RRP·지급준비금  ② TGA  ③ Net LIQ·SOFR Spread·M2             ║
║  ④ HY/IG OAS·VIX·수익률곡선  ⑤ 스왑라인·DXY·JPY                   ║
╚══════════════════════════════════════════════════════════════════════╝""")


# ══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    MacroDashboard().run()
