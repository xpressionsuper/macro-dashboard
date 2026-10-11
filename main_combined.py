#!/usr/bin/env python3
"""
main_combined.py ─ 유동성 + 경기침체 통합 대시보드 진입점
=====================================================================
유동성 모니터(카테고리 ①~⑤)와 경기침체 모니터(카테고리 ⑥~⑨)를
하나의 사이드바·하나의 HTML 파일로 합칩니다. 각 파이프라인의 데이터
수집·진단 로직은 main.py / main_recession.py 것을 그대로 재사용하고,
카테고리 번호만 겹치지 않게 밀어서 합칩니다.

실행:
  python main_combined.py
"""

import sys
import warnings
from datetime import datetime

warnings.filterwarnings("ignore")

# Windows 콘솔이 cp949(한글 기본 코드페이지)일 때 ╔═╗ 같은 박스문자·이모지를
# 못 그려 그대로 죽는 문제 방지 — 콘솔 코드페이지와 무관하게 UTF-8로 출력
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import importlib.util

_REQUIRED = ["fredapi", "plotly", "pandas", "numpy", "requests"]
_missing  = [p for p in _REQUIRED if not importlib.util.find_spec(p)]
if _missing:
    print(f"❌ 미설치 패키지: {', '.join(_missing)}")
    print(f"   실행: pip install {' '.join(_missing)}")
    sys.exit(1)

import pandas as pd
import requests

import config as liq_cfg
import config_recession as rec_cfg
import config_inflation as inf_cfg
import config_investment as inv_cfg
from data.fred_client      import FredClient
from data.treasury_client  import TreasuryClient
from data.derived          import DerivedCalculator
from indicators.fed_supply       import FedSupplyGroup
from indicators.treasury_channel import TreasuryChannelGroup
from indicators.net_liquidity    import NetLiquidityGroup
from indicators.financial_market import FinancialMarketGroup
from indicators.global_dollar    import GlobalDollarGroup
from indicators.base             import IndicatorResult, GenericIndicatorGroup
from indicators                  import capitulation
from dashboard.chart_builder     import ChartBuilder
from dashboard.html_exporter     import HtmlExporter
from notify.telegram_sender      import TelegramSender
from main import MacroDashboard as _YahooFetcher   # Yahoo 벤치마크 fetch 재사용

OUTPUT_HTML  = "combined_dashboard.html"
REC_OFFSET   = 5    # 경기침체 카테고리 번호를 5만큼 밀어 유동성(1~5)과 겹치지 않게 함
INF_OFFSET   = 9    # 물가 카테고리 번호를 9만큼 밀어 유동성+침체(1~9)와 겹치지 않게 함
INV_OFFSET   = 13   # 투자 카테고리 번호를 13만큼 밀어 앞의 세 대시보드(1~13)와 겹치지 않게 함


class CombinedDashboard:
    """유동성 + 경기침체 파이프라인을 한 화면으로 합쳐 조율합니다."""

    def run(self) -> None:
        self._print_banner()
        self._validate_env()

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M KST")

        print("\n[1/8]  유동성 데이터 수집")
        liq_raw = self._collect_liquidity_data()
        print(f"       수집 완료: {len(liq_raw)}개 시리즈\n")

        print("[2/8]  경기침체 데이터 수집")
        rec_raw = self._collect_recession_data()
        print(f"       수집 완료: {len(rec_raw)}개 시리즈\n")

        print("[3/8]  물가 데이터 수집")
        inf_raw = self._collect_inflation_data()
        print(f"       수집 완료: {len(inf_raw)}개 시리즈\n")

        print("[4/8]  투자 데이터 수집")
        inv_raw = self._collect_investment_data()
        print(f"       수집 완료: {len(inv_raw)}개 시리즈\n")

        print("[5/8]  파생 지표 계산 (유동성 파생 + 물가 YoY 변환)")
        derived = DerivedCalculator(liq_raw).compute()
        for k, s in derived.items():
            print(f"  ✅  {k:<20}  {len(s)} pts")
        liq_raw.update(derived)
        self._apply_yoy_transform(inf_raw)

        print("\n[6/8]  지표 진단")
        liq_released = FredClient(liq_cfg.FRED_API_KEY).fetch_batch_release_dates(liq_cfg.ALL_INDICATORS)
        rec_released = FredClient(rec_cfg.FRED_API_KEY).fetch_batch_release_dates(rec_cfg.ALL_INDICATORS)
        inf_released = FredClient(liq_cfg.FRED_API_KEY).fetch_batch_release_dates(inf_cfg.ALL_INDICATORS)

        liq_results = self._run_liquidity_diagnosis(liq_raw, liq_released)
        rec_results = self._run_recession_diagnosis(rec_raw, rec_released)
        inf_results = self._run_generic_diagnosis(inf_raw, inf_cfg.CATEGORY_META, inf_released)
        inv_results = self._run_generic_diagnosis(inv_raw, inv_cfg.CATEGORY_META)
        for r in rec_results:
            r.category += REC_OFFSET
        for r in inf_results:
            r.category += INF_OFFSET
        for r in inv_results:
            r.category += INV_OFFSET
        all_results = liq_results + rec_results + inf_results + inv_results
        print(
            f"       진단 완료: {len(all_results)}개 지표 "
            f"(유동성 {len(liq_results)} + 침체 {len(rec_results)} + 물가 {len(inf_results)} + 투자 {len(inv_results)})"
        )

        # SPX/GOLD 벤치마크는 모든 파이프라인이 공유 → 한 번만 수집
        merged_raw = {**liq_raw, **rec_raw, **inf_raw, **inv_raw}
        merged_raw["SPX"]  = liq_raw.get("SPX")
        merged_raw["GOLD"] = liq_raw.get("GOLD")

        category_meta = {
            **liq_cfg.CATEGORY_META,
            **{cat + REC_OFFSET: meta for cat, meta in rec_cfg.CATEGORY_META.items()},
            **{cat + INF_OFFSET: meta for cat, meta in inf_cfg.CATEGORY_META.items()},
            **{cat + INV_OFFSET: meta for cat, meta in inv_cfg.CATEGORY_META.items()},
        }

        print("\n[7/8]  차트 생성")
        builder = ChartBuilder(all_results, merged_raw)
        figures = builder.build_all()
        print(f"       {len(figures)}개 카테고리 차트 완성")

        print("\n[8/8]  HTML 대시보드 생성")
        HtmlExporter(
            figures, all_results, timestamp, category_meta,
            page_title="🏦📉💵📈 미국 경기·유동성·물가·투자 통합 대시보드",
            sidebar_title="📊 통합 모니터",
            data_sources="FRED · TreasuryDirect · NY Fed",
            sidebar_groups=[
                ("유동성", list(liq_cfg.CATEGORY_META.keys())),
                ("경기침체", [cat + REC_OFFSET for cat in rec_cfg.CATEGORY_META.keys()]),
                ("물가", [cat + INF_OFFSET for cat in inf_cfg.CATEGORY_META.keys()]),
                ("투자", [cat + INV_OFFSET for cat in inv_cfg.CATEGORY_META.keys()]),
            ],
            top_panel=capitulation.render_panel(merged_raw),
            top_nav=("🏳️ 연준 항복 판정", "fed-capitulation"),
        ).export(OUTPUT_HTML)

        TelegramSender(liq_cfg.TELEGRAM_TOKEN, liq_cfg.TELEGRAM_CHAT_ID).send_report(
            all_results, OUTPUT_HTML, category_meta,
            title="미국 경기·유동성·물가·투자 통합 대시보드",
        )

        print("\n  🎉  모든 작업 완료!\n")

    # ── 유동성 파이프라인 ────────────────────────────────────────────

    def _collect_liquidity_data(self) -> dict:
        raw = {}
        fred_client = FredClient(liq_cfg.FRED_API_KEY)
        raw.update(fred_client.fetch_batch(liq_cfg.ALL_INDICATORS))

        tga = TreasuryClient().fetch_tga()
        if not tga.empty:
            raw["TGA"] = tga

        raw["SPX"]  = fred_client.fetch_series("SP500", label="S&P500 (오버레이)")
        raw["GOLD"] = _YahooFetcher._fetch_yahoo_series("GC=F", label="금 선물 (오버레이)")
        return raw

    def _run_liquidity_diagnosis(self, raw: dict, released: dict) -> list[IndicatorResult]:
        groups = [
            FedSupplyGroup(raw, released), TreasuryChannelGroup(raw, released),
            NetLiquidityGroup(raw, released),
            FinancialMarketGroup(raw, released), GlobalDollarGroup(raw, released),
        ]
        results = []
        for group in groups:
            cat_results = group.diagnose()
            results.extend(cat_results)
            print(f"  ✅  {group.category_name}  ({len(cat_results)}개)")
        return results

    # ── 경기침체 파이프라인 ──────────────────────────────────────────

    def _collect_recession_data(self) -> dict:
        fred_client = FredClient(rec_cfg.FRED_API_KEY)
        return fred_client.fetch_batch(rec_cfg.ALL_INDICATORS)

    def _run_recession_diagnosis(self, raw: dict, released: dict) -> list[IndicatorResult]:
        return self._run_generic_diagnosis(raw, rec_cfg.CATEGORY_META, released)

    # ── 물가 파이프라인 ──────────────────────────────────────────────

    def _collect_inflation_data(self) -> dict:
        fred_client = FredClient(liq_cfg.FRED_API_KEY)
        return fred_client.fetch_batch(inf_cfg.ALL_INDICATORS)

    @staticmethod
    def _apply_yoy_transform(raw: dict) -> None:
        """CPI·PCE·PPI·임금처럼 레벨(지수)로만 제공되는 시리즈를 YoY(%) 로 바꿉니다."""
        for key in inf_cfg.YOY_TRANSFORM_KEYS:
            series = raw.get(key)
            if series is None or series.empty or len(series) < 13:
                continue
            raw[key] = series.pct_change(12).mul(100).dropna().rename(key)

    # ── 투자(밸류에이션·심리) 파이프라인 ─────────────────────────────

    def _collect_investment_data(self) -> dict:
        """버핏 지표(시가총액/GDP), CAPE, SKEW, 마진부채를 수집·계산합니다."""
        fred_client = FredClient(liq_cfg.FRED_API_KEY)
        equity_mv = fred_client.fetch_series(
            inv_cfg.EQUITY_MV_FRED_ID, divisor=1_000, label="전체 상장기업 시가총액 (원시)"
        )  # 백만 달러 → 십억 달러로 환산해 GDP와 단위 맞춤
        gdp = fred_client.fetch_series(inv_cfg.GDP_FRED_ID, label="명목 GDP (원시)")

        raw: dict = {}
        if not equity_mv.empty and not gdp.empty:
            df = pd.DataFrame({"MV": equity_mv, "GDP": gdp}).dropna()
            raw["BUFFETT"] = (df["MV"] / df["GDP"] * 100).rename("BUFFETT")
            print(f"  ✅  버핏 지표 (계산됨)                       [BUFFETT]  {len(raw['BUFFETT'])} pts")
        else:
            raw["BUFFETT"] = pd.Series(dtype=float, name="BUFFETT")

        raw["CAPE"]        = self._fetch_shiller_cape()
        raw["SKEW"]        = self._fetch_cboe_skew()
        raw["MARGIN_DEBT"] = self._fetch_finra_margin_debt()

        raw["DEBT_TOTAL"] = self._fetch_treasury_total_debt()
        raw["DEBT_SHORT"], raw["DEBT_LONG"] = self._fetch_treasury_debt_by_maturity()

        interest = fred_client.fetch_series("A091RC1Q027SBEA", label="연방정부 이자비용 (원시)")
        if not interest.empty and not gdp.empty:
            df2 = pd.DataFrame({"INT": interest, "GDP": gdp}).dropna()
            raw["INTEREST_GDP"] = (df2["INT"] / df2["GDP"] * 100).rename("INTEREST_GDP")
            print(f"  ✅  이자비용/GDP (계산됨)                    [INTEREST_GDP]  {len(raw['INTEREST_GDP'])} pts")
        else:
            raw["INTEREST_GDP"] = pd.Series(dtype=float, name="INTEREST_GDP")

        short, long_ = raw["DEBT_SHORT"], raw["DEBT_LONG"]
        if not short.empty and not long_.empty:
            df3 = pd.DataFrame({"SHORT": short, "LONG": long_}).dropna()
            raw["DEBT_SHORT_SHARE"] = (df3["SHORT"] / (df3["SHORT"] + df3["LONG"]) * 100).rename("DEBT_SHORT_SHARE")
            print(f"  ✅  국채 중 단기채 비중 (계산됨)               [DEBT_SHORT_SHARE]  {len(raw['DEBT_SHORT_SHARE'])} pts")
        else:
            raw["DEBT_SHORT_SHARE"] = pd.Series(dtype=float, name="DEBT_SHORT_SHARE")

        return raw

    @staticmethod
    def _fetch_treasury_total_debt() -> pd.Series:
        """
        TreasuryDirect MSPD Table 1의 월별 총부채를 가져옵니다. FRED의 GFDEBTN은
        분기 시리즈라 갱신이 느려서, 단기·장기 국채와 같은 월별 소스로 통일합니다.
        """
        label, sid = "연방정부 총부채", "DEBT_TOTAL"
        try:
            resp = requests.get(
                "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/debt/mspd/mspd_table_1",
                params={
                    "fields": "record_date,security_type_desc,total_mil_amt",
                    "filter": "security_type_desc:eq:Total Public Debt Outstanding",
                    "sort": "record_date",
                    "page[size]": 5000,
                },
                timeout=30,
            )
            df = pd.DataFrame(resp.json()["data"])
            s = pd.Series(
                pd.to_numeric(df["total_mil_amt"], errors="coerce").values / 1_000,  # 백만 → 십억 달러
                index=pd.to_datetime(df["record_date"]),
                name=sid,
            ).sort_index().dropna()
            print(f"  ✅  {label:<36} [{sid}]  {len(s)} pts")
            return s
        except Exception as exc:
            print(f"  ❌  {label:<36} [{sid}]  최종 실패 → {exc}")
            return pd.Series(dtype=float, name=sid)

    @staticmethod
    def _fetch_treasury_debt_by_maturity() -> tuple[pd.Series, pd.Series]:
        """
        TreasuryDirect MSPD Table 1에서 월별 단기(Bills)·장기(Notes+Bonds)
        국채 발행잔액을 가져옵니다 (키 불필요).
        """
        label, sid = "단기·장기 국채 잔액", "DEBT_MATURITY"
        try:
            resp = requests.get(
                "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/debt/mspd/mspd_table_1",
                params={
                    "fields": "record_date,security_type_desc,security_class_desc,total_mil_amt",
                    "filter": "security_type_desc:eq:Marketable,security_class_desc:in:(Bills,Notes,Bonds)",
                    "sort": "record_date",
                    "page[size]": 5000,
                },
                timeout=30,
            )
            df = pd.DataFrame(resp.json()["data"])
            df["record_date"]   = pd.to_datetime(df["record_date"])
            df["total_mil_amt"] = pd.to_numeric(df["total_mil_amt"], errors="coerce")

            wide = df.pivot_table(
                index="record_date", columns="security_class_desc",
                values="total_mil_amt", aggfunc="last",
            ) / 1_000   # 백만 → 십억 달러

            short = wide["Bills"].dropna().rename("DEBT_SHORT")
            long_ = (wide["Notes"] + wide["Bonds"]).dropna().rename("DEBT_LONG")
            print(f"  ✅  단기국채 (Bills)                         [DEBT_SHORT]  {len(short)} pts")
            print(f"  ✅  장기국채 (Notes+Bonds)                    [DEBT_LONG]  {len(long_)} pts")
            return short, long_
        except Exception as exc:
            print(f"  ❌  {label:<36} [{sid}]  최종 실패 → {exc}")
            empty = pd.Series(dtype=float)
            return empty.rename("DEBT_SHORT"), empty.rename("DEBT_LONG")

    @staticmethod
    def _fetch_shiller_cape() -> pd.Series:
        """multpl.com에서 실러 CAPE 월간 시계열을 가져옵니다 (키 불필요)."""
        label, sid = "실러 CAPE 비율", "CAPE"
        try:
            tables = pd.read_html(
                "https://www.multpl.com/shiller-pe/table/by-month",
            )
            df = tables[0]
            s = pd.Series(
                df["Value"].astype(float).values,
                index=pd.to_datetime(df["Date"]),
                name=sid,
            ).sort_index().dropna()
            print(f"  ✅  {label:<36} [{sid}]  {len(s)} pts")
            return s
        except Exception as exc:
            print(f"  ❌  {label:<36} [{sid}]  최종 실패 → {exc}")
            return pd.Series(dtype=float, name=sid)

    @staticmethod
    def _fetch_cboe_skew() -> pd.Series:
        """CBOE가 공개하는 SKEW 지수 일별 CSV를 가져옵니다 (키 불필요)."""
        label, sid = "CBOE SKEW 지수", "SKEW"
        try:
            df = pd.read_csv(
                "https://cdn.cboe.com/api/global/us_indices/daily_prices/SKEW_History.csv",
            )
            s = pd.Series(
                df["SKEW"].astype(float).values,
                index=pd.to_datetime(df["DATE"]),
                name=sid,
            ).sort_index().dropna()
            print(f"  ✅  {label:<36} [{sid}]  {len(s)} pts")
            return s
        except Exception as exc:
            print(f"  ❌  {label:<36} [{sid}]  최종 실패 → {exc}")
            return pd.Series(dtype=float, name=sid)

    @staticmethod
    def _fetch_finra_margin_debt() -> pd.Series:
        """FINRA가 공개하는 전체 히스토리 엑셀(1997년~)에서 마진부채(백만 달러)를 가져옵니다."""
        label, sid = "증권사 마진부채", "MARGIN_DEBT"
        try:
            df = pd.read_excel(
                "https://www.finra.org/sites/default/files/2021-03/margin-statistics.xlsx"
            )
            debit_col = [c for c in df.columns if "Debit Balances" in c][0]
            s = pd.Series(
                (df[debit_col].astype(float) / 1_000).values,   # 백만 → 십억 달러
                index=pd.to_datetime(df["Year-Month"], format="%Y-%m"),
                name=sid,
            ).sort_index().dropna()
            print(f"  ✅  {label:<36} [{sid}]  {len(s)} pts")
            return s
        except Exception as exc:
            print(f"  ❌  {label:<36} [{sid}]  최종 실패 → {exc}")
            return pd.Series(dtype=float, name=sid)

    # ── 공용: 카테고리 메타 기반 범용 진단 ────────────────────────────

    @staticmethod
    def _run_generic_diagnosis(raw: dict, category_meta: dict, released: dict = None) -> list[IndicatorResult]:
        results = []
        for cat, (name, _desc, indicators) in category_meta.items():
            group = GenericIndicatorGroup(raw, indicators, cat, name, released=released)
            cat_results = group.diagnose()
            results.extend(cat_results)
            print(f"  ✅  {name}  ({len(cat_results)}개)")
        return results

    # ── 유틸 ──────────────────────────────────────────────────────────

    @staticmethod
    def _validate_env() -> None:
        print(f"\n  환경변수:")
        print(f"    FRED_API_KEY     : {'✅' if liq_cfg.FRED_API_KEY     else '❌ 없음 → 종료'}")

        if not liq_cfg.FRED_API_KEY:
            print("\n❌  FRED_API_KEY 미설정. https://fred.stlouisfed.org 에서 무료 발급.")
            sys.exit(1)

    @staticmethod
    def _print_banner() -> None:
        print("""
╔══════════════════════════════════════════════════════════════════════╗
║         🏦📉💵📈  미국 경기·유동성·물가·투자 통합 모니터              ║
║  ①~⑤ 유동성   ⑥~⑨ 경기침체   ⑩~⑬ 물가   ⑭ 투자(버핏 지표)          ║
╚══════════════════════════════════════════════════════════════════════╝""")


if __name__ == "__main__":
    CombinedDashboard().run()
