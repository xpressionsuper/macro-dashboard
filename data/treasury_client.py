"""
data/treasury_client.py ─ Treasury Fiscal Data API 클라이언트
=====================================================================
TGA(Treasury General Account) 잔고를 매일 업데이트되는
fiscaldata.treasury.gov API에서 수집합니다.
"""

from datetime import datetime, timedelta

import pandas as pd
import requests

from config import LOOKBACK_DAYS


class TreasuryClient:
    """Treasury Fiscal Data API에서 TGA 잔고를 수집합니다."""

    _BASE_URL = (
        "https://api.fiscaldata.treasury.gov/services/api/v1"
        "/accounting/dts/operating_cash_balance"
    )

    # account_type 필드가 API 버전에 따라 다를 수 있어 복수 패턴 매칭
    _SEARCH_PATTERNS = [
        "Federal Reserve",
        "federal reserve",
        "Deposits, Federal",
        "Treasury General Account",
    ]

    def __init__(self):
        self._start = (
            datetime.now() - timedelta(days=LOOKBACK_DAYS)
        ).strftime("%Y-%m-%d")

    # ── Public API ───────────────────────────────────────────────────

    def fetch_tga(self, retries: int = 3) -> pd.Series:
        """
        TGA 잔고 시계열을 반환합니다.

        Returns:
            DatetimeIndex, 단위 $B 인 pd.Series.
            실패 시 빈 Series 반환.
        """
        for attempt in range(retries):
            try:
                rows = self._request_all_rows()
                tga  = self._filter_federal_reserve(rows)

                if tga.empty:
                    raise ValueError("Federal Reserve 계좌 데이터 없음")

                print(f"  ✅  {'TGA 잔고':<36} [Treasury API]  {len(tga)} pts")
                return tga

            except Exception as exc:
                import time
                wait = 2 ** attempt
                if attempt < retries - 1:
                    print(f"  ↺   TGA 재시도 {attempt+1}/{retries} ({wait}s)… ({exc})")
                    time.sleep(wait)
                else:
                    print(f"  ❌  TGA 최종 실패 → {exc}")
                    return pd.Series(dtype=float, name="TGA")

    # ── Private helpers ──────────────────────────────────────────────

    def _request_all_rows(self) -> list[dict]:
        """API에서 전체 페이지를 수집합니다."""
        params = {
            "fields":     "record_date,open_today_bal,account_type",
            "filter":     f"record_date:gte:{self._start}",
            "sort":       "record_date",
            "page[size]": 10_000,
        }
        resp = requests.get(self._BASE_URL, params=params, timeout=30)

        if resp.status_code != 200:
            raise ValueError(
                f"HTTP {resp.status_code} "
                f"(x-deny-reason: {resp.headers.get('x-deny-reason','없음')})"
            )

        rows = resp.json().get("data", [])
        if not rows:
            raise ValueError("API 응답 data 필드 없음")

        return rows

    def _filter_federal_reserve(self, rows: list[dict]) -> pd.Series:
        """
        account_type 필드에서 Federal Reserve 계좌 행만 추출하여
        날짜-잔고 Series로 변환합니다.
        """
        matched = []
        for row in rows:
            acct = row.get("account_type", "")
            if any(pat.lower() in acct.lower() for pat in self._SEARCH_PATTERNS):
                matched.append(row)

        if not matched:
            # 진단용: 실제 account_type 목록 출력
            types = sorted(set(r.get("account_type", "") for r in rows))
            print(f"       [TGA 진단] account_type 목록: {types}")
            raise ValueError("매칭되는 Federal Reserve 행 없음")

        df = pd.DataFrame(matched)
        df["record_date"]    = pd.to_datetime(df["record_date"])
        df["open_today_bal"] = pd.to_numeric(df["open_today_bal"], errors="coerce") / 1_000

        series = (
            df.set_index("record_date")["open_today_bal"]
            .dropna()
            .sort_index()
            .rename("TGA")
        )
        return series
