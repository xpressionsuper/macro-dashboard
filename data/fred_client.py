"""
data/fred_client.py ─ FRED API 클라이언트
=====================================================================
지수 백오프 재시도, 호출 간격 제어를 포함한 안정적인 FRED 데이터 수집기.
"""

import time
import warnings
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from fredapi import Fred

from config import LOOKBACK_DAYS, REQUEST_DELAY, IndicatorMeta

warnings.filterwarnings("ignore")


class FredClient:
    """FRED API 래퍼 — 재시도·레이트 리밋 제어를 내장합니다."""

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("FRED_API_KEY가 설정되지 않았습니다.")
        self._fred  = Fred(api_key=api_key)
        self._start = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    # ── Public API ───────────────────────────────────────────────────

    def fetch_series(
        self,
        fred_id: str,
        divisor: float = 1.0,
        label: str = "",
        retries: int = 3,
    ) -> pd.Series:
        """
        단일 FRED 시리즈를 수집합니다.

        Args:
            fred_id:  FRED 시리즈 ID (e.g. "WALCL")
            divisor:  단위 환산 제수  (million→billion: 1000)
            label:    출력 로그용 표시명
            retries:  최대 재시도 횟수

        Returns:
            DatetimeIndex를 가진 pd.Series, 실패 시 빈 Series
        """
        display = label or fred_id

        for attempt in range(retries):
            try:
                raw = self._fred.get_series(fred_id, observation_start=self._start)

                if raw is None or raw.dropna().empty:
                    raise ValueError("빈 시리즈 반환")

                series = (raw / divisor).dropna()
                series.index = pd.to_datetime(series.index)
                print(f"  ✅  {display:<36} [{fred_id}]  {len(series)} pts")
                time.sleep(REQUEST_DELAY)
                return series

            except Exception as exc:
                wait = 2 ** attempt          # 1 → 2 → 4 초
                if attempt < retries - 1:
                    print(
                        f"  ↺   [{fred_id}] 재시도 {attempt+1}/{retries} "
                        f"({wait}s 대기)… ({exc})"
                    )
                    time.sleep(wait)
                else:
                    print(f"  ❌  {display:<36} [{fred_id}]  최종 실패 → {exc}")
                    return pd.Series(dtype=float, name=fred_id)

    def fetch_batch(
        self,
        indicators: list[IndicatorMeta],
    ) -> dict[str, pd.Series]:
        """
        여러 지표를 순서대로 수집하여 {key: Series} 딕셔너리로 반환합니다.
        available=False 이거나 fred_id가 없는 지표는 건너뜁니다.
        """
        result: dict[str, pd.Series] = {}

        for meta in indicators:
            if not meta.available or not meta.fred_id:
                continue
            result[meta.key] = self.fetch_series(
                fred_id=meta.fred_id,
                divisor=meta.divisor,
                label=meta.name_ko,
            )

        return result

    # ── 실제 발표(갱신)일 조회 ───────────────────────────────────────

    def fetch_release_date(self, fred_id: str) -> Optional[str]:
        """
        해당 시리즈가 FRED에 실제로 마지막 반영된 날짜(발표일)를 가져옵니다.
        latest_date(관측 대상 기간)와 달리, 실제 데이터가 갱신된 시점입니다.
        """
        try:
            info = self._fred.get_series_info(fred_id)
            last_updated = info.get("last_updated")
            if not last_updated:
                return None
            time.sleep(REQUEST_DELAY)
            return str(last_updated).split(" ")[0]
        except Exception:
            return None

    def fetch_batch_release_dates(
        self, indicators: list[IndicatorMeta]
    ) -> dict[str, str]:
        """여러 지표의 발표일을 {key: 날짜} 딕셔너리로 반환합니다."""
        result: dict[str, str] = {}
        for meta in indicators:
            if not meta.available or not meta.fred_id:
                continue
            date = self.fetch_release_date(meta.fred_id)
            if date:
                result[meta.key] = date
        return result
