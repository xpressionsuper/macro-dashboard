"""
data/derived.py ─ 파생 지표 계산기
=====================================================================
FRED 원시 데이터에서 합성/파생 시계열을 생성합니다.

파생 지표 목록:
  NET_LIQ   : 넷 유동성 = BS − RRP − TGA
  SOFR_SPR  : SOFR − IORB 스프레드 (bps)
  M2_YOY    : M2 전년비 증가율 (%)
"""

import numpy as np
import pandas as pd


class DerivedCalculator:
    """원시 시계열 딕셔너리를 받아 파생 지표를 계산합니다."""

    def __init__(self, raw: dict[str, pd.Series]):
        """
        Args:
            raw: FredClient + TreasuryClient가 반환한 {key: Series} 딕셔너리.
        """
        self._raw = raw

    # ── Public API ───────────────────────────────────────────────────

    def compute(self) -> dict[str, pd.Series]:
        """
        모든 파생 지표를 계산하여 {key: Series} 딕셔너리로 반환합니다.
        입력 데이터가 부족한 지표는 빈 Series를 넣습니다.
        """
        derived: dict[str, pd.Series] = {}

        derived["NET_LIQ"]  = self._net_liquidity()
        derived["SOFR_SPR"] = self._sofr_spread()
        derived["M2_YOY"]   = self._m2_yoy()

        return derived

    # ── Private: 개별 파생 지표 ──────────────────────────────────────

    def _net_liquidity(self) -> pd.Series:
        """
        넷 유동성 = Fed 대차대조표(BS) − 역레포(RRP) − TGA
        세 시리즈를 공통 날짜 기준으로 정렬 후 빼줍니다.
        """
        bs  = self._raw.get("BS",  pd.Series(dtype=float))
        rrp = self._raw.get("RRP", pd.Series(dtype=float))
        tga = self._raw.get("TGA", pd.Series(dtype=float))

        if bs.empty or rrp.empty:
            return pd.Series(dtype=float, name="NET_LIQ")

        # 주별·일별 혼합 대응: 공통 날짜로 reindex 후 forward-fill
        df = pd.DataFrame({"BS": bs, "RRP": rrp, "TGA": tga}).sort_index()
        df = df.ffill().dropna(subset=["BS", "RRP"])

        if tga.empty:
            # TGA 수집 실패 시 BS−RRP만 계산
            net = (df["BS"] - df["RRP"]).rename("NET_LIQ")
        else:
            df = df.dropna(subset=["TGA"])
            net = (df["BS"] - df["RRP"] - df["TGA"]).rename("NET_LIQ")

        return net

    def _sofr_spread(self) -> pd.Series:
        """
        SOFR − IORB 스프레드 (단위: bps = ×100)
        두 시리즈를 공통 날짜로 정렬 후 차이를 구합니다.
        """
        sofr = self._raw.get("SOFR", pd.Series(dtype=float))
        iorb = self._raw.get("IORB", pd.Series(dtype=float))

        if sofr.empty or iorb.empty:
            return pd.Series(dtype=float, name="SOFR_SPR")

        df  = pd.DataFrame({"SOFR": sofr, "IORB": iorb}).dropna()
        spr = ((df["SOFR"] - df["IORB"]) * 100).rename("SOFR_SPR")  # % → bps
        return spr

    def _m2_yoy(self) -> pd.Series:
        """
        M2 전년비 증가율 (%).
        M2SL은 월간 데이터이므로 12개월 전 대비 % 변화.
        """
        m2 = self._raw.get("M2", pd.Series(dtype=float))

        if m2.empty or len(m2) < 13:
            return pd.Series(dtype=float, name="M2_YOY")

        yoy = m2.pct_change(12).mul(100).dropna().rename("M2_YOY")
        return yoy
