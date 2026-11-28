"""
data/imf_gold.py ─ IMF International Liquidity(IL) 국가별 중앙은행 금 보유량 → 누적 순매입(톤)
=====================================================================
IMF가 각국 신고를 받아 매달 갱신하는 금 보유량(RGV_REVS, 순금 트로이온스)을
키 없이 SDMX API로 받아, 국가별로 CB_GOLD_BASE_MONTH 말 보유량 대비 증감
(그 달 이전 기록이 없으면 첫 기록 대비)을 톤 단위로 만듭니다.
미보고 매입 추정치가 들어간 WGC 집계와는 다릅니다.

자체 점검: python -m data.imf_gold
"""

from __future__ import annotations

import time
import xml.etree.ElementTree as ET

import pandas as pd
import requests

import config_investment as inv_cfg

API_URL = "https://api.imf.org/external/sdmx/2.1/data/IMF.STA,IL,latest/{countries}.RGV_REVS.FTO.M"
OZ_PER_TONNE = 32_150.7466


def fetch_holdings(countries: list[str]) -> dict[str, pd.Series]:
    """국가 코드 → 월별 금 보유량(톤). IMF 서버가 가끔 늦게 응답해 3번까지 시도."""
    url = API_URL.format(countries="+".join(countries))
    for attempt in range(3):
        try:
            resp = requests.get(
                url, timeout=60,
                params={"startPeriod": "2000-01", "detail": "dataonly"},
                headers={"Accept": "application/vnd.sdmx.structurespecificdata+xml;version=2.1"},
            )
            resp.raise_for_status()
            break
        except Exception:
            if attempt == 2:
                raise
            time.sleep(15 * (attempt + 1))

    out = {}
    for series in ET.fromstring(resp.content).iter("Series"):
        obs = [(o.get("TIME_PERIOD"), o.get("OBS_VALUE")) for o in series.iter("Obs") if o.get("OBS_VALUE")]
        idx = pd.to_datetime([p.replace("-M", "-") + "-01" for p, _ in obs])   # "2026-M09" → 2026-09-01
        out[series.get("COUNTRY")] = pd.Series([float(v) for _, v in obs], index=idx).sort_index() / OZ_PER_TONNE
    return out


def build(holdings: pd.Series, base_month: str) -> pd.Series:
    """보유량(톤) → base_month 이후 누적 순매입."""
    s = holdings.dropna().sort_index()
    before = s[:base_month]
    base = before.iloc[-1] if not before.empty else s.iloc[0]
    return (s - base)[base_month:]


def load() -> dict[str, pd.Series]:
    """지표 키 → 시계열. 실패한 나라는 빈 시리즈."""
    groups = inv_cfg.CB_GOLD_GROUPS
    holdings = fetch_holdings(sorted({c for countries in groups.values() for c in countries}))
    raw = {}
    for prefix, countries in groups.items():
        for code in countries:
            key = f"{prefix}{code}"
            raw[key] = (build(holdings[code], inv_cfg.CB_GOLD_BASE_MONTH) if code in holdings
                        else pd.Series(dtype=float)).rename(key)
    return raw


if __name__ == "__main__":
    idx = pd.to_datetime(["2009-11-01", "2009-12-01", "2010-01-01", "2010-03-01", "2011-01-01"])
    cum = build(pd.Series([90, 100, 110, 120, 150], index=idx), "2009-12")
    assert list(cum) == [0, 10, 20, 50], list(cum)
    cum = build(pd.Series([300.0, 310.0], index=idx[2:4]), "2009-12")   # 기준월 이전 기록 없음 → 첫 기록 대비
    assert list(cum) == [0, 10], list(cum)
    print("ok")
