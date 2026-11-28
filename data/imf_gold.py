"""
data/imf_gold.py ─ IMF International Liquidity(IL) 국가별 중앙은행 금 보유량 → 누적·12개월 순매입(톤)
=====================================================================
IMF가 각국 신고를 받아 매달 갱신하는 금 보유량(RGV_REVS, 순금 트로이온스)을
키 없이 SDMX API로 받아, 국가별로
  · 누적 순매입: GOLD_BASE_MONTH 말 보유량 대비 증감 (그 달 이전 기록이 없으면 첫 기록 대비)
  · 12개월 순매입: 1년 전 같은 달 보유량 대비 증감
을 톤 단위로 만듭니다. 미보고 매입 추정치가 들어간 WGC 집계와는 다릅니다.

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


def build(holdings: pd.Series, base_month: str) -> tuple[pd.Series, pd.Series]:
    """보유량(톤) → (누적 순매입, 12개월 순매입). 둘 다 base_month 이후 구간만."""
    s = holdings.dropna().sort_index()
    before = s[:base_month]
    base = before.iloc[-1] if not before.empty else s.iloc[0]
    cum = (s - base)[base_month:]
    # 줄 수가 아닌 날짜 기준 12개월 — 중간에 빠진 달이 있어도 정확히 1년 전과 비교
    yoy = (s - s.shift(freq=pd.DateOffset(months=12)).reindex(s.index)).dropna()[base_month:]
    return cum, yoy


def load() -> dict[str, pd.Series]:
    """지표 키 → 시계열. 실패한 나라는 빈 시리즈."""
    holdings = fetch_holdings(list(inv_cfg.CB_GOLD_COUNTRIES))
    raw = {}
    for code in inv_cfg.CB_GOLD_COUNTRIES:
        cum_key, yoy_key = f"CB_GOLD_CUM_{code}", f"CB_GOLD_12M_{code}"
        if code in holdings:
            cum, yoy = build(holdings[code], inv_cfg.CB_GOLD_BASE_MONTH)
            raw[cum_key], raw[yoy_key] = cum.rename(cum_key), yoy.rename(yoy_key)
        else:
            raw[cum_key] = pd.Series(dtype=float, name=cum_key)
            raw[yoy_key] = pd.Series(dtype=float, name=yoy_key)
    return raw


if __name__ == "__main__":
    idx = pd.to_datetime(["2009-11-01", "2009-12-01", "2010-01-01", "2010-03-01", "2011-01-01", "2011-03-01"])
    cum, yoy = build(pd.Series([90, 100, 110, 120, 150, 125], index=idx), "2009-12")
    assert list(cum) == [0, 10, 20, 50, 25], list(cum)
    assert list(yoy) == [40, 5], list(yoy)          # 2011-01 vs 2010-01, 2011-03 vs 2010-03 (2010-02 결측이어도 정확)
    cum, _ = build(pd.Series([300.0, 310.0], index=idx[2:4]), "2009-12")   # 기준월 이전 기록 없음 → 첫 기록 대비
    assert list(cum) == [0, 10], list(cum)
    print("ok")
