"""
data/imf_gold.py ─ IMF 국가별 중앙은행 금 보유량 → 순매입 상위 N개국 누적 순매입 · GDP 상위 N개국 보유량(톤)
=====================================================================
IMF가 각국 신고를 받아 매달 갱신하는 금 보유량(International Liquidity, RGV_REVS,
순금 트로이온스)을 키 없이 SDMX API로 전 세계 한 번에 받아 톤으로 바꾸고, 매 실행마다
  · 순매입 상위 N개국: CB_GOLD_BASE_MONTH 말 대비 증감이 큰 순서 (국제기구·지역 합계 제외)
  · GDP 상위 N개국: IMF 세계경제전망(WEO) 지난해 명목 달러 GDP 순서 (조회 실패 시 고정 목록)
을 새로 뽑습니다. 지표 키는 나라가 아니라 순위 자리(CB_GOLD_CUM_1 …)라서, 순위가 바뀌어도
설정을 고칠 필요가 없습니다. 미보고 매입 추정치가 들어간 WGC 집계와는 다릅니다.

자체 점검: python -m data.imf_gold
"""

from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from datetime import datetime

import pandas as pd
import requests

import config_investment as inv_cfg

BASE_URL = "https://api.imf.org/external/sdmx/2.1/data"
GOLD_URL = BASE_URL + "/IMF.STA,IL,latest/.RGV_REVS.FTO.M"    # 앞자리(나라)를 비우면 전 세계
GDP_URL = BASE_URL + "/IMF.RES,WEO,latest/.NGDPD.A"           # 명목 GDP (달러)
OZ_PER_TONNE = 32_150.7466


def _get_sdmx(url: str, params: dict) -> ET.Element:
    """IMF 서버가 가끔 늦게 응답해 3번까지 시도."""
    for attempt in range(3):
        try:
            resp = requests.get(
                url, timeout=90, params={**params, "detail": "dataonly"},
                headers={"Accept": "application/vnd.sdmx.structurespecificdata+xml;version=2.1"},
            )
            resp.raise_for_status()
            return ET.fromstring(resp.content)
        except Exception:
            if attempt == 2:
                raise
            time.sleep(15 * (attempt + 1))


def fetch_holdings() -> dict[str, pd.Series]:
    """국가 코드 → 월별 금 보유량(톤), 전 세계."""
    out = {}
    for series in _get_sdmx(GOLD_URL, {"startPeriod": "2000-01"}).iter("Series"):
        obs = [(o.get("TIME_PERIOD"), o.get("OBS_VALUE")) for o in series.iter("Obs") if o.get("OBS_VALUE")]
        idx = pd.to_datetime([p.replace("-M", "-") + "-01" for p, _ in obs])   # "2026-M09" → 2026-09-01
        out[series.get("COUNTRY")] = pd.Series([float(v) for _, v in obs], index=idx).sort_index() / OZ_PER_TONNE
    return out


def fetch_gdp_ranking(year: int) -> list[str]:
    """그 해 명목 GDP가 큰 순서의 국가 코드."""
    gdp = {}
    for series in _get_sdmx(GDP_URL, {"startPeriod": year, "endPeriod": year}).iter("Series"):
        for o in series.iter("Obs"):
            if o.get("OBS_VALUE"):
                gdp[series.get("COUNTRY")] = float(o.get("OBS_VALUE"))
    return sorted(gdp, key=gdp.get, reverse=True)


def is_country(code: str) -> bool:
    """G001(세계)·G163(유로존) 같은 합계와 BIS·IMF 같은 기관을 걸러냄."""
    return len(code) == 3 and code.isalpha() and code not in inv_cfg.CB_GOLD_NON_COUNTRIES


def build(holdings: pd.Series, base_month: str) -> pd.Series:
    """보유량(톤) → base_month 이후 누적 순매입 (그 달 이전 기록이 없으면 첫 기록 대비)."""
    s = holdings.dropna().sort_index()
    before = s[:base_month]
    base = before.iloc[-1] if not before.empty else s.iloc[0]
    return (s - base)[base_month:]


def top_buyers(holdings: dict[str, pd.Series], base_month: str, n: int) -> list[tuple[str, pd.Series]]:
    """누적 순매입 마지막 값이 큰 순서로 n개국 (코드, 누적 시계열)."""
    cums = [(c, build(s, base_month)) for c, s in holdings.items() if is_country(c) and not s.dropna().empty]
    cums = [(c, s) for c, s in cums if not s.empty]
    return sorted(cums, key=lambda cs: cs[1].iloc[-1], reverse=True)[:n]


def top_economies(holdings: dict[str, pd.Series], ranking: list[str], n: int) -> list[str]:
    """GDP 순위에서 금 보유량 기록이 있는 나라만 앞에서 n개."""
    return [c for c in ranking if is_country(c) and c in holdings][:n]


def load() -> tuple[dict[str, pd.Series], dict[str, str]]:
    """(지표 키 → 시계열, 지표 키 → 그 자리에 뽑힌 나라 이름)."""
    base, n = inv_cfg.CB_GOLD_BASE_MONTH, inv_cfg.CB_GOLD_TOP_N
    holdings = fetch_holdings()
    try:
        ranking = fetch_gdp_ranking(datetime.now().year - 1)
    except Exception as exc:
        print(f"  ⚠️  IMF WEO GDP 순위 조회 실패 → 고정 목록 사용 ({exc})")
        ranking = inv_cfg.CB_GOLD_GDP_FALLBACK

    raw, names = {}, {}
    for rank, (code, cum) in enumerate(top_buyers(holdings, base, n), start=1):
        raw[f"CB_GOLD_CUM_{rank}"] = cum
        names[f"CB_GOLD_CUM_{rank}"] = inv_cfg.CB_GOLD_NAMES.get(code, code)
    for rank, code in enumerate(top_economies(holdings, ranking, n), start=1):
        raw[f"CB_GOLD_GDP_{rank}"] = holdings[code].dropna().sort_index()[base:]
        names[f"CB_GOLD_GDP_{rank}"] = inv_cfg.CB_GOLD_NAMES.get(code, code)
    return {k: s.rename(k) for k, s in raw.items()}, names


if __name__ == "__main__":
    idx = pd.to_datetime(["2009-11-01", "2009-12-01", "2010-01-01", "2010-03-01", "2011-01-01"])
    cum = build(pd.Series([90, 100, 110, 120, 150], index=idx), "2009-12")
    assert list(cum) == [0, 10, 20, 50], list(cum)
    cum = build(pd.Series([300.0, 310.0], index=idx[2:4]), "2009-12")   # 기준월 이전 기록 없음 → 첫 기록 대비
    assert list(cum) == [0, 10], list(cum)

    s = lambda *v: pd.Series(v, index=idx[1:1 + len(v)])
    h = {"AAA": s(100, 150), "BBB": s(100, 300), "CCC": s(100, 90), "G001": s(0, 999), "IMF": s(0, 999), "DDD": s(200, 260)}
    assert [c for c, _ in top_buyers(h, "2009-12", 3)] == ["BBB", "DDD", "AAA"]   # 합계·기관 제외, 증가폭 순
    assert top_economies(h, ["ZZZ", "G001", "CCC", "AAA", "BBB"], 2) == ["CCC", "AAA"]   # 금 기록 없는 나라·합계 건너뜀
    print("ok")
