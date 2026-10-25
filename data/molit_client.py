"""
data/molit_client.py ─ 국토교통부 아파트 매매 실거래가 API → 지역별 월간 84㎡ 환산 중위가격
=====================================================================
API는 '한 지역 × 한 달' 단위로만 조회되므로, 2020년부터의 과거 집계는
data/kr_apt_median.csv에 저장해 두고 매 실행 때는 최근 몇 달만 다시 받습니다
(신고 기한이 30일이라 최근 달 값은 한동안 계속 바뀜).

과거 집계 파일 다시 만들기 (수동, 약 1,300회 호출):
  python -m data.molit_client
"""

from __future__ import annotations

import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from statistics import median

import pandas as pd
import requests

import config_korea as kr_cfg

API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
SEED_CSV = Path(__file__).with_name("kr_apt_median.csv")
REFRESH_MONTHS = 3   # 집계 파일 마지막 달 기준 몇 달 전부터 다시 받을지


def fetch_median_84(api_key: str, lawd_cd: str, ym: str) -> float | None:
    """한 지역·한 달 거래의 ㎡당 가격 중위값 × 84 (억원). 해제된 거래는 제외, 거래 없으면 None."""
    for attempt in range(3):
        try:
            resp = requests.get(API_URL, timeout=30, params={
                "serviceKey": api_key, "LAWD_CD": lawd_cd, "DEAL_YMD": ym,
                "numOfRows": 9999, "pageNo": 1,
            })
            root = ET.fromstring(resp.content)
            break
        except (requests.RequestException, ET.ParseError):
            if attempt == 2:
                raise
            time.sleep(5 * (attempt + 1))
    if root.findtext(".//resultCode") not in ("000", "00"):
        raise RuntimeError(root.findtext(".//resultMsg"))

    per_m2 = []
    for it in root.iter("item"):
        if (it.findtext("cdealType") or "").strip() == "O":   # 계약 해제
            continue
        amount = float(it.findtext("dealAmount").replace(",", ""))   # 만원
        area = float(it.findtext("excluUseAr"))                       # 전용 ㎡
        per_m2.append(amount / area)
    return median(per_m2) * 84 / 10_000 if per_m2 else None   # 만원 → 억원


def fetch_months(api_key: str, months: list[str]) -> pd.DataFrame:
    """months × 모든 지역 집계 (index=월초 날짜, columns=지표 키)."""
    rows = {}
    for ym in months:
        rows[pd.Timestamp(f"{ym[:4]}-{ym[4:]}-01")] = {
            key: fetch_median_84(api_key, lawd, ym) for key, lawd in kr_cfg.MOLIT_REGIONS.items()
        }
    return pd.DataFrame.from_dict(rows, orient="index")


def _month_range(start: pd.Timestamp, end: pd.Timestamp) -> list[str]:
    return [d.strftime("%Y%m") for d in pd.date_range(start, end, freq="MS")]


def load(api_key: str) -> pd.DataFrame:
    """저장된 과거 집계 + 최근 몇 달 재조회. 이번 달은 거래 신고가 덜 모여 제외."""
    seed = pd.read_csv(SEED_CSV, index_col=0, parse_dates=True)
    last_full = pd.Timestamp.now().normalize().replace(day=1) - pd.offsets.MonthBegin(1)
    # ponytail: 재조회 구간이 집계 파일 이후로 계속 늘어남 — 1년쯤 지나면 `python -m data.molit_client`로 파일 갱신
    start = seed.index.max() - pd.offsets.MonthBegin(REFRESH_MONTHS - 1)
    recent = fetch_months(api_key, _month_range(start, last_full))
    return recent.combine_first(seed).sort_index()


if __name__ == "__main__":
    import config  # noqa: F401  (.env → os.environ)
    end = pd.Timestamp.now().normalize().replace(day=1) - pd.offsets.MonthBegin(1)
    df = fetch_months(os.environ["MOLIT_API_KEY"], _month_range(pd.Timestamp(kr_cfg.MOLIT_START), end))
    df.round(4).to_csv(SEED_CSV)
    print(f"saved {SEED_CSV} ({len(df)} months × {len(df.columns)} regions)")
