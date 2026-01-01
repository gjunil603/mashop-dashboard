from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
import requests

import time
import random

from zoneinfo import ZoneInfo

from .config import (
    DATA_DIR,
    DOCS_DIR,
    MAPS_JSON_PATH,
    INDEX_HTML_PATH,
    DEFAULT_DAYS_FOR_UI,
    DAYS_TO_FETCH,
    MIN_TRADECOUNT,
    HOUR_ORDER,
    POINTS_MAX_DAYS,
)
from .api import fetch_period
from .storage import load_maps_list, read_history, write_history, dump_raw
from .util import ensure_dir, parse_dt, weekday_kr, last_n_days_range
from .report import build_report_html
from mashop.config import KEEP_DAYS
from mashop.storage import trim_history_days


def _collect_recent_df(session: requests.Session, keyword: str, days_to_fetch: int) -> pd.DataFrame:
    start_date, end_date = last_n_days_range(days_to_fetch, include_today=True)
    rows = fetch_period(session, keyword, start_date, end_date)
    
    print(f"[FETCH] {keyword} start={start_date} end={end_date}") #로그
    
    dump_raw(keyword, f"{start_date}_to_{end_date}.json", rows)

    out = []
    for it in rows:
        dt_s = it.get("dateTime")
        if not dt_s:
            continue
        try:
            dt = parse_dt(str(dt_s))
        except Exception:
            continue

        price = it.get("price")
        tc = it.get("tradeCount")

        out.append(
            {
                "mapName": it.get("mapName", keyword),
                "dateTime": dt.strftime("%Y-%m-%dT%H:%M:%S"),
                "date": dt.strftime("%Y-%m-%d"),
                "time": dt.strftime("%H:%M"),
                "weekday": weekday_kr(dt),
                "price": float(price) if price is not None else None,
                "tradeCount": float(tc) if tc is not None else None,
                "timeUnit": it.get("timeUnit"),
            }
        )

    df = pd.DataFrame(out)
    if not df.empty:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["tradeCount"] = pd.to_numeric(df["tradeCount"], errors="coerce")
    return df


def _merge_history(old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    if old_df is None or old_df.empty:
        merged = new_df.copy()
    elif new_df is None or new_df.empty:
        merged = old_df.copy()
    else:
        merged = pd.concat([old_df, new_df], ignore_index=True)

    if merged.empty:
        return merged

    merged["dateTime"] = merged["dateTime"].astype(str)
    merged = merged.drop_duplicates(subset=["dateTime"], keep="last")
    merged = merged.sort_values(by=["dateTime"], ascending=True)

    merged["price"] = pd.to_numeric(merged["price"], errors="coerce")
    merged["tradeCount"] = pd.to_numeric(merged["tradeCount"], errors="coerce")

    # 누락된 date/time/weekday 보정(가능하면)
    def _fill_row(row):
        if (not row.get("date")) or (not row.get("time")) or (not row.get("weekday")):
            try:
                dt = parse_dt(row["dateTime"])
                row["date"] = row.get("date") or dt.strftime("%Y-%m-%d")
                row["time"] = row.get("time") or dt.strftime("%H:%M")
                row["weekday"] = row.get("weekday") or weekday_kr(dt)
            except Exception:
                pass
        return row

    merged = merged.apply(_fill_row, axis=1)
    return merged


def _format_price_kr(x: float | int | None) -> str:
    if x is None:
        return "-"
    try:
        v = float(x)
    except Exception:
        return "-"
    if math.isnan(v):
        return "-"
    if v < 0:
        v = abs(v)

    if v >= 100_000_000:
        eok = v / 100_000_000
        eok_r = round(eok, 1)
        if abs(eok_r - round(eok_r)) < 1e-9:
            return f"{int(round(eok_r))}억"
        return f"{eok_r}억"
    else:
        man = int(round(v / 10_000))
        return f"{man}만"


def build_daily_series(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    날짜별 원본 그래프:
    - 하루 = 1라인
    - x = HOUR_ORDER (01:00..23:00, 00:00)
    - hover = [time, price_str, trade_str, date, weekday]
    """
    if df is None or df.empty:
        return []

    sub = df.copy()
    sub["dt"] = pd.to_datetime(sub["dateTime"], errors="coerce")
    sub = sub.dropna(subset=["dt"])
    if sub.empty:
        return []

    sub["date"] = sub["dt"].dt.strftime("%Y-%m-%d")
    sub["time"] = sub["dt"].dt.strftime("%H:%M")

    packs = []
    for d, g in sub.groupby("date", as_index=False):
        g = g.sort_values("dt")
        last_by_time = g.drop_duplicates(subset=["time"], keep="last")

        price_map = {row["time"]: row["price"] for _, row in last_by_time.iterrows()}
        tc_map = {row["time"]: row.get("tradeCount") for _, row in last_by_time.iterrows()}
        wd_map = {row["time"]: row.get("weekday") for _, row in last_by_time.iterrows()}

        x = HOUR_ORDER[:]
        y = []
        hover = []
        for t in x:
            p = price_map.get(t, None)
            tc = tc_map.get(t, None)
            wd = wd_map.get(t, None) or "-"
            if p is None or (isinstance(p, float) and math.isnan(p)):
                y.append(None)
                hover.append([t, "-", "-", d, wd])
            else:
                y.append(float(p))
                trade_str = "-"
                if tc is not None and not (isinstance(tc, float) and math.isnan(tc)):
                    trade_str = f"{int(round(float(tc)))}"
                hover.append([t, _format_price_kr(float(p)), trade_str, d, wd])

        packs.append({"label": d, "x": x, "y": y, "hover": hover})

    packs.sort(key=lambda x: x["label"])
    return packs


def build_points(df: pd.DataFrame, max_days: int) -> List[Dict[str, Any]]:
    """
    요일 평균을 브라우저에서 계산하기 위한 점 데이터(가볍게).
    최근 max_days만 전달.
    """
    if df is None or df.empty:
        return []

    sub = df.copy()
    sub["dt"] = pd.to_datetime(sub["dateTime"], errors="coerce")
    sub = sub.dropna(subset=["dt"])
    if sub.empty:
        return []

    KST = ZoneInfo("Asia/Seoul")

    cutoff = datetime.now(KST).replace(tzinfo=None) - timedelta(days=max_days)
    sub = sub[sub["dt"] >= cutoff]
    if sub.empty:
        return []

    sub = sub.sort_values("dt").drop_duplicates(subset=["dateTime"], keep="last")
    sub["date"] = sub["dt"].dt.strftime("%Y-%m-%d")
    sub["time"] = sub["dt"].dt.strftime("%H:%M")

    pts = []
    for _, r in sub.iterrows():
        p = r.get("price")
        if p is None or (isinstance(p, float) and math.isnan(p)):
            continue
        tc = r.get("tradeCount")
        pts.append(
            {
                "date": str(r["date"]),
                "time": str(r["time"]),
                "weekday": str(r.get("weekday") or "-"),
                "price": float(p),
                "tradeCount": None
                if tc is None or (isinstance(tc, float) and math.isnan(tc))
                else float(tc),
            }
        )
    return pts


def main():
    # 폴더 준비
    ensure_dir(DATA_DIR)
    ensure_dir(DOCS_DIR)

    maps = load_maps_list(MAPS_JSON_PATH)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) mashop-dashboard/2.0",
            "Accept": "application/json",
        }
    )

    # 사냥터별로: 최근 N일 fetch -> merge -> save
    per_map_history: Dict[str, pd.DataFrame] = {}
    
    for i, kw in enumerate(maps):
        old_df = read_history(kw)
    
        try:
            new_df = _collect_recent_df(session, kw, DAYS_TO_FETCH)
        except Exception as e:
            print(f"[WARN] fetch failed: {kw} -> {e}")
            new_df = pd.DataFrame(columns=(old_df.columns if old_df is not None else []))
    
        merged = _merge_history(old_df, new_df)
        # ✅ 오래된 데이터 자동 정리 (최근 180일만 유지)
        merged = trim_history_days(merged, KEEP_DAYS)
        write_history(kw, merged)
        per_map_history[kw] = merged
    
        print(f"[OK] {kw}: rows={len(merged)} (fetched={len(new_df) if new_df is not None else 0})")
    
        # ✅ 다음 사냥터 요청 전 랜덤 딜레이 (마지막은 제외)
        if i < len(maps) - 1:
            delay = random.uniform(0.8, 1.8)
            print(f"    sleep {delay:.2f}s before next map")
            time.sleep(delay)

    # 대시보드에 넣을 데이터 구성
    daily_series: Dict[str, Any] = {}
    points: Dict[str, Any] = {}

    for kw in maps:
        df = per_map_history.get(kw)
        daily_series[kw] = build_daily_series(df)
        points[kw] = build_points(df, max_days=POINTS_MAX_DAYS)

    html = build_report_html(
        maps=maps,
        daily_series=daily_series,
        points=points,
        default_days=DEFAULT_DAYS_FOR_UI,
        min_trade=MIN_TRADECOUNT,
    )

    with open(INDEX_HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print("[DONE] generated")
    print(" -", INDEX_HTML_PATH)

