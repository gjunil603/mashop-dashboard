from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Tuple

import pandas as pd

from .config import MAPS_DIR
from .util import ensure_dir, windows_safe_slug


def load_maps_list(maps_json_path: str) -> List[str]:
    if not os.path.exists(maps_json_path):
        raise FileNotFoundError(f"{maps_json_path} 파일이 없습니다.")
    with open(maps_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError('maps.json 형식이 잘못되었습니다. 예: ["미나르숲:남겨진 용의 둥지", "..."]')

    maps = [str(x).strip() for x in data if str(x).strip()]
    if not maps:
        raise ValueError("maps.json이 비어있습니다. 맵 이름을 1개 이상 넣어주세요.")
    return maps


def map_paths(keyword: str) -> Tuple[str, str, str]:
    """
    returns (map_dir, history_csv_path, raw_dump_dir)
    """
    slug = windows_safe_slug(keyword)
    map_dir = os.path.join(MAPS_DIR, slug)
    history_path = os.path.join(map_dir, "history.csv")
    raw_dump_dir = os.path.join(map_dir, "raw_dump")
    return map_dir, history_path, raw_dump_dir


def read_history(keyword: str) -> pd.DataFrame:
    _, history_path, _ = map_paths(keyword)
    if not os.path.exists(history_path):
        return pd.DataFrame(columns=["dateTime", "date", "time", "weekday", "price", "tradeCount", "timeUnit", "mapName"])

    try:
        df = pd.read_csv(history_path, encoding="utf-8")
    except Exception:
        df = pd.read_csv(history_path, encoding="utf-8-sig")

    # 컬럼 방어
    for col in ["dateTime", "date", "time", "weekday", "price", "tradeCount", "timeUnit", "mapName"]:
        if col not in df.columns:
            df[col] = None

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["tradeCount"] = pd.to_numeric(df["tradeCount"], errors="coerce")
    df["dateTime"] = df["dateTime"].astype(str)
    return df


def write_history(keyword: str, df: pd.DataFrame) -> None:
    map_dir, history_path, raw_dump_dir = map_paths(keyword)
    ensure_dir(map_dir)
    ensure_dir(raw_dump_dir)
    df.to_csv(history_path, index=False, encoding="utf-8")


def dump_raw(keyword: str, filename: str, obj: Any) -> None:
    map_dir, _, raw_dump_dir = map_paths(keyword)
    ensure_dir(map_dir)
    ensure_dir(raw_dump_dir)
    path = os.path.join(raw_dump_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def _ensure_datetime_col(df: pd.DataFrame, col: str = "dateTime") -> pd.Series:
    """
    df[col]을 pandas datetime으로 안전하게 변환한 Series 반환
    """
    if df is None or df.empty or col not in df.columns:
        return pd.Series([], dtype="datetime64[ns]")
    return pd.to_datetime(df[col], errors="coerce")


def trim_history_days(df: pd.DataFrame, keep_days: int, col: str = "dateTime") -> pd.DataFrame:
    """
    history DataFrame을 최근 keep_days만 남기도록 정리
    - 기준 시점: df의 가장 최신 dateTime (현재시간 기준이 아니라 데이터 기준)
    - dateTime 파싱 실패 행은 제거(안전)
    """
    if df is None or df.empty:
        return df

    if col not in df.columns:
        return df

    ts = _ensure_datetime_col(df, col)
    if ts.empty:
        return df.iloc[0:0].copy()

    # 파싱 실패(NaT) 제거
    valid_mask = ts.notna()
    df2 = df.loc[valid_mask].copy()
    ts2 = ts.loc[valid_mask]

    if df2.empty:
        return df2

    latest = ts2.max()
    cutoff = latest - pd.Timedelta(days=int(keep_days))

    keep_mask = ts2 >= cutoff
    df3 = df2.loc[keep_mask].copy()

    # 정렬(있으면 안정적)
    df3[col] = pd.to_datetime(df3[col], errors="coerce")
    df3 = df3.sort_values(col).reset_index(drop=True)
    return df3
