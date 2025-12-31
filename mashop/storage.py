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

