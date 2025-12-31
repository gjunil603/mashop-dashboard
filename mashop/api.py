from __future__ import annotations

from typing import Any, Dict, List
import requests

from .config import API_BASE


def fetch_period(session: requests.Session, keyword: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """
    GET /api/v2/maps/price-stat/period?keyword=...&startDate=...&endDate=...
    """
    url = f"{API_BASE}/api/v2/maps/price-stat/period"
    params = {"keyword": keyword, "startDate": start_date, "endDate": end_date}
    r = session.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for k in ("data", "items", "result", "content"):
            if k in data and isinstance(data[k], list):
                return data[k]

    return []

