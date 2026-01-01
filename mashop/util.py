from __future__ import annotations

import hashlib
import os
import re
from datetime import date, timedelta, datetime
from typing import Tuple

from zoneinfo import ZoneInfo

from .config import WEEKDAY_KR

KST = ZoneInfo("Asia/Seoul")

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def windows_safe_slug(name: str, max_len: int = 80) -> str:
    """
    윈도우에서도 안전한 폴더명:
    - 금지문자: \ / : * ? " < > |
    - 공백/탭 등은 _
    - _ 연속은 하나로
    - 너무 짧거나 비면 해시 사용
    """
    s = (name or "").strip()

    # 윈도우 금지 문자/제어 문자 제거
    s = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", s)
    # 공백류 -> _
    s = re.sub(r"\s+", "_", s)
    # 연속 _ 정리
    s = re.sub(r"_+", "_", s).strip("_")

    if not s:
        h = hashlib.md5((name or "").encode("utf-8")).hexdigest()[:10]
        s = f"map_{h}"

    if len(s) > max_len:
        h = hashlib.md5((name or "").encode("utf-8")).hexdigest()[:10]
        s = s[: max_len - 11].rstrip("_") + "_" + h

    return s


def parse_dt(s: str) -> datetime:
    """
    mashop API dateTime 파싱 규칙:
    - timezone 정보가 없는 ISO 문자열은 KST로 간주한다. (사이트 표시와 일치)
    - timezone(Z 또는 +09:00 등)가 있으면 그걸 존중하고 KST로 변환한다.
    - 반환은 'KST 기준 naive datetime' (tzinfo 제거)로 통일한다.
    """
    s = (s or "").strip()

    # 1) 가장 흔한 ISO 형태 처리
    #    Python fromisoformat은 'Z'를 직접 못 먹어서 +00:00으로 치환
    s2 = s.replace("Z", "+00:00")

    dt = datetime.fromisoformat(s2)

    # 2) tzinfo가 없으면 KST로 붙이기
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    else:
        # tz가 있으면 KST로 변환
        dt = dt.astimezone(KST)

    # 3) 내부 저장/비교는 naive(KST)로 통일
    return dt.replace(tzinfo=None)


def weekday_kr(dt: datetime) -> str:
    return WEEKDAY_KR[dt.weekday()]


def last_n_days_range(days: int, include_today: bool = True) -> tuple[str, str]:
    """
    KST(한국시간) 기준으로 start/end 날짜(YYYY-MM-DD)를 반환.

    - include_today=True:
        end = 오늘(KST)
        start = 오늘(KST) - days
      예) days=7 -> 8일치 범위가 될 수 있음(시작/끝 포함)

    - include_today=False:
        end = 어제(KST)
        start = 어제(KST) - (days-1)

    반환값은 API 파라미터에 그대로 사용.
    """
    today_kst: date = datetime.now(KST).date()

    if include_today:
        end = today_kst
        start = today_kst - timedelta(days=int(days))
    else:
        end = today_kst - timedelta(days=1)
        start = end - timedelta(days=max(int(days) - 1, 0))

    return start.isoformat(), end.isoformat()

