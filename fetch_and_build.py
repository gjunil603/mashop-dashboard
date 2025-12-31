# fetch_and_build.py
from __future__ import annotations

import os
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd


# =========================
# 설정값
# =========================
API_BASE = "https://api.mashop.kr"

# 리포트(대시보드)에서 분석에 쓸 기간(최근 N일)
DAYS_FOR_REPORT = int(os.environ.get("DAYS_FOR_REPORT", "14"))

# 데이터 누적 저장(히스토리) 시, 매 실행마다 최근 N일을 가져와 합치는 방식
# (너무 길게 가져오면 느려지니 적당히)
DAYS_TO_FETCH = int(os.environ.get("DAYS_TO_FETCH", "30"))

# 거래량 필터(평균 거래량이 너무 적으면 가격 튐이 심해서 제외)
MIN_TRADECOUNT = float(os.environ.get("MIN_TRADECOUNT", "5"))

# 타임존: mashop API의 dateTime이 한국시간 형태로 오는 것으로 가정(문자열에 TZ 없음)
# (실제론 서버/브라우저에 따라 다를 수 있으나, 네 테스트 데이터 기준 KST로 맞춰 처리)
WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]

DATA_DIR = "data"
DOCS_DIR = "docs"
MAPS_JSON_PATH = "maps.json"
HISTORY_CSV_PATH = os.path.join(DATA_DIR, "history.csv")
INDEX_HTML_PATH = os.path.join(DOCS_DIR, "index.html")
RAW_DUMP_DIR = os.path.join(DATA_DIR, "raw_dump")

# 사이트 차트처럼 "01:00 ~ 23:00, 00:00" 순서로 보이게
HOUR_ORDER = [f"{h:02d}:00" for h in range(1, 24)] + ["00:00"]


# =========================
# 유틸
# =========================
def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(DOCS_DIR, exist_ok=True)
    os.makedirs(RAW_DUMP_DIR, exist_ok=True)


def load_maps() -> List[str]:
    if not os.path.exists(MAPS_JSON_PATH):
        raise FileNotFoundError(f"{MAPS_JSON_PATH} 파일이 없습니다. maps.json을 먼저 추가하세요.")
    with open(MAPS_JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        maps = [str(x).strip() for x in data if str(x).strip()]
        if not maps:
            raise ValueError("maps.json이 비어있습니다. 맵 이름을 1개 이상 넣어주세요.")
        return maps
    raise ValueError("maps.json 형식이 잘못되었습니다. 예: [\"미나르숲:남겨진 용의 둥지\", \"...\" ]")


def parse_dt_kst(dt_str: str) -> datetime:
    # 예: "2025-12-31T01:00:00"
    # TZ 정보 없음 -> 한국시간으로 간주(naive)
    return datetime.fromisoformat(dt_str)


def to_date_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def to_time_str(dt: datetime) -> str:
    return dt.strftime("%H:%M")


def weekday_kr(dt: datetime) -> str:
    return WEEKDAY_KR[dt.weekday()]


def format_price_kr(x: float | int | None) -> str:
    """
    7,000,000 -> 700만
    17,000,000 -> 1700만
    120,000,000 -> 1.2억
    100,000,000 -> 1억
    """
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
        if abs(eok - round(eok)) < 1e-9:
            return f"{int(round(eok))}억"
        # 소수 1자리까지만
        return f"{eok:.1f}억".rstrip("0").rstrip(".") + "억" if not str(eok).endswith("0") else f"{eok:.1f}억"
    else:
        man = int(round(v / 10_000))
        return f"{man}만"


def safe_json_dump(path: str, obj: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


# =========================
# API 수집
# =========================
def fetch_period(keyword: str, start_date: str, end_date: str, session: Optional[requests.Session] = None) -> List[Dict[str, Any]]:
    """
    GET /api/v2/maps/price-stat/period?keyword=...&startDate=YYYY-MM-DD&endDate=YYYY-MM-DD
    """
    s = session or requests.Session()
    url = f"{API_BASE}/api/v2/maps/price-stat/period"
    params = {
        "keyword": keyword,
        "startDate": start_date,
        "endDate": end_date,
    }
    r = s.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    # 보통 list 형태. 만약 dict 형태면 내부 키 탐색
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # 흔한 케이스 방어
        for k in ["data", "items", "result", "content"]:
            if k in data and isinstance(data[k], list):
                return data[k]
    return []


def last_n_days_range(n: int, include_today: bool = True) -> Tuple[str, str]:
    """
    - n=7이면: 오늘 포함해서 최근 7일 범위
    - start는 'n일-1일 전' 00:00 기준으로 API에 날짜만 던짐
    """
    today = date.today()
    if include_today:
        start = today - timedelta(days=n - 1)
        end = today
    else:
        start = today - timedelta(days=n)
        end = today - timedelta(days=1)
    return start.isoformat(), end.isoformat()


def collect_recent(keyword: str, days_to_fetch: int, session: requests.Session) -> pd.DataFrame:
    start_date, end_date = last_n_days_range(days_to_fetch, include_today=True)
    rows = fetch_period(keyword, start_date, end_date, session=session)

    # raw dump 저장(디버깅용)
    dump_path = os.path.join(RAW_DUMP_DIR, f"{keyword}_{start_date}_to_{end_date}.json")
    safe_json_dump(dump_path, rows)

    out = []
    for it in rows:
        dt_s = it.get("dateTime")
        price = it.get("price")
        tc = it.get("tradeCount")
        if not dt_s:
            continue
        try:
            dt = parse_dt_kst(str(dt_s))
        except Exception:
            continue

        out.append({
            "keyword": keyword,
            "mapName": it.get("mapName", keyword),
            "dateTime": dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "date": to_date_str(dt),
            "time": to_time_str(dt),
            "weekday": weekday_kr(dt),
            "price": float(price) if price is not None else None,
            "tradeCount": float(tc) if tc is not None else None,
            "timeUnit": it.get("timeUnit"),
        })

    df = pd.DataFrame(out)
    return df


# =========================
# 히스토리 누적/정리
# =========================
def load_history() -> pd.DataFrame:
    if not os.path.exists(HISTORY_CSV_PATH):
        return pd.DataFrame(columns=["keyword", "mapName", "dateTime", "date", "time", "weekday", "price", "tradeCount", "timeUnit"])
    try:
        df = pd.read_csv(HISTORY_CSV_PATH, encoding="utf-8")
    except Exception:
        df = pd.read_csv(HISTORY_CSV_PATH, encoding="utf-8-sig")

    # 필드 보정
    for col in ["keyword", "dateTime", "date", "time", "weekday", "mapName", "timeUnit"]:
        if col not in df.columns:
            df[col] = None
    for col in ["price", "tradeCount"]:
        if col not in df.columns:
            df[col] = None
    return df


def merge_history(old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    if old_df is None or old_df.empty:
        merged = new_df.copy()
    elif new_df is None or new_df.empty:
        merged = old_df.copy()
    else:
        merged = pd.concat([old_df, new_df], ignore_index=True)

    # 중복 제거: keyword + dateTime 기준
    merged["keyword"] = merged["keyword"].astype(str)
    merged["dateTime"] = merged["dateTime"].astype(str)
    merged = merged.drop_duplicates(subset=["keyword", "dateTime"], keep="last")

    # 정렬
    merged = merged.sort_values(by=["keyword", "dateTime"], ascending=[True, True])

    # NaN 보정
    if "tradeCount" in merged.columns:
        merged["tradeCount"] = pd.to_numeric(merged["tradeCount"], errors="coerce")
    merged["price"] = pd.to_numeric(merged["price"], errors="coerce")

    return merged


def save_history(df: pd.DataFrame):
    df.to_csv(HISTORY_CSV_PATH, index=False, encoding="utf-8")


# =========================
# 리포트(대시보드) 생성
# =========================
@dataclass
class SeriesPack:
    label: str               # 날짜 라벨
    x: List[str]             # 시간축 (HOUR_ORDER)
    y: List[Optional[float]] # 가격
    hover: List[List[str]]   # customdata: [time, price_str, trade_str, date, weekday]


def build_series_for_kw(hist: pd.DataFrame, kw: str, days_for_report: int) -> List[SeriesPack]:
    """
    kw에 대해 최근 N일의 날짜별 라인(각 날짜가 한 라인) 생성
    """
    sub = hist[hist["keyword"] == kw].copy()
    if sub.empty:
        return []

    # 최근 N일로 제한
    sub["dt"] = pd.to_datetime(sub["dateTime"], errors="coerce")
    sub = sub.dropna(subset=["dt"])
    cutoff = datetime.now() - timedelta(days=days_for_report)
    sub = sub[sub["dt"] >= cutoff]
    if sub.empty:
        return []

    # 날짜별로 분리
    sub["date"] = sub["dt"].dt.strftime("%Y-%m-%d")
    sub["time"] = sub["dt"].dt.strftime("%H:%M")
    sub["weekday"] = sub["dt"].dt.weekday.map(lambda i: WEEKDAY_KR[int(i)])

    packs: List[SeriesPack] = []
    for d, g in sub.groupby("date", as_index=False):
        # 시간별 하나로 맞추기 (중복이 있으면 마지막 값)
        g = g.sort_values("dt")
        last_by_time = g.drop_duplicates(subset=["time"], keep="last")

        # dict로 빠르게 매핑
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
                hover.append([
                    t,
                    format_price_kr(float(p)),
                    str(int(tc)) if tc is not None and not (isinstance(tc, float) and math.isnan(tc)) else "-",
                    d,
                    wd
                ])

        packs.append(SeriesPack(label=d, x=x, y=y, hover=hover))

    # 날짜 정렬(오래된 -> 최신)
    packs.sort(key=lambda p: p.label)
    return packs


def compute_weekday_extrema(hist: pd.DataFrame, kw: str, days_for_report: int) -> List[Dict[str, Any]]:
    """
    최근 N일 동안:
    요일(월~일)별로 시간대 평균 가격(avg_price)을 만들고
    - 최고가 시간 (avg_price 최대)
    - 최저가 시간 (avg_price 최소)
    """
    sub = hist[hist["keyword"] == kw].copy()
    if sub.empty:
        return []

    sub["dt"] = pd.to_datetime(sub["dateTime"], errors="coerce")
    sub = sub.dropna(subset=["dt"])
    cutoff = datetime.now() - timedelta(days=days_for_report)
    sub = sub[sub["dt"] >= cutoff]
    if sub.empty:
        return []

    sub["weekday"] = sub["dt"].dt.weekday.map(lambda i: WEEKDAY_KR[int(i)])
    sub["time"] = sub["dt"].dt.strftime("%H:%M")

    # 그룹 평균
    g = sub.groupby(["weekday", "time"], as_index=False).agg(
        avg_price=("price", "mean"),
        avg_trade=("tradeCount", "mean"),
        n=("price", "count")
    )
    g["avg_price"] = pd.to_numeric(g["avg_price"], errors="coerce")
    g["avg_trade"] = pd.to_numeric(g["avg_trade"], errors="coerce").fillna(0)

    # 거래량 필터(원하면 여기 한 줄 삭제하면 됨)
    g = g[g["avg_trade"] >= MIN_TRADECOUNT]

    rows = []
    for wd in WEEKDAY_KR:
        gw = g[g["weekday"] == wd].copy()
        if gw.empty:
            rows.append({
                "weekday": wd,
                "best_time": "-",
                "best_price_str": "-",
                "worst_time": "-",
                "worst_price_str": "-"
            })
            continue

        best = gw.loc[gw["avg_price"].idxmax()]
        worst = gw.loc[gw["avg_price"].idxmin()]

        rows.append({
            "weekday": wd,
            "best_time": str(best["time"]),
            "best_price_str": format_price_kr(float(best["avg_price"])),
            "worst_time": str(worst["time"]),
            "worst_price_str": format_price_kr(float(worst["avg_price"]))
        })
    return rows


def build_report(hist: pd.DataFrame, maps: List[str], days_for_report: int) -> str:
    # 데이터 없는 맵은 그래도 목록엔 표시
    maps_sorted = maps[:]

    # JS용 데이터 구성
    all_series: Dict[str, List[Dict[str, Any]]] = {}
    weekday_extrema: Dict[str, List[Dict[str, Any]]] = {}

    for kw in maps_sorted:
        packs = build_series_for_kw(hist, kw, days_for_report)
        all_series[kw] = [{
            "label": p.label,
            "x": p.x,
            "y": p.y,
            "hover": p.hover
        } for p in packs]

        weekday_extrema[kw] = compute_weekday_extrema(hist, kw, days_for_report)

    # HTML 생성 (⚠ f-string 충돌 방지: plotly %{...} 는 %{{...}}로 작성)
    # 브레이스 충돌이 나지 않도록, python은 f-string을 최소화하고 .format 사용
    html = """<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>MaShop 시세 대시보드</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  body {{
    margin: 0;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans KR", "Apple SD Gothic Neo", "Malgun Gothic", Arial, sans-serif;
    background: #0b0f14;
    color: #e8eef6;
  }}
  .wrap {{
    max-width: 1200px;
    margin: 0 auto;
    padding: 18px;
  }}
  .top {{
    display: flex;
    gap: 12px;
    align-items: center;
    justify-content: space-between;
    flex-wrap: wrap;
    margin-bottom: 12px;
  }}
  h1 {{
    font-size: 18px;
    margin: 0;
    font-weight: 700;
  }}
  .sub {{
    color: #a7b3c2;
    font-size: 12px;
    margin-top: 4px;
  }}
  .controls {{
    display: flex;
    gap: 8px;
    align-items: center;
    flex-wrap: wrap;
  }}
  select, button {{
    background: #111826;
    border: 1px solid #223044;
    color: #e8eef6;
    padding: 8px 10px;
    border-radius: 10px;
    outline: none;
    cursor: pointer;
  }}
  button.active {{
    border-color: #4da3ff;
  }}
  .grid {{
    display: grid;
    grid-template-columns: 1.2fr 0.8fr;
    gap: 12px;
  }}
  @media (max-width: 960px) {{
    .grid {{ grid-template-columns: 1fr; }}
  }}
  .card {{
    background: #0f1622;
    border: 1px solid #1f2b3d;
    border-radius: 14px;
    padding: 12px;
  }}
  .small {{
    color: #a7b3c2;
    font-size: 12px;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    margin-top: 8px;
  }}
  th, td {{
    padding: 8px 6px;
    border-bottom: 1px solid #1f2b3d;
    font-size: 13px;
  }}
  th {{
    text-align: left;
    color: #a7b3c2;
    font-weight: 600;
  }}
  td.r {{
    text-align: right;
    font-variant-numeric: tabular-nums;
  }}
  .hint {{
    color: #6f8298;
    font-size: 12px;
    margin-top: 8px;
  }}
</style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div>
        <h1>사냥터 시세 자동 수집기 & 분석 대시보드</h1>
        <div class="sub">최근 <b>{days_for_report}</b>일 기반 · 요일/시간 패턴 확인</div>
      </div>
      <div class="controls">
        <select id="kwSelect"></select>
        <button class="rangeBtn" data-days="7">7일</button>
        <button class="rangeBtn active" data-days="{days_for_report}">{days_for_report}일</button>
        <button class="rangeBtn" data-days="30">30일</button>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <div id="chart" style="height:520px;"></div>
        <div class="hint">※ 차트 점에 마우스를 올리면 <b>날짜/요일/시간/가격/거래량</b>이 표시됩니다.</div>
      </div>

      <div class="card">
        <b>요일별 최고가 / 최저가 (평균가 기준)</b>
        <div class="small">최근 N일 · 평균 거래량 <b>{min_trade}</b> 이상만 반영</div>
        <div id="weekTableWrap"></div>
      </div>
    </div>
  </div>

<script>
const MAPS = {maps_json};
const SERIES = {series_json};
const WEEK = {week_json};
const HOUR_ORDER = {hour_order};

function formatPriceKrFromNumber(num) {{
  if (num === null || num === undefined || isNaN(num)) return "-";
  const v = Number(num);
  if (v >= 100000000) {{
    const eok = v / 100000000;
    const rounded = Math.round(eok * 10) / 10;
    if (Math.abs(rounded - Math.round(rounded)) < 1e-9) return `${{Math.round(rounded)}}억`;
    return `${{rounded}}억`;
  }} else {{
    const man = Math.round(v / 10000);
    return `${{man}}만`;
  }}
}}

function setActiveRange(days) {{
  document.querySelectorAll(".rangeBtn").forEach(btn => {{
    btn.classList.toggle("active", String(btn.dataset.days) === String(days));
  }});
}}

function renderWeekTable(kw) {{
  const rows = (WEEK[kw] || []);
  const wrap = document.getElementById("weekTableWrap");

  if (!rows.length) {{
    wrap.innerHTML = "<div class='small' style='margin-top:10px;'>데이터가 부족합니다.</div>";
    return;
  }}

  let html = `
    <table>
      <thead>
        <tr>
          <th>요일</th>
          <th>최고가 시간</th>
          <th class="r">최고가(평균)</th>
          <th>최저가 시간</th>
          <th class="r">최저가(평균)</th>
        </tr>
      </thead>
      <tbody>
  `;

  rows.forEach(r => {{
    html += `
      <tr>
        <td>${{r.weekday}}</td>
        <td>${{r.best_time}}</td>
        <td class="r">${{r.best_price_str}}</td>
        <td>${{r.worst_time}}</td>
        <td class="r">${{r.worst_price_str}}</td>
      </tr>
    `;
  }});

  html += `</tbody></table>`;
  wrap.innerHTML = html;
}}

function buildTraces(kw, days) {{
  const packs = (SERIES[kw] || []);
  // days에 따라 최근 days만 보여주기(클라이언트에서 잘라서 빠르게)
  const sliced = packs.slice(Math.max(0, packs.length - days), packs.length);

  const traces = [];
  sliced.forEach(s => {{
    // customdata: [time, price_str, tradeCount, date, weekday]
    const customdata = s.hover.map(h => [h[0], h[1], h[2], h[3], h[4]]);
    traces.push({{
      x: s.x,
      y: s.y,
      type: "scatter",
      mode: "lines+markers",
      name: s.label,
      customdata: customdata,
      hovertemplate:
        "<b>" + s.label + "</b><br>" +
        "날짜: %{{customdata[3]}} (%{{customdata[4]}})<br>" +
        "시간: %{{customdata[0]}}<br>" +
        "가격: %{{customdata[1]}}<br>" +
        "거래: %{{customdata[2]}}<extra></extra>",
      connectgaps: false
    }});
  }});
  return traces;
}}

function renderChart(kw, days) {{
  const traces = buildTraces(kw, days);

  const layout = {{
    title: kw,
    paper_bgcolor: "#0f1622",
    plot_bgcolor: "#0f1622",
    font: {{ color: "#e8eef6" }},
    margin: {{ l: 55, r: 20, t: 50, b: 50 }},
    xaxis: {{
      type: "category",
      categoryorder: "array",
      categoryarray: HOUR_ORDER,
      title: "시간",
      gridcolor: "#1f2b3d",
      tickangle: -45
    }},
    yaxis: {{
      title: "가격",
      gridcolor: "#1f2b3d",
      tickformat: ","
    }},
    hovermode: "closest",
    legend: {{
      orientation: "h"
    }}
  }};

  Plotly.newPlot("chart", traces, layout, {{
    displayModeBar: true,
    responsive: true
  }});
}}

function init() {{
  const kwSelect = document.getElementById("kwSelect");
  MAPS.forEach(k => {{
    const opt = document.createElement("option");
    opt.value = k;
    opt.textContent = k;
    kwSelect.appendChild(opt);
  }});

  let currentDays = {days_for_report};
  if (kwSelect.options.length) kwSelect.value = MAPS[0];

  function rerender() {{
    const kw = kwSelect.value;
    renderWeekTable(kw);
    renderChart(kw, currentDays);
  }}

  kwSelect.addEventListener("change", () => {{
    rerender();
  }});

  document.querySelectorAll(".rangeBtn").forEach(btn => {{
    btn.addEventListener("click", () => {{
      currentDays = Number(btn.dataset.days);
      setActiveRange(currentDays);
      rerender();
    }});
  }});

  setActiveRange(currentDays);
  rerender();
}}

init();
</script>
</body>
</html>
""".format(
        days_for_report=days_for_report,
        min_trade=MIN_TRADECOUNT,
        maps_json=json.dumps(maps_sorted, ensure_ascii=False),
        series_json=json.dumps(all_series, ensure_ascii=False),
        week_json=json.dumps(weekday_extrema, ensure_ascii=False),
        hour_order=json.dumps(HOUR_ORDER, ensure_ascii=False),
    )

    return html


def main():
    ensure_dirs()
    maps = load_maps()

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) mashop-dashboard/1.0"
    })

    old_hist = load_history()

    new_parts = []
    for kw in maps:
        try:
            df_new = collect_recent(kw, DAYS_TO_FETCH, session=session)
            if df_new is not None and not df_new.empty:
                new_parts.append(df_new)
            else:
                # 데이터 없으면 그냥 스킵
                pass
        except Exception as e:
            print(f"[WARN] fetch failed: {kw} -> {e}")

    if new_parts:
        new_hist = pd.concat(new_parts, ignore_index=True)
    else:
        new_hist = pd.DataFrame(columns=["keyword", "mapName", "dateTime", "date", "time", "weekday", "price", "tradeCount", "timeUnit"])

    merged = merge_history(old_hist, new_hist)
    save_history(merged)

    html = build_report(merged, maps, DAYS_FOR_REPORT)
    with open(INDEX_HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print("[OK] history.csv / index.html generated")
    print(" -", HISTORY_CSV_PATH)
    print(" -", INDEX_HTML_PATH)


if __name__ == "__main__":
    main()
