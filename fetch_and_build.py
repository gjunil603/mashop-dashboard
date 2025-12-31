# fetch_and_build.py
from __future__ import annotations

import os
import json
import math
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd


# =========================
# 설정값
# =========================
API_BASE = "https://api.mashop.kr"

# 대시보드 기본 선택 기간(버튼에 표시되는 기본 active)
DEFAULT_DAYS_FOR_UI = int(os.environ.get("DAYS_FOR_REPORT", "14"))

# 실행마다 API에서 가져오는 기간(누적 갱신용)
# 30이면 최근 30일치 받아서 history.csv에 merge
DAYS_TO_FETCH = int(os.environ.get("DAYS_TO_FETCH", "30"))

# 요일 평균/최고최저 계산 시 평균 거래량이 너무 적으면(가격 튐) 제외
MIN_TRADECOUNT = float(os.environ.get("MIN_TRADECOUNT", "5"))

WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]

DATA_DIR = "data"
DOCS_DIR = "docs"
MAPS_JSON_PATH = "maps.json"
HISTORY_CSV_PATH = os.path.join(DATA_DIR, "history.csv")
INDEX_HTML_PATH = os.path.join(DOCS_DIR, "index.html")
RAW_DUMP_DIR = os.path.join(DATA_DIR, "raw_dump")

# 사이트처럼 01:00 시작 ~ 23:00, 마지막에 00:00
HOUR_ORDER = [f"{h:02d}:00" for h in range(1, 24)] + ["00:00"]


# =========================
# 유틸
# =========================
def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(DOCS_DIR, exist_ok=True)
    os.makedirs(RAW_DUMP_DIR, exist_ok=True)


def load_maps() -> List[str]:
    """
    maps.json은 반드시 리스트 형식:
      ["미나르숲:남겨진 용의 둥지", "사냥터B", ...]
    """
    if not os.path.exists(MAPS_JSON_PATH):
        raise FileNotFoundError(f"{MAPS_JSON_PATH} 파일이 없습니다.")
    with open(MAPS_JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError('maps.json 형식이 잘못되었습니다. 예: ["미나르숲:남겨진 용의 둥지", "..."]')
    maps = [str(x).strip() for x in data if str(x).strip()]
    if not maps:
        raise ValueError("maps.json이 비어있습니다. 맵 이름을 1개 이상 넣어주세요.")
    return maps


def parse_dt_kst(dt_str: str) -> datetime:
    # 예: "2025-12-31T01:00:00" (TZ 정보 없음)
    return datetime.fromisoformat(dt_str)


def weekday_kr_from_dt(dt: datetime) -> str:
    return WEEKDAY_KR[dt.weekday()]


def format_price_kr(x: float | int | None) -> str:
    """
    7,000,000 -> 700만
    17,000,000 -> 1700만
    100,000,000 -> 1억
    120,000,000 -> 1.2억
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
        # 소수 1자리
        eok_r = round(eok, 1)
        if abs(eok_r - round(eok_r)) < 1e-9:
            return f"{int(round(eok_r))}억"
        return f"{eok_r}억"
    else:
        man = int(round(v / 10_000))
        return f"{man}만"


def safe_json_dump(path: str, obj: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def last_n_days_range(n: int, include_today: bool = True) -> Tuple[str, str]:
    today = date.today()
    if include_today:
        start = today - timedelta(days=n - 1)
        end = today
    else:
        start = today - timedelta(days=n)
        end = today - timedelta(days=1)
    return start.isoformat(), end.isoformat()


# =========================
# API 수집
# =========================
def fetch_period(keyword: str, start_date: str, end_date: str, session: requests.Session) -> List[Dict[str, Any]]:
    url = f"{API_BASE}/api/v2/maps/price-stat/period"
    params = {"keyword": keyword, "startDate": start_date, "endDate": end_date}
    r = session.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    # 일반적으로 list지만, 방어적으로 처리
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("data", "items", "result", "content"):
            if k in data and isinstance(data[k], list):
                return data[k]
    return []


def collect_recent(keyword: str, days_to_fetch: int, session: requests.Session) -> pd.DataFrame:
    start_date, end_date = last_n_days_range(days_to_fetch, include_today=True)
    rows = fetch_period(keyword, start_date, end_date, session=session)

    dump_path = os.path.join(RAW_DUMP_DIR, f"{keyword}_{start_date}_to_{end_date}.json")
    safe_json_dump(dump_path, rows)

    out = []
    for it in rows:
        dt_s = it.get("dateTime")
        if not dt_s:
            continue
        try:
            dt = parse_dt_kst(str(dt_s))
        except Exception:
            continue

        price = it.get("price")
        tc = it.get("tradeCount")

        out.append({
            "keyword": keyword,
            "mapName": it.get("mapName", keyword),
            "dateTime": dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "date": dt.strftime("%Y-%m-%d"),
            "time": dt.strftime("%H:%M"),
            "weekday": weekday_kr_from_dt(dt),
            "price": float(price) if price is not None else None,
            "tradeCount": float(tc) if tc is not None else None,
            "timeUnit": it.get("timeUnit"),
        })

    return pd.DataFrame(out)


# =========================
# 히스토리 누적/정리
# =========================
def load_history() -> pd.DataFrame:
    if not os.path.exists(HISTORY_CSV_PATH):
        return pd.DataFrame(columns=["keyword", "mapName", "dateTime", "date", "time", "weekday", "price", "tradeCount", "timeUnit"])

    # utf-8 또는 utf-8-sig 방어
    try:
        df = pd.read_csv(HISTORY_CSV_PATH, encoding="utf-8")
    except Exception:
        df = pd.read_csv(HISTORY_CSV_PATH, encoding="utf-8-sig")

    for col in ["keyword", "mapName", "dateTime", "date", "time", "weekday", "timeUnit"]:
        if col not in df.columns:
            df[col] = None
    for col in ["price", "tradeCount"]:
        if col not in df.columns:
            df[col] = None

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["tradeCount"] = pd.to_numeric(df["tradeCount"], errors="coerce")
    return df


def merge_history(old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    if old_df is None or old_df.empty:
        merged = new_df.copy()
    elif new_df is None or new_df.empty:
        merged = old_df.copy()
    else:
        merged = pd.concat([old_df, new_df], ignore_index=True)

    merged["keyword"] = merged["keyword"].astype(str)
    merged["dateTime"] = merged["dateTime"].astype(str)

    # 중복 제거: keyword + dateTime
    merged = merged.drop_duplicates(subset=["keyword", "dateTime"], keep="last")
    merged = merged.sort_values(by=["keyword", "dateTime"], ascending=[True, True])

    merged["price"] = pd.to_numeric(merged["price"], errors="coerce")
    merged["tradeCount"] = pd.to_numeric(merged["tradeCount"], errors="coerce")

    # date/time/weekday 보정(혹시 누락된 레코드 대비)
    # (가능한 경우만)
    def _fill_row(row):
        if (not row.get("date")) or (not row.get("time")) or (not row.get("weekday")):
            try:
                dt = parse_dt_kst(row["dateTime"])
                row["date"] = row.get("date") or dt.strftime("%Y-%m-%d")
                row["time"] = row.get("time") or dt.strftime("%H:%M")
                row["weekday"] = row.get("weekday") or weekday_kr_from_dt(dt)
            except Exception:
                pass
        return row

    merged = merged.apply(_fill_row, axis=1)
    return merged


def save_history(df: pd.DataFrame):
    df.to_csv(HISTORY_CSV_PATH, index=False, encoding="utf-8")


# =========================
# 리포트 데이터 구성
# =========================
def build_daily_series_for_kw(hist: pd.DataFrame, kw: str) -> List[Dict[str, Any]]:
    """
    날짜별 선 그래프용 데이터:
    - 한 날짜 = 한 라인
    - x = HOUR_ORDER
    - y = 시간대 가격
    - hover = [time, price_str, trade_str, date, weekday]
    """
    sub = hist[hist["keyword"] == kw].copy()
    if sub.empty:
        return []

    sub["dt"] = pd.to_datetime(sub["dateTime"], errors="coerce")
    sub = sub.dropna(subset=["dt"])
    if sub.empty:
        return []

    sub["date"] = sub["dt"].dt.strftime("%Y-%m-%d")
    sub["time"] = sub["dt"].dt.strftime("%H:%M")
    sub["weekday"] = sub["dt"].dt.weekday.map(lambda i: WEEKDAY_KR[int(i)])

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
                hover.append([t, format_price_kr(float(p)), trade_str, d, wd])

        packs.append({"label": d, "x": x, "y": y, "hover": hover})

    packs.sort(key=lambda x: x["label"])
    return packs


def build_points_for_kw(hist: pd.DataFrame, kw: str, max_days: int = 60) -> List[Dict[str, Any]]:
    """
    요일 평균 그래프/요일별 최고최저 표를 클라이언트에서 계산하기 위한 '점' 데이터(가볍게)
    최근 max_days만 전달.
    """
    sub = hist[hist["keyword"] == kw].copy()
    if sub.empty:
        return []

    sub["dt"] = pd.to_datetime(sub["dateTime"], errors="coerce")
    sub = sub.dropna(subset=["dt"])
    if sub.empty:
        return []

    cutoff = datetime.now() - timedelta(days=max_days)
    sub = sub[sub["dt"] >= cutoff]
    if sub.empty:
        return []

    sub = sub.sort_values("dt")
    # 같은 dateTime이 중복될 수 있으니 마지막 유지
    sub = sub.drop_duplicates(subset=["dateTime"], keep="last")

    sub["date"] = sub["dt"].dt.strftime("%Y-%m-%d")
    sub["time"] = sub["dt"].dt.strftime("%H:%M")
    sub["weekday"] = sub["dt"].dt.weekday.map(lambda i: WEEKDAY_KR[int(i)])

    pts = []
    for _, r in sub.iterrows():
        p = r.get("price")
        if p is None or (isinstance(p, float) and math.isnan(p)):
            continue
        tc = r.get("tradeCount")
        pts.append({
            "date": str(r["date"]),
            "time": str(r["time"]),
            "weekday": str(r["weekday"]),
            "price": float(p),
            "tradeCount": None if tc is None or (isinstance(tc, float) and math.isnan(tc)) else float(tc),
        })
    return pts


def build_report_html(maps: List[str], daily_series: Dict[str, Any], points: Dict[str, Any]) -> str:
    # f-string 충돌(Plotly %{...}) 방지 위해 .format 사용
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
  .card {{
    background: #0f1622;
    border: 1px solid #1f2b3d;
    border-radius: 14px;
    padding: 12px;
    margin-bottom: 12px;
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
        <h1>사냥터 시세 대시보드</h1>
        <div class="sub">사냥터 선택 → 기간(7/14/30일) → 날짜별 원본 + 요일 평균 패턴을 같이 확인</div>
      </div>
      <div class="controls">
        <select id="kwSelect"></select>
        <button class="rangeBtn" data-days="7">7일</button>
        <button class="rangeBtn active" data-days="{default_days}">{default_days}일</button>
        <button class="rangeBtn" data-days="30">30일</button>
      </div>
    </div>

    <div class="card">
      <b>📈 날짜별 시간 그래프 (원본 확인용)</b>
      <div class="small">한 줄 = 하루 · 점에 마우스를 올리면 날짜/요일/시간/가격/거래량 표시</div>
      <div id="chartDaily" style="height:520px;"></div>
      <div class="hint">※ 선이 많아지면(예: 30일) 겹쳐 보일 수 있어요. 아래 요일 평균 그래프로 패턴을 보는 걸 추천합니다.</div>
    </div>

    <div class="card">
      <b>📊 요일 평균 그래프 (분석용)</b>
      <div class="small">월~일 최대 7줄 · 평균 거래량 {min_trade} 이상만 반영 · 표본수(n)도 툴팁에 표시</div>
      <div id="chartWeek" style="height:520px;"></div>
    </div>

    <div class="card">
      <b>📋 요일별 최고가 / 최저가 (평균가 기준)</b>
      <div class="small">선택 기간 기준 · 평균 거래량 {min_trade} 이상만 반영</div>
      <div id="weekTableWrap"></div>
    </div>
  </div>

<script>
const MAPS = {maps_json};
const DAILY = {daily_json};
const POINTS = {points_json};
const HOUR_ORDER = {hour_order};
const WEEK_ORDER = ["월","화","수","목","금","토","일"];
const MIN_TRADE = {min_trade};

function setActiveRange(days) {{
  document.querySelectorAll(".rangeBtn").forEach(btn => {{
    btn.classList.toggle("active", String(btn.dataset.days) === String(days));
  }});
}}

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

function ymdToday() {{
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth()+1).padStart(2,"0");
  const day = String(d.getDate()).padStart(2,"0");
  return `${{y}}-${{m}}-${{day}}`;
}}

function ymdMinusDays(n) {{
  const d = new Date();
  d.setDate(d.getDate() - n);
  const y = d.getFullYear();
  const m = String(d.getMonth()+1).padStart(2,"0");
  const day = String(d.getDate()).padStart(2,"0");
  return `${{y}}-${{m}}-${{day}}`;
}}

function filterPointsByDays(points, days) {{
  const start = ymdMinusDays(days - 1);
  // date는 "YYYY-MM-DD" → 문자열 비교 가능
  return points.filter(p => p.date >= start && p.date <= ymdToday());
}}

function buildDailyTraces(kw, days) {{
  const packs = (DAILY[kw] || []);
  const start = ymdMinusDays(days - 1);
  const filtered = packs.filter(p => p.label >= start && p.label <= ymdToday());

  const traces = [];
  filtered.forEach(s => {{
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
        "거래: %{{customdata[2]}}건<extra></extra>",
      connectgaps: false
    }});
  }});
  return traces;
}}

function renderDailyChart(kw, days) {{
  const traces = buildDailyTraces(kw, days);
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
    legend: {{ orientation: "h" }}
  }};
  Plotly.newPlot("chartDaily", traces, layout, {{ displayModeBar: true, responsive: true }});
}}

function buildWeekAvgStats(kw, days) {{
  const ptsAll = (POINTS[kw] || []);
  const pts = filterPointsByDays(ptsAll, days);

  // stats[weekday][time] = {sumPrice, cnt, sumTrade}
  const stats = {{}};
  WEEK_ORDER.forEach(w => {{
    stats[w] = {{}};
    HOUR_ORDER.forEach(t => {{
      stats[w][t] = {{ sumPrice: 0, cnt: 0, sumTrade: 0 }};
    }});
  }});

  pts.forEach(p => {{
    const w = p.weekday;
    const t = p.time;
    const price = Number(p.price);
    const trade = (p.tradeCount === null || p.tradeCount === undefined) ? null : Number(p.tradeCount);

    if (!WEEK_ORDER.includes(w)) return;
    if (!HOUR_ORDER.includes(t)) return;
    if (isNaN(price)) return;

    const cell = stats[w][t];
    cell.sumPrice += price;
    cell.cnt += 1;
    if (trade !== null && !isNaN(trade)) cell.sumTrade += trade;
  }});

  // 평균 계산 + trade 필터 적용(평균 거래량 기준)
  const avg = {{}};
  WEEK_ORDER.forEach(w => {{
    avg[w] = {{}};
    HOUR_ORDER.forEach(t => {{
      const cell = stats[w][t];
      if (cell.cnt <= 0) {{
        avg[w][t] = {{ avgPrice: null, n: 0, avgTrade: null }};
        return;
      }}
      const avgPrice = cell.sumPrice / cell.cnt;

      // avgTrade는 tradeCount가 누락되면 정확하지 않을 수 있어도 참고용
      const avgTrade = cell.sumTrade / cell.cnt;

      // 평균 거래량 필터(거래량이 너무 적으면 제외)
      if (avgTrade !== null && !isNaN(avgTrade) && avgTrade < MIN_TRADE) {{
        avg[w][t] = {{ avgPrice: null, n: cell.cnt, avgTrade: avgTrade }};
      }} else {{
        avg[w][t] = {{ avgPrice: avgPrice, n: cell.cnt, avgTrade: avgTrade }};
      }}
    }});
  }});

  return avg;
}}

function buildWeekAvgTraces(kw, days) {{
  const avg = buildWeekAvgStats(kw, days);
  const traces = [];

  WEEK_ORDER.forEach(w => {{
    const y = [];
    const custom = [];
    HOUR_ORDER.forEach(t => {{
      const cell = avg[w][t];
      y.push(cell.avgPrice === null ? null : cell.avgPrice);
      const pstr = (cell.avgPrice === null) ? "-" : formatPriceKrFromNumber(cell.avgPrice);
      const nstr = String(cell.n || 0);
      const trstr = (cell.avgTrade === null || isNaN(cell.avgTrade)) ? "-" : String(Math.round(cell.avgTrade));
      custom.push([w, t, pstr, nstr, trstr]);
    }});

    traces.push({{
      x: HOUR_ORDER,
      y: y,
      type: "scatter",
      mode: "lines+markers",
      name: w,
      customdata: custom,
      hovertemplate:
        "<b>%{{customdata[0]}}</b><br>" +
        "시간: %{{customdata[1]}}<br>" +
        "평균가: %{{customdata[2]}}<br>" +
        "표본(n): %{{customdata[3]}}<br>" +
        "평균거래: %{{customdata[4]}}건<extra></extra>",
      connectgaps: false
    }});
  }});

  return traces;
}}

function renderWeekChart(kw, days) {{
  const traces = buildWeekAvgTraces(kw, days);
  const layout = {{
    title: kw + " · 요일 평균",
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
      title: "평균 가격",
      gridcolor: "#1f2b3d",
      tickformat: ","
    }},
    hovermode: "closest",
    legend: {{ orientation: "h" }}
  }};
  Plotly.newPlot("chartWeek", traces, layout, {{ displayModeBar: true, responsive: true }});
}}

function renderWeekTable(kw, days) {{
  const avg = buildWeekAvgStats(kw, days);
  const wrap = document.getElementById("weekTableWrap");

  // 요일별 best/worst
  const rows = [];
  WEEK_ORDER.forEach(w => {{
    let bestT = "-", bestP = null;
    let worstT = "-", worstP = null;

    HOUR_ORDER.forEach(t => {{
      const cell = avg[w][t];
      const p = cell.avgPrice;
      if (p === null || p === undefined || isNaN(p)) return;

      if (bestP === null || p > bestP) {{
        bestP = p;
        bestT = t;
      }}
      if (worstP === null || p < worstP) {{
        worstP = p;
        worstT = t;
      }}
    }});

    rows.push({{
      weekday: w,
      best_time: bestT,
      best_price_str: bestP === null ? "-" : formatPriceKrFromNumber(bestP),
      worst_time: worstT,
      worst_price_str: worstP === null ? "-" : formatPriceKrFromNumber(worstP),
    }});
  }});

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

function init() {{
  const kwSelect = document.getElementById("kwSelect");
  MAPS.forEach(k => {{
    const opt = document.createElement("option");
    opt.value = k;
    opt.textContent = k;
    kwSelect.appendChild(opt);
  }});

  let currentDays = {default_days};
  if (kwSelect.options.length) kwSelect.value = MAPS[0];

  function rerender() {{
    const kw = kwSelect.value;
    renderDailyChart(kw, currentDays);
    renderWeekChart(kw, currentDays);
    renderWeekTable(kw, currentDays);
  }}

  kwSelect.addEventListener("change", () => rerender());

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
        default_days=DEFAULT_DAYS_FOR_UI,
        min_trade=MIN_TRADECOUNT,
        maps_json=json.dumps(maps, ensure_ascii=False),
        daily_json=json.dumps(daily_series, ensure_ascii=False),
        points_json=json.dumps(points, ensure_ascii=False),
        hour_order=json.dumps(HOUR_ORDER, ensure_ascii=False),
    )

    return html


# =========================
# main
# =========================
def main():
    ensure_dirs()
    maps = load_maps()

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) mashop-dashboard/1.0",
        "Accept": "application/json",
    })

    old_hist = load_history()

    new_parts = []
    for kw in maps:
        try:
            df_new = collect_recent(kw, DAYS_TO_FETCH, session=session)
            if df_new is not None and not df_new.empty:
                new_parts.append(df_new)
            else:
                print(f"[INFO] no data: {kw}")
        except Exception as e:
            print(f"[WARN] fetch failed: {kw} -> {e}")

    new_hist = pd.concat(new_parts, ignore_index=True) if new_parts else pd.DataFrame(
        columns=["keyword", "mapName", "dateTime", "date", "time", "weekday", "price", "tradeCount", "timeUnit"]
    )

    merged = merge_history(old_hist, new_hist)
    save_history(merged)

    # 대시보드 데이터 구성
    daily_series: Dict[str, Any] = {}
    points: Dict[str, Any] = {}

    for kw in maps:
        daily_series[kw] = build_daily_series_for_kw(merged, kw)
        points[kw] = build_points_for_kw(merged, kw, max_days=60)

    html = build_report_html(maps, daily_series, points)
    with open(INDEX_HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print("[OK] generated")
    print(" -", HISTORY_CSV_PATH)
    print(" -", INDEX_HTML_PATH)


if __name__ == "__main__":
    main()
