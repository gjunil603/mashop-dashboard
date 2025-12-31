import json
import os
from datetime import datetime, timedelta
import requests
import pandas as pd

API = "https://api.mashop.kr/api/v2/maps/price-stat/period"
WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]

# 사이트 시간축(01~23,00)
HOUR_ORDER = list(range(1, 24)) + [0]
HOUR_LABELS = [f"{h:02d}:00" for h in HOUR_ORDER]

DATA_PATH = "data/history.csv"
DOCS_PATH = "docs/index.html"
MAPS_PATH = "maps.json"

# ===== 설정값 (원하면 나중에 maps.json에 옮길 수 있음) =====
DAYS_FOR_REPORT = 14          # 웹페이지에서 기본 표시 기간(최근 N일)
MIN_TRADECOUNT = 10           # 추천 판매시간 계산 시 최소 거래건수(평균 기준)


def format_price_kr(price) -> str:
    if price is None or (isinstance(price, float) and pd.isna(price)):
        return "-"
    p = int(price)
    if p >= 100_000_000:
        eok = p // 100_000_000
        man = (p % 100_000_000) // 10_000
        return f"{eok}억 {man:,}만" if man else f"{eok}억"
    return f"{p // 10_000:,}만"


def parse_dt_seoul(dt_str: str):
    """
    mashop API dateTime이 tz 없이 내려오는 케이스가 있어 UTC로 가정 -> KST 변환
    """
    if not dt_str:
        return pd.NaT
    s = str(dt_str)
    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return pd.NaT
    if dt.tzinfo is None:
        return pd.to_datetime(s, utc=True).tz_convert("Asia/Seoul").tz_localize(None)
    return dt.tz_convert("Asia/Seoul").tz_localize(None)


def daterange_days(end_date: pd.Timestamp, days: int):
    start = (end_date - pd.Timedelta(days=days - 1)).date()
    end = end_date.date()
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def load_maps():
    with open(MAPS_PATH, "r", encoding="utf-8") as f:
        j = json.load(f)
    maps = j.get("maps", [])
    maps = [m.strip() for m in maps if m and str(m).strip()]
    if not maps:
        raise ValueError("maps.json에 maps 목록이 비었습니다.")
    return maps


def fetch_period(keyword: str, start_date: str, end_date: str):
    params = {"keyword": keyword, "startDate": start_date, "endDate": end_date}
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    r = requests.get(API, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError(f"Unexpected JSON shape: {type(data)}")
    return data


def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs("docs", exist_ok=True)


def load_history():
    if os.path.exists(DATA_PATH):
        df = pd.read_csv(DATA_PATH, encoding="utf-8-sig")
        return df
    return pd.DataFrame(columns=[
        "mapName", "keyword", "date", "weekday", "hour", "time", "dateTime",
        "price", "tradeCount", "timeUnit"
    ])


def save_history(df: pd.DataFrame):
    df.to_csv(DATA_PATH, index=False, encoding="utf-8-sig")


def build_report(df_all: pd.DataFrame, maps: list[str], days_for_report: int):
    # 기준일: df 내 최대 dateTime
    df_all["dateTime_dt"] = pd.to_datetime(df_all["dateTime"], errors="coerce")
    df_all = df_all.dropna(subset=["dateTime_dt"])
    max_dt = df_all["dateTime_dt"].max()
    start_s, end_s = daterange_days(max_dt, days_for_report)

    # 최근 N일만 필터
    sdt = pd.to_datetime(start_s)
    edt = pd.to_datetime(end_s) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    df = df_all[(df_all["dateTime_dt"] >= sdt) & (df_all["dateTime_dt"] <= edt)].copy()

    # 사냥터 목록 정렬(드롭다운)
    maps_sorted = [m for m in maps if m in set(df["keyword"].unique())] + [m for m in maps if m not in set(df["keyword"].unique())]

    # 사냥터별 추천 판매시간 TOP3 만들기
    # 기준: (요일,시간) 평균가격이 높은 순 / 거래건수 평균이 MIN_TRADECOUNT 이상
    rec = {}
    for kw in maps_sorted:
        sub = df[df["keyword"] == kw].copy()
        if sub.empty:
            rec[kw] = []
            continue

        g = sub.groupby(["weekday", "time"], as_index=False).agg(
            avg_price=("price", "mean"),
            avg_trade=("tradeCount", "mean"),
            n=("price", "count")
        )
        g = g.dropna(subset=["avg_price"])
        g["avg_trade"] = g["avg_trade"].fillna(0)
        g = g[g["avg_trade"] >= MIN_TRADECOUNT]

        g = g.sort_values("avg_price", ascending=False).head(3)

        items = []
        for _, r in g.iterrows():
            items.append({
                "weekday": r["weekday"],
                "time": r["time"],
                "avg_price": float(r["avg_price"]),
                "avg_price_str": format_price_kr(r["avg_price"]),
                "avg_trade": int(round(r["avg_trade"])),
                "n": int(r["n"])
            })
        rec[kw] = items

    # Plotly에 넘길 데이터(사냥터/날짜별 라인)
    # 사이트 방식: 01~23은 해당 날짜, 마지막 00:00은 다음날 00:00을 붙여줌
    def build_series_for_kw(kw: str):
        dkw = df[df["keyword"] == kw].copy()
        if dkw.empty:
            return {"dates": [], "series": []}

        agg = dkw.groupby(["date", "weekday", "hour"], as_index=False).agg(
            price=("price", "mean"),
            trade=("tradeCount", "mean"),
        )
        key = {}
        for _, r in agg.iterrows():
            key[(r["date"], int(r["hour"]))] = (r["price"], r["trade"], r["weekday"])

        dates_sorted = sorted(agg["date"].unique())
        series = []
        for d in dates_sorted:
            d_dt = pd.to_datetime(d)
            d_next = (d_dt + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

            # 요일
            w = None
            for h in range(1, 24):
                v = key.get((d, h))
                if v:
                    w = v[2]
                    break
            if w is None:
                v0 = key.get((d, 0))
                w = v0[2] if v0 else ""

            label = f"{d}({w})" if w else d

            x = [f"{h:02d}:00" for h in range(1, 24)] + ["00:00"]
            y = []
            hover = []  # (시간,가격문자,거래문자,기준날짜)

            # 01~23은 d
            for h in range(1, 24):
                v = key.get((d, h))
                if v:
                    price, trade, _ = v
                    y.append(float(price))
                    hover.append((f"{h:02d}:00", format_price_kr(price), f"{int(round(trade))}건" if pd.notna(trade) else "", d))
                else:
                    y.append(None)
                    hover.append((f"{h:02d}:00", "-", "", d))

            # 마지막 00:00은 다음날 00:00
            v00 = key.get((d_next, 0))
            if v00:
                price, trade, _ = v00
                y.append(float(price))
                hover.append(("00:00", format_price_kr(price), f"{int(round(trade))}건" if pd.notna(trade) else "", d_next))
            else:
                y.append(None)
                hover.append(("00:00", "-", "", d_next))

            series.append({"label": label, "x": x, "y": y, "hover": hover})

        return {"dates": dates_sorted, "series": series}

    per_kw = {kw: build_series_for_kw(kw) for kw in maps_sorted}

    # HTML 생성(Plotly CDN + JS 드롭다운)
    html = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>MaShop 사냥터 시세 대시보드</title>
  <script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, "Malgun Gothic", sans-serif; margin: 16px; }}
    .row {{ display:flex; gap:12px; flex-wrap:wrap; align-items:center; }}
    select, button {{ padding:8px 10px; font-size:14px; }}
    .card {{ border:1px solid #ddd; border-radius:10px; padding:12px; margin-top:12px; }}
    .small {{ color:#666; font-size:12px; }}
    .rec li {{ margin: 6px 0; }}
    .pill {{ display:inline-block; padding:2px 8px; border:1px solid #ccc; border-radius:999px; font-size:12px; margin-left:6px; color:#444; }}
  </style>
</head>
<body>
  <h2>MaShop 사냥터 시세 대시보드</h2>
  <div class="small">최근 {days_for_report}일 기준 · 업데이트 기준시각: {max_dt.strftime("%Y-%m-%d %H:%M")} (KST)</div>

  <div class="card">
    <div class="row">
      <div><b>사냥터 선택</b></div>
      <select id="kwSelect"></select>
      <div class="small">추천 판매시간은 거래(평균) {MIN_TRADECOUNT}건 이상만 반영</div>
    </div>
  </div>

  <div class="card">
    <b>추천 판매시간 TOP 3</b>
    <ul class="rec" id="recList"></ul>
  </div>

  <div class="card">
    <b>날짜별 시간대 가격 변화</b>
    <div class="small">x축: 01:00~23:00 + 마지막 00:00(다음날 00:00)</div>
    <div id="chart" style="width:100%;height:520px;"></div>
  </div>

<script>
const MAPS = {json.dumps(maps_sorted, ensure_ascii=False)};
const DATA = {json.dumps(per_kw, ensure_ascii=False)};
const REC  = {json.dumps(rec, ensure_ascii=False)};

const kwSelect = document.getElementById("kwSelect");
const recList = document.getElementById("recList");

function renderDropdown() {{
  kwSelect.innerHTML = "";
  MAPS.forEach((kw) => {{
    const opt = document.createElement("option");
    opt.value = kw;
    opt.textContent = kw;
    kwSelect.appendChild(opt);
  }});
}}

function renderRecommendations(kw) {{
  recList.innerHTML = "";
  const items = REC[kw] || [];
  if (!items.length) {{
    const li = document.createElement("li");
    li.textContent = "추천할 데이터가 부족합니다.";
    recList.appendChild(li);
    return;
  }}
  items.forEach((it, idx) => {{
    const li = document.createElement("li");
    li.innerHTML = `<b>#${{idx+1}}</b> ${{it.weekday}} ${{it.time}} · 평균 ${{it.avg_price_str}} 
      <span class="pill">평균거래 ${{it.avg_trade}}건</span>
      <span class="pill">표본 ${{it.n}}</span>`;
    recList.appendChild(li);
  }});
}}

function renderChart(kw) {{
  const obj = DATA[kw];
  const series = (obj && obj.series) ? obj.series : [];
  const traces = [];

  series.forEach(s => {{
    const customdata = s.hover.map(h => [h[0], h[1], h[2], h[3]]);
    traces.push({{
      x: s.x,
      y: s.y,
      type: "scatter",
      mode: "lines+markers",
      name: s.label,
      customdata,
      hovertemplate:
          "<b>"+s.label+"</b><br>" +
          "시간: %{{customdata[0]}}<br>" +
          "가격: %{{customdata[1]}}<br>" +
          "거래: %{{customdata[2]}}<br>" +
          "기준날짜: %{{customdata[3]}}<extra></extra>",
      connectgaps: false
    }});
  }});

  const layout = {{
    title: kw,
    margin: {{l: 50, r: 20, t: 50, b: 50}},
    xaxis: {{
      type: "category",
      categoryorder: "array",
      categoryarray: {json.dumps([f"{h:02d}:00" for h in range(1,24)] + ["00:00"], ensure_ascii=False)},
      title: "시간"
    }},
    yaxis: {{ title: "가격" }},
    hovermode: "closest",
    legend: {{orientation: "h"}}
  }};

  Plotly.newPlot("chart", traces, layout, {{displayModeBar: true, responsive: true}});
}}

kwSelect.addEventListener("change", () => {{
  const kw = kwSelect.value;
  renderRecommendations(kw);
  renderChart(kw);
}});

renderDropdown();
const initial = MAPS[0];
kwSelect.value = initial;
renderRecommendations(initial);
renderChart(initial);
</script>
</body>
</html>
"""
    with open(DOCS_PATH, "w", encoding="utf-8") as f:
        f.write(html)


def main():
    ensure_dirs()
    maps = load_maps()

    # 기존 누적 데이터 로드
    hist = load_history()

    # 수집 범위: 리포트 기간보다 넉넉히(00시/경계 섞임 대응)
    end_dt = pd.Timestamp.now(tz="Asia/Seoul").tz_localize(None)
    start_date, end_date = daterange_days(end_dt, DAYS_FOR_REPORT + 1)  # +1일 여유

    new_rows = []
    for kw in maps:
        data = fetch_period(kw, start_date, end_date)
        for it in data:
            dt = parse_dt_seoul(it.get("dateTime"))
            if pd.isna(dt):
                continue
            price = pd.to_numeric(it.get("price"), errors="coerce")
            if pd.isna(price):
                continue

            d = dt.strftime("%Y-%m-%d")
            w = WEEKDAY_KR[dt.weekday()]
            hour = int(dt.strftime("%H"))

            new_rows.append({
                "mapName": it.get("mapName") or kw,
                "keyword": kw,
                "date": d,
                "weekday": w,
                "hour": hour,
                "time": f"{hour:02d}:00",
                "dateTime": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "price": float(price),
                "tradeCount": pd.to_numeric(it.get("tradeCount"), errors="coerce"),
                "timeUnit": it.get("timeUnit"),
            })

    new_df = pd.DataFrame(new_rows)
    if not new_df.empty:
        # 누적 + 중복 제거 (사냥터+dateTime 기준)
        merged = pd.concat([hist, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["keyword", "dateTime"], keep="last")
        merged = merged.sort_values(["keyword", "dateTime"])
        save_history(merged)
        hist = merged

    # 리포트 생성
    if hist.empty:
        raise ValueError("누적 데이터가 비었습니다. (키워드/기간 확인 필요)")
    build_report(hist, maps, DAYS_FOR_REPORT)


if __name__ == "__main__":
    main()


