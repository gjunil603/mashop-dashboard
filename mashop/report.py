from __future__ import annotations

import json
from typing import Any, Dict, List

from .config import HOUR_ORDER


def build_report_html(
    maps: List[str],
    daily_series: Dict[str, Any],
    points: Dict[str, Any],
    default_days: int,
    min_trade: float,
) -> str:
    html = r"""<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>MaShop 시세 대시보드</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
  body {
    margin: 0;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans KR", "Apple SD Gothic Neo", "Malgun Gothic", Arial, sans-serif;
    background: #0b0f14;
    color: #e8eef6;
  }
  .wrap { max-width: 1200px; margin: 0 auto; padding: 18px; }
  .top { display:flex; gap:12px; align-items:center; justify-content:space-between; flex-wrap:wrap; margin-bottom:12px; }
  h1 { font-size:18px; margin:0; font-weight:700; }
  .sub { color:#a7b3c2; font-size:12px; margin-top:4px; }
  .controls { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
  select, button { background:#111826; border:1px solid #223044; color:#e8eef6; padding:8px 10px; border-radius:10px; outline:none; cursor:pointer; }
  button.active { border-color:#4da3ff; }
  .card { background:#0f1622; border:1px solid #1f2b3d; border-radius:14px; padding:12px; margin-bottom:12px; }
  .small { color:#a7b3c2; font-size:12px; }
  table { width:100%; border-collapse:collapse; margin-top:8px; }
  th, td { padding:8px 6px; border-bottom:1px solid #1f2b3d; font-size:13px; }
  th { text-align:left; color:#a7b3c2; font-weight:600; }
  td.r { text-align:right; font-variant-numeric: tabular-nums; }
  .hint { color:#6f8298; font-size:12px; margin-top:8px; }
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
        <button class="rangeBtn active" data-days="__DEFAULT_DAYS__">__DEFAULT_DAYS__일</button>
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
      <div class="small">월~일 최대 7줄 · 평균 거래량 __MIN_TRADE__ 이상만 반영 · 표본수(n)도 툴팁에 표시</div>
      <div id="chartWeek" style="height:520px;"></div>
    </div>

    <div class="card">
      <b>📋 요일별 최고가 / 최저가 (평균가 기준)</b>
      <div class="small">선택 기간 기준 · 평균 거래량 __MIN_TRADE__ 이상만 반영</div>
      <div id="weekTableWrap"></div>
    </div>
  </div>

<script>
const MAPS = __MAPS_JSON__;
const DAILY = __DAILY_JSON__;
const POINTS = __POINTS_JSON__;
const HOUR_ORDER = __HOUR_ORDER__;
const WEEK_ORDER = ["월","화","수","목","금","토","일"];
const MIN_TRADE = __MIN_TRADE__;

function setActiveRange(days) {
  document.querySelectorAll(".rangeBtn").forEach(btn => {
    btn.classList.toggle("active", String(btn.dataset.days) === String(days));
  });
}

function formatPriceKrFromNumber(num) {
  if (num === null || num === undefined || isNaN(num)) return "-";
  const v = Number(num);
  if (v >= 100000000) {
    const eok = v / 100000000;
    const rounded = Math.round(eok * 10) / 10;
    if (Math.abs(rounded - Math.round(rounded)) < 1e-9) return `${Math.round(rounded)}억`;
    return `${rounded}억`;
  } else {
    const man = Math.round(v / 10000);
    return `${man}만`;
  }
}

function ymdToday() {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth()+1).padStart(2,"0");
  const day = String(d.getDate()).padStart(2,"0");
  return `${y}-${m}-${day}`;
}

function ymdMinusDays(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  const y = d.getFullYear();
  const m = String(d.getMonth()+1).padStart(2,"0");
  const day = String(d.getDate()).padStart(2,"0");
  return `${y}-${m}-${day}`;
}

function filterPointsByDays(points, days) {
  const start = ymdMinusDays(days - 1);
  return points.filter(p => p.date >= start && p.date <= ymdToday());
}

function buildDailyTraces(kw, days) {
  const packs = (DAILY[kw] || []);
  const start = ymdMinusDays(days - 1);
  const filtered = packs.filter(p => p.label >= start && p.label <= ymdToday());

  const traces = [];
  filtered.forEach(s => {
    const customdata = s.hover.map(h => [h[0], h[1], h[2], h[3], h[4]]);
    traces.push({
      x: s.x,
      y: s.y,
      type: "scatter",
      mode: "lines+markers",
      name: s.label,
      customdata: customdata,
      hovertemplate:
        "<b>" + s.label + "</b><br>" +
        "날짜: %{customdata[3]} (%{customdata[4]})<br>" +
        "시간: %{customdata[0]}<br>" +
        "가격: %{customdata[1]}<br>" +
        "거래: %{customdata[2]}건<extra></extra>",
      connectgaps: false
    });
  });
  return traces;
}

function renderDailyChart(kw, days) {
  const traces = buildDailyTraces(kw, days);
  const layout = {
    title: kw,
    paper_bgcolor: "#0f1622",
    plot_bgcolor: "#0f1622",
    font: { color: "#e8eef6" },
    margin: { l: 55, r: 20, t: 50, b: 50 },
    xaxis: {
      type: "category",
      categoryorder: "array",
      categoryarray: HOUR_ORDER,
      title: "시간",
      gridcolor: "#1f2b3d",
      tickangle: -45
    },
    yaxis: {
      title: "가격",
      gridcolor: "#1f2b3d",
      tickformat: ","
    },
    hovermode: "closest",
    legend: { orientation: "h" }
  };
  Plotly.newPlot("chartDaily", traces, layout, { displayModeBar: true, responsive: true });
}

function buildWeekAvgStats(kw, days) {
  const ptsAll = (POINTS[kw] || []);
  const pts = filterPointsByDays(ptsAll, days);

  const stats = {};
  WEEK_ORDER.forEach(w => {
    stats[w] = {};
    HOUR_ORDER.forEach(t => {
      stats[w][t] = { sumPrice: 0, cnt: 0, sumTrade: 0 };
    });
  });

  pts.forEach(p => {
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
  });

  const avg = {};
  WEEK_ORDER.forEach(w => {
    avg[w] = {};
    HOUR_ORDER.forEach(t => {
      const cell = stats[w][t];
      if (cell.cnt <= 0) {
        avg[w][t] = { avgPrice: null, n: 0, avgTrade: null };
        return;
      }
      const avgPrice = cell.sumPrice / cell.cnt;
      const avgTrade = cell.sumTrade / cell.cnt;

      if (avgTrade !== null && !isNaN(avgTrade) && avgTrade < MIN_TRADE) {
        avg[w][t] = { avgPrice: null, n: cell.cnt, avgTrade: avgTrade };
      } else {
        avg[w][t] = { avgPrice: avgPrice, n: cell.cnt, avgTrade: avgTrade };
      }
    });
  });

  return avg;
}

function buildWeekAvgTraces(kw, days) {
  const avg = buildWeekAvgStats(kw, days);
  const traces = [];

  WEEK_ORDER.forEach(w => {
    const y = [];
    const custom = [];
    HOUR_ORDER.forEach(t => {
      const cell = avg[w][t];
      y.push(cell.avgPrice === null ? null : cell.avgPrice);
      const pstr = (cell.avgPrice === null) ? "-" : formatPriceKrFromNumber(cell.avgPrice);
      const nstr = String(cell.n || 0);
      const trstr = (cell.avgTrade === null || isNaN(cell.avgTrade)) ? "-" : String(Math.round(cell.avgTrade));
      custom.push([w, t, pstr, nstr, trstr]);
    });

    traces.push({
      x: HOUR_ORDER,
      y: y,
      type: "scatter",
      mode: "lines+markers",
      name: w,
      customdata: custom,
      hovertemplate:
        "<b>%{customdata[0]}</b><br>" +
        "시간: %{customdata[1]}<br>" +
        "평균가: %{customdata[2]}<br>" +
        "표본(n): %{customdata[3]}<br>" +
        "평균거래: %{customdata[4]}건<extra></extra>",
      connectgaps: false
    });
  });

  return traces;
}

function renderWeekChart(kw, days) {
  const traces = buildWeekAvgTraces(kw, days);
  const layout = {
    title: kw + " · 요일 평균",
    paper_bgcolor: "#0f1622",
    plot_bgcolor: "#0f1622",
    font: { color: "#e8eef6" },
    margin: { l: 55, r: 20, t: 50, b: 50 },
    xaxis: {
      type: "category",
      categoryorder: "array",
      categoryarray: HOUR_ORDER,
      title: "시간",
      gridcolor: "#1f2b3d",
      tickangle: -45
    },
    yaxis: {
      title: "평균 가격",
      gridcolor: "#1f2b3d",
      tickformat: ","
    },
    hovermode: "closest",
    legend: { orientation: "h" }
  };
  Plotly.newPlot("chartWeek", traces, layout, { displayModeBar: true, responsive: true });
}

function renderWeekTable(kw, days) {
  const avg = buildWeekAvgStats(kw, days);
  const wrap = document.getElementById("weekTableWrap");

  const rows = [];
  WEEK_ORDER.forEach(w => {
    let bestT = "-", bestP = null;
    let worstT = "-", worstP = null;

    HOUR_ORDER.forEach(t => {
      const cell = avg[w][t];
      const p = cell.avgPrice;
      if (p === null || p === undefined || isNaN(p)) return;

      if (bestP === null || p > bestP) { bestP = p; bestT = t; }
      if (worstP === null || p < worstP) { worstP = p; worstT = t; }
    });

    rows.push({
      weekday: w,
      best_time: bestT,
      best_price_str: bestP === null ? "-" : formatPriceKrFromNumber(bestP),
      worst_time: worstT,
      worst_price_str: worstP === null ? "-" : formatPriceKrFromNumber(worstP),
    });
  });

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

  rows.forEach(r => {
    html += `
      <tr>
        <td>${r.weekday}</td>
        <td>${r.best_time}</td>
        <td class="r">${r.best_price_str}</td>
        <td>${r.worst_time}</td>
        <td class="r">${r.worst_price_str}</td>
      </tr>
    `;
  });

  html += `</tbody></table>`;
  wrap.innerHTML = html;
}

function init() {
  const kwSelect = document.getElementById("kwSelect");
  MAPS.forEach(k => {
    const opt = document.createElement("option");
    opt.value = k;
    opt.textContent = k;
    kwSelect.appendChild(opt);
  });

  let currentDays = __DEFAULT_DAYS__;
  if (kwSelect.options.length) kwSelect.value = MAPS[0];

  function rerender() {
    const kw = kwSelect.value;
    renderDailyChart(kw, currentDays);
    renderWeekChart(kw, currentDays);
    renderWeekTable(kw, currentDays);
  }

  kwSelect.addEventListener("change", () => rerender());

  document.querySelectorAll(".rangeBtn").forEach(btn => {
    btn.addEventListener("click", () => {
      currentDays = Number(btn.dataset.days);
      setActiveRange(currentDays);
      rerender();
    });
  });

  setActiveRange(currentDays);
  rerender();
}

init();
</script>
</body>
</html>
"""

    html = html.replace("__DEFAULT_DAYS__", str(default_days))
    html = html.replace("__MIN_TRADE__", str(min_trade))
    html = html.replace("__MAPS_JSON__", json.dumps(maps, ensure_ascii=False))
    html = html.replace("__DAILY_JSON__", json.dumps(daily_series, ensure_ascii=False))
    html = html.replace("__POINTS_JSON__", json.dumps(points, ensure_ascii=False))
    html = html.replace("__HOUR_ORDER__", json.dumps(HOUR_ORDER, ensure_ascii=False))
    return html

