import os

API_BASE = "https://api.mashop.kr"

# 웹에서 기본 선택 기간 버튼(초기 active)
DEFAULT_DAYS_FOR_UI = int(os.environ.get("DAYS_FOR_REPORT", "14"))

# 매 실행마다 API에서 가져와서 누적 갱신할 기간(최근 N일)
DAYS_TO_FETCH = int(os.environ.get("DAYS_TO_FETCH", "7"))

# 요일 평균/최고최저 계산에서 평균 거래량이 너무 적으면 제외(노이즈 방지)
MIN_TRADECOUNT = float(os.environ.get("MIN_TRADECOUNT", "5"))

DATA_DIR = "data"
DOCS_DIR = "docs"
MAPS_JSON_PATH = "maps.json"

# 사냥터별 데이터 저장 폴더
MAPS_DIR = os.path.join(DATA_DIR, "maps")
INDEX_HTML_PATH = os.path.join(DOCS_DIR, "index.html")

WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]

# 사이트처럼 01:00 시작 ~ 23:00, 마지막에 00:00
HOUR_ORDER = [f"{h:02d}:00" for h in range(1, 24)] + ["00:00"]

# points는 클라이언트(브라우저)에서 요일평균 계산할 때 쓰는 "가벼운 점" 데이터
POINTS_MAX_DAYS = int(os.environ.get("POINTS_MAX_DAYS", "60"))

# 오래된 데이터 자동 정리(보관 기간)
KEEP_DAYS = 180

# raw_dump 보관 기간 (일)
RAW_KEEP_DAYS = 14
