#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF–산업 매칭 스크립트 (Prefect 파이프라인 대응 버전)
------------------------------------------------
- RISE ETF 구성내역 + BigFinance 산업 기업 매칭
- chart_index.csv 기반 chart_path 매칭
- industry_update_date 우선순위:
      1) industry_update_date_raw
      2) industry_update_date_header
      3) chart_update_date
- 최근 7일 내 산업 업데이트 ETF 저장
- 최근 산업 관련 chart JSON도 자동 복사(out/bigRise/recent)
"""

import pandas as pd
from pathlib import Path
from tqdm import tqdm
import logging, sys, time, shutil
from datetime import datetime, timedelta


# =====================================================
# 경로 설정 (Prefect 환경 호환)
# =====================================================
BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "out"
LOG_DIR = BASE_DIR / "logs"

OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

today = time.strftime("%Y%m%d")
LOG_FILE = LOG_DIR / f"bigrise_{today}.log"


# =====================================================
# 로깅
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"),
              logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# =====================================================
# 경로
# =====================================================
RISE_PATH = OUT_DIR / "riseETF" / f"rise_finder_{today}_with_holdings_flattened.csv"
INDUSTRY_PATH = OUT_DIR / "bigfinance" / f"industry_categories_{today}_with_meta_companies.csv"
CHART_INDEX_PATH = OUT_DIR / "bigfinance" / "chart" / "chart_index.csv"

OUTPUT_DIR = OUT_DIR / "bigRise"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = OUTPUT_DIR / f"bigrise_{today}.csv"
RECENT_PATH = OUTPUT_DIR / f"bigrise_recent_{today}.csv"
RECENT_CHART_DIR = OUTPUT_DIR / "recent"     # 🔥 추가된 폴더
RECENT_CHART_DIR.mkdir(parents=True, exist_ok=True)


# =====================================================
# 날짜 파서
# =====================================================
def parse_date(date_str):
    if pd.isna(date_str):
        return None
    s = str(date_str).strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except:
            pass
    return None


# =====================================================
# update_date 최종 선택
# =====================================================
def get_update_date(row):
    for col in ["industry_update_date_raw", "industry_update_date_header", "chart_update_date"]:
        v = row.get(col, None)
        if pd.notna(v) and str(v).strip() != "":
            return v
    return ""


# =====================================================
# 최근 산업 chart 파일 복사 기능
# =====================================================
def copy_recent_charts(csv_path):
    """
    bigrise_recent_YYYYMMDD.csv 에 있는 industry_chart_path의 JSON 파일을
    out/bigRise/recent/ 폴더에 복사한다.
    """

    df = pd.read_csv(csv_path)

    if "industry_chart_path" not in df.columns:
        log.warning("⚠️ industry_chart_path 컬럼이 없어 chart 복사를 하지 않습니다.")
        return

    copied = 0

    for path_str in df["industry_chart_path"]:
        if pd.isna(path_str) or not str(path_str).strip():
            continue

        # industry_chart_path 는 "./out/..." 형태 → "./" 제거
        rel = path_str.replace("./", "")
        src = BASE_DIR / rel

        if not src.exists():
            log.warning(f"⚠️ chart 파일 없음: {src}")
            continue

        dst = RECENT_CHART_DIR / src.name
        shutil.copy2(src, dst)
        copied += 1

    log.info(f"📁 최근 chart 파일 복사 완료: 총 {copied}개")


# =====================================================
# 메인
# =====================================================
def main():
    log.info("🚀 ETF–산업 매칭 파이프라인 시작")

    if not RISE_PATH.exists():
        log.error(f"❌ ETF 파일 없음: {RISE_PATH}")
        sys.exit(1)

    if not INDUSTRY_PATH.exists():
        log.error(f"❌ 산업 파일 없음: {INDUSTRY_PATH}")
        sys.exit(1)

    rise_df = pd.read_csv(RISE_PATH)
    industry_df = pd.read_csv(INDUSTRY_PATH)

    # 산업 날짜 보호
    industry_df = industry_df.rename(columns={
        "update_date": "industry_update_date_raw",
        "updateDate": "industry_update_date_header"
    })

    # chart_index 병합
    if CHART_INDEX_PATH.exists():
        chart_df = pd.read_csv(CHART_INDEX_PATH)
        chart_df = chart_df.rename(columns={"update_date": "chart_update_date"})

        industry_df = industry_df.merge(
            chart_df[
                [
                    "main_code",
                    "group_id",
                    "sub_code",
                    "data_code",
                    "file_path",
                    "chart_update_date"
                ]
            ],
            on=["main_code", "group_id", "sub_code", "data_code"],
            how="left"
        )

        industry_df = industry_df.rename(columns={"file_path": "chart_path"})

    # 매칭
    matches = []

    for _, row in tqdm(industry_df.iterrows(), total=len(industry_df), ncols=90):
        comps = str(row.get("companies", ""))
        if not comps or comps.lower() == "nan":
            continue

        matched = rise_df[rise_df["item_name"].astype(str).apply(lambda x: x in comps)]
        if matched.empty:
            continue

        info = f"{row['sub_name']}-{row['data_name']}"

        temp = matched.copy()
        temp["industry_info"] = info
        temp["industry_frequency"] = row.get("frequency", "")
        temp["industry_source"] = row.get("source", "")
        temp["industry_update_date"] = get_update_date(row)
        temp["industry_chart_path"] = row.get("chart_path", "")

        matches.append(temp)

    # 매칭 반영
    if matches:
        merged_df = pd.concat(matches).drop_duplicates(subset=["item_name"])
        rise_df = rise_df.merge(
            merged_df[
                [
                    "item_name",
                    "industry_info",
                    "industry_frequency",
                    "industry_source",
                    "industry_update_date",
                    "industry_chart_path"
                ]
            ],
            on="item_name",
            how="left",
        )

    # 전체 저장
    rise_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    # 최근 7일 필터링
    rise_df["parsed_date"] = rise_df["industry_update_date"].apply(parse_date)
    cutoff = datetime.now() - timedelta(days=7)

    recent_df = rise_df[
        rise_df["parsed_date"].notna() & (rise_df["parsed_date"] >= cutoff)
    ]

    if len(recent_df) > 0:
        recent_df.to_csv(RECENT_PATH, index=False, encoding="utf-8-sig")
        log.info(f"📆 최근 7일 산업 업데이트 ETF 저장 → {RECENT_PATH}")

        # 🔥 최근 chart 파일 복사 실행
        copy_recent_charts(RECENT_PATH)
    else:
        log.info("⚪ 최근 7일 내 산업 업데이트 없음")

    log.info("🎯 ETF–산업 매칭 파이프라인 완료")


# =====================================================
# 실행
# =====================================================
if __name__ == "__main__":
    main()
