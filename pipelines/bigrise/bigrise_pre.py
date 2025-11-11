#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF–산업 매칭 스크립트 (Prefect 파이프라인 대응 버전)
------------------------------------------------
- RISE ETF 구성내역과 BigFinance 산업 기업 데이터를 매칭
- 결과: ETF 구성종목별 industry_info / frequency / source / update_date 추가
- 최근 7일 이내 update_date 산업에 속한 ETF만 별도 CSV로 저장
- 경로 구조: project-root/out/, project-root/logs/
"""

import pandas as pd
from pathlib import Path
from tqdm import tqdm
import logging, sys, time
from datetime import datetime, timedelta

# =====================================================
# 경로 설정 (Prefect 환경 호환)
# =====================================================
BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "out"
LOG_DIR = BASE_DIR / "logs"

OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 오늘 날짜 기반
today = time.strftime("%Y%m%d")
LOG_FILE = LOG_DIR / f"bigrise_{today}.log"

# =====================================================
# 로깅 설정
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# =====================================================
# 입력·출력 경로 설정
# =====================================================
RISE_PATH = OUT_DIR / "riseETF" / f"rise_finder_{today}_with_holdings_flattened.csv"
INDUSTRY_PATH = OUT_DIR / "bigfinance" / f"industry_categories_{today}_with_meta_companies.csv"
OUTPUT_DIR = OUT_DIR / "bigRise"
OUTPUT_PATH = OUTPUT_DIR / f"bigrise_{today}.csv"
RECENT_PATH = OUTPUT_DIR / f"bigrise_recent_{today}.csv"

# =====================================================
# 실행 함수
# =====================================================
def parse_date(date_str):
    """YYYYMMDD 또는 YYYY-MM-DD 형식 모두 datetime으로 변환"""
    if pd.isna(date_str):
        return None
    try:
        return datetime.strptime(str(date_str), "%Y-%m-%d")
    except ValueError:
        try:
            return datetime.strptime(str(date_str), "%Y%m%d")
        except ValueError:
            return None


def main():
    try:
        log.info("🚀 ETF–산업 매칭 시작")
        log.info(f"📂 RISE: {RISE_PATH}")
        log.info(f"📂 INDUSTRY: {INDUSTRY_PATH}")

        if not RISE_PATH.exists():
            log.error(f"❌ ETF 파일이 없습니다: {RISE_PATH}")
            sys.exit(1)
        if not INDUSTRY_PATH.exists():
            log.error(f"❌ 산업 파일이 없습니다: {INDUSTRY_PATH}")
            sys.exit(1)

        # 데이터 로드
        rise_df = pd.read_csv(RISE_PATH)
        industry_df = pd.read_csv(INDUSTRY_PATH)

        log.info(f"🔍 매칭 수행 중... (산업데이터 {len(industry_df)}행)")
        matches = []

        for _, row in tqdm(industry_df.iterrows(), total=len(industry_df), desc="Matching"):
            companies = str(row.get("companies", ""))
            if not companies or companies.lower() == "nan":
                continue

            matched = rise_df[rise_df["item_name"].astype(str).apply(lambda x: x in companies)]
            if not matched.empty:
                info = f"{row['sub_name']}-{row['data_name']}"
                temp = matched.copy()
                temp["industry_info"] = info
                temp["industry_frequency"] = row.get("frequency", "")
                temp["industry_source"] = row.get("source", "")
                temp["industry_update_date"] = row.get("update_date", "")
                matches.append(temp)

        # 매칭 결과 반영
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
                    ]
                ],
                on="item_name",
                how="left",
            )
            log.info(f"✅ 매칭 완료: {rise_df['industry_info'].notna().sum()} / {len(rise_df)}")
        else:
            log.warning("⚠️ 매칭 결과가 없습니다. 빈 컬럼 추가만 수행합니다.")
            rise_df["industry_info"] = None
            rise_df["industry_frequency"] = None
            rise_df["industry_source"] = None
            rise_df["industry_update_date"] = None

        # 💾 전체 결과 저장
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        rise_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
        log.info(f"💾 전체 결과 저장 완료 → {OUTPUT_PATH}")

        # 🕒 최근 7일 이내 update_date 필터링
        today_dt = datetime.now()
        recent_cutoff = today_dt - timedelta(days=7)

        rise_df["parsed_date"] = rise_df["industry_update_date"].apply(parse_date)
        recent_df = rise_df[
            rise_df["parsed_date"].notna() & (rise_df["parsed_date"] >= recent_cutoff)
        ]

        if len(recent_df) > 0:
            recent_df.to_csv(RECENT_PATH, index=False, encoding="utf-8-sig")
            log.info(f"📆 최근 7일 이내 산업 매칭 ETF 저장 완료 → {RECENT_PATH}")
            log.info(f"🟢 {len(recent_df)}건 저장됨")
        else:
            log.info("⚪ 최근 7일 이내 산업 업데이트 없음")

        log.info("🎯 ETF–산업 매칭 파이프라인 완료")

    except Exception as e:
        log.exception(f"❌ 실행 중 오류 발생: {e}")
        sys.exit(1)


# =====================================================
# 메인 실행
# =====================================================
if __name__ == "__main__":
    main()
