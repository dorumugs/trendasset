#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
from tqdm import tqdm

# ---------- 경로 설정 ----------
BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "out"
RISE_PATH = OUT_DIR / "riseETF" / "rise_finder_20251107_with_holdings_flattened.csv"
INDUSTRY_PATH = OUT_DIR / "bigfinance" / "industry_categories_20251107_with_meta_companies.csv"
OUTPUT_PATH = OUT_DIR / "bigrise_20251107.csv"

# ---------- CSV 로드 ----------
rise_df = pd.read_csv(RISE_PATH)
industry_df = pd.read_csv(INDUSTRY_PATH)

# ---------- 매칭 수행 ----------
matches = []

print("🔍 Matching item_name with industry companies...")
for _, row in tqdm(industry_df.iterrows(), total=len(industry_df)):
    companies = str(row["companies"])
    if not companies or companies.lower() == "nan":
        continue

    # item_name이 companies 문자열에 포함되면 매칭
    matched = rise_df[rise_df["item_name"].astype(str).apply(lambda x: x in companies)]
    if not matched.empty:
        info = f"{row['sub_name']}-{row['data_name']}"
        temp = matched.copy()
        temp["industry_info"] = info
        temp["industry_frequency"] = row.get("frequency", "")
        temp["industry_source"] = row.get("source", "")
        matches.append(temp)

# ---------- 병합 ----------
if matches:
    merged_df = pd.concat(matches).drop_duplicates(subset=["item_name"])
    rise_df = rise_df.merge(
        merged_df[["item_name", "industry_info", "industry_frequency", "industry_source"]],
        on="item_name",
        how="left"
    )
else:
    rise_df["industry_info"] = None
    rise_df["industry_frequency"] = None
    rise_df["industry_source"] = None

# ---------- 저장 ----------
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
rise_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

print(f"\n✅ 완료: {OUTPUT_PATH}")
print(f"매칭된 행 수: {rise_df['industry_info'].notna().sum()} / {len(rise_df)}")
