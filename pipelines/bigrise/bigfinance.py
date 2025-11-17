#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigFinance 산업 카테고리 크롤러 (Prefect 파이프라인 대응 버전)
------------------------------------------------
- 로그인 → 산업 카테고리 수집 → 평탄화 CSV 생성
- header + companies 병합
- chart 데이터 JSON 저장
- chart 메타 저장: out/bigfinance/chart/chart_manifest.json + chart_index.csv

chart 저장 구조:
out/bigfinance/{data_type}/{main_code}/{group_id}/{sub_code}/{data_code}-{sub_name}-{data_name}.json

chart_index.csv 의 file_path 는 반드시 아래 형태:
./out/bigfinance/chart/0/2/1/2-글로벌_자동차_판매_국가별_(월)-USA.json
"""

import os, time, json, csv, random, sys, logging
from datetime import datetime
from urllib.parse import urljoin
from pathlib import Path
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import requests
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3


# =====================================================
# 경로 설정
# =====================================================
BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "out" / "bigfinance"
LOG_DIR = BASE_DIR / "logs"
CHART_META_DIR = OUT_DIR / "chart"

OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
CHART_META_DIR.mkdir(parents=True, exist_ok=True)

today = datetime.now().strftime("%Y%m%d")
CSV_FILE = OUT_DIR / f"industry_categories_{today}.csv"
OUT_FILE = OUT_DIR / f"industry_categories_{today}_with_meta_companies.csv"


# =====================================================
# 로깅
# =====================================================
log_path = LOG_DIR / f"bigfinance_{today}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(log_path, encoding="utf-8"),
              logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# =====================================================
# ENV 설정
# =====================================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

BASE_URL = os.getenv("BASE_URL", "https://bigfinance.co.kr").rstrip("/")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
LOGIN_PAGE = os.getenv("LOGIN_PAGE", "/login")
HEADLESS = os.getenv("HEADLESS", "false").lower() in ("1", "true", "yes")
KEEP_TEMP = os.getenv("KEEP_TEMP", "false").lower() in ("1", "true", "yes")

API_PATH = "/api/industry/categories"


# =====================================================
# Selenium
# =====================================================
chrome_opts = Options()
if HEADLESS:
    chrome_opts.add_argument("--headless=new")
chrome_opts.add_argument("--no-sandbox")
chrome_opts.add_argument("--disable-gpu")
chrome_opts.add_argument("--disable-dev-shm-usage")
chrome_opts.add_argument("--window-size=1280,850")
chrome_opts.add_argument("--blink-settings=imagesEnabled=false")
chrome_opts.add_argument("--disable-extensions")
chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
chrome_opts.add_argument("--remote-debugging-port=9222")

try:
    driver = webdriver.Chrome(service=Service(), options=chrome_opts)
except Exception as e:
    log.error(f"❌ Chrome 실행 실패: {e}")
    sys.exit(1)


# =====================================================
# 로그인
# =====================================================
def selenium_login(driver):
    log.info("[*] 로그인 시도 중...")
    driver.get(urljoin(BASE_URL, LOGIN_PAGE))
    time.sleep(2)

    try:
        radio = driver.find_element(By.ID, "enterprise-users")
        driver.execute_script("arguments[0].click();", radio)
    except:
        pass

    id_input = driver.find_element(By.XPATH, "//input[@type='text']")
    pw_input = driver.find_element(By.XPATH, "//input[@type='password']")
    id_input.send_keys(USERNAME)
    pw_input.send_keys(PASSWORD)

    btn = driver.find_element(By.XPATH, "//button[contains(text(),'로그인')]")
    driver.execute_script("arguments[0].click();", btn)
    time.sleep(2)

    cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
    return cookies


# =====================================================
# Session
# =====================================================
def make_requests_session(cookies):
    sess = requests.Session()
    sess.headers.update({
        "accept": "application/json",
        "user-agent": "Mozilla/5.0",
        "origin": BASE_URL,
        "referer": BASE_URL,
        "x-xsrf-token": cookies.get("XSRF-TOKEN")
    })
    for k, v in cookies.items():
        sess.cookies.set(k, v, domain="bigfinance.co.kr", path="/")
    return sess


# =====================================================
# 산업 카테고리 API
# =====================================================
def fetch_api(sess, path):
    url = urljoin(BASE_URL, path)
    r = sess.get(url, verify=False, timeout=30)
    return r.json() if r.status_code == 200 else None


# =====================================================
# CSV 평탄화
# =====================================================
def flatten_categories(data):
    rows = []
    for main in data.get("categories", []):
        for group in main.get("groups", []):
            for sub in group.get("subCategories", []):
                for cat in sub.get("dataCategories", []):
                    rows.append({
                        "main_code": main.get("code"),
                        "main_name": main.get("name"),
                        "group_id": group.get("groupId"),
                        "group_name": group.get("groupName"),
                        "sub_code": sub.get("subCode"),
                        "sub_name": sub.get("subName"),
                        "update_date": sub.get("updateDate"),
                        "data_type": sub.get("industryDataType"),
                        "data_code": cat.get("dataCode"),
                        "data_name": cat.get("dataName"),
                        "last_update": cat.get("lastUpdateDatetime")
                    })
    return rows


def save_to_csv(rows, out_path):
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)


# =====================================================
# header + companies
# =====================================================
def safe_get_json(sess, url):
    for _ in range(3):
        try:
            r = sess.get(url, verify=False, timeout=20)
            if r.status_code == 200:
                return r.json()
        except:
            pass
        time.sleep(1)
    return None


def fetch_header_meta(sess, main_code, sub_code):
    u = f"{BASE_URL}/api/industry/header/codes/{main_code}/subCodes/{sub_code}"
    d = safe_get_json(sess, u)
    return d if isinstance(d, dict) else {}


def fetch_companies(sess, main_code, sub_code):
    u = f"{BASE_URL}/api/industry/codes/{main_code}/subCodes/{sub_code}/companies"
    d = safe_get_json(sess, u)
    if isinstance(d, list):
        return [{"code": x.get("companyCode"), "name": x.get("companyName")} for x in d]
    if isinstance(d, dict) and "companies" in d:
        return [{"code": x.get("companyCode"), "name": x.get("companyName")} for x in d["companies"]]
    return []


def enrich_with_meta(sess, csv_path, out_path):
    df = pd.read_csv(csv_path)
    pairs = df.groupby(["main_code", "sub_code"]).size().index.tolist()

    cache_header = {}
    cache_comp = {}

    def job(a, b):
        cache_header[(a, b)] = fetch_header_meta(sess, a, b)
        cache_comp[(a, b)] = fetch_companies(sess, a, b)

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(job, a, b) for (a, b) in pairs]
        for _ in tqdm(as_completed(futures), total=len(futures), ncols=90, desc="header+companies"):
            pass

    for idx, r in df.iterrows():
        k = (r["main_code"], r["sub_code"])
        for k2, v2 in cache_header.get(k, {}).items():
            df.at[idx, k2] = v2
        df.at[idx, "companies"] = json.dumps(cache_comp.get(k, []), ensure_ascii=False)

    df.to_csv(out_path, index=False, encoding="utf-8-sig")


# =====================================================
# sanitize filename
# =====================================================
def sanitize_filename(text):
    if pd.isna(text):
        return "unknown"
    t = str(text).replace(" ", "_")
    for b in ['\\', '/', ':', '*', '?', '"', '<', '>', '|']:
        t = t.replace(b, "")
    return t


# =====================================================
# chart JSON 다운로드
# =====================================================
def fetch_chart_json(sess, main_code, group_id, sub_code,
                     data_code, data_type, sub_name, data_name):

    url = f"{BASE_URL}/api/industry/chart/codes/{main_code}/subCodes/{sub_code}?dataCode={data_code}"

    try:
        r = sess.get(url, verify=False, timeout=20)
        if r.status_code != 200:
            return None

        data = r.json()

        safe_type = sanitize_filename(data_type)
        safe_sub = sanitize_filename(sub_name)
        safe_name = sanitize_filename(data_name)

        out_dir = OUT_DIR / safe_type / str(main_code) / str(group_id) / str(sub_code)
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / f"{data_code}-{safe_sub}-{safe_name}.json"

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        return out_path

    except:
        return None


# =====================================================
# chart manifest + index 저장
# =====================================================
def build_chart_manifest(items):
    manifest = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_items": len(items),
        "items": items
    }

    # JSON
    with open(CHART_META_DIR / "chart_manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    # CSV
    pd.DataFrame(items).to_csv(
        CHART_META_DIR / "chart_index.csv",
        index=False,
        encoding="utf-8-sig"
    )

    log.info("📁 chart manifest + index 저장 완료")


# =====================================================
# chart 병렬 다운로드
# =====================================================
def download_all_charts(sess, csv_path, max_workers=6):
    df = pd.read_csv(csv_path)

    tasks = []
    for _, r in df.iterrows():
        tasks.append((
            r["main_code"], r["group_id"], r["sub_code"],
            r["data_code"], r["data_type"],
            r["sub_name"], r["data_name"],
            r["update_date"]
        ))

    index_items = []

    def work(t):
        (main_code, group_id, sub_code,
         data_code, data_type,
         sub_name, data_name,
         update_date) = t

        out_path = fetch_chart_json(
            sess, main_code, group_id, sub_code,
            data_code, data_type, sub_name, data_name
        )

        if out_path:
            # 상대경로 변환 (핵심!)
            rel_path = f"./{out_path.relative_to(BASE_DIR)}"

            index_items.append({
                "data_type": data_type,
                "main_code": main_code,
                "group_id": group_id,
                "sub_code": sub_code,
                "data_code": data_code,
                "sub_name": sanitize_filename(sub_name),
                "data_name": sanitize_filename(data_name),
                "file_path": rel_path,
                "update_date": update_date
            })

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(work, t) for t in tasks]
        for _ in tqdm(as_completed(futures), total=len(futures), ncols=90, desc="chart 수집"):
            pass

    build_chart_manifest(index_items)


# =====================================================
# main
# =====================================================
def main():
    try:
        cookies = selenium_login(driver)
        sess = make_requests_session(cookies)

        data = fetch_api(sess, API_PATH)
        rows = flatten_categories(data)
        save_to_csv(rows, CSV_FILE)

        enrich_with_meta(sess, CSV_FILE, OUT_FILE)

        download_all_charts(sess, OUT_FILE, max_workers=6)

        if not KEEP_TEMP and CSV_FILE.exists():
            CSV_FILE.unlink()

    except Exception as e:
        log.exception(f"❌ 오류 발생: {e}")

    finally:
        driver.quit()
        log.info("[*] Chrome 세션 종료")


if __name__ == "__main__":
    main()
