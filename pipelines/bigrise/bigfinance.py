#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BigFinance 산업 카테고리 크롤러 (Prefect 파이프라인 대응 버전)
------------------------------------------------
- 로그인 → API 호출 → JSON 평탄화 → header/companies 병합
- 경로 구조: project-root/out/bigfinance/, project-root/logs/
- 제어: .env 파일에서 KEEP_TEMP=true 설정 시 중간 CSV 보존
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
# 경로 설정 (Prefect 환경 호환)
# =====================================================
BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "out" / "bigfinance"
LOG_DIR = BASE_DIR / "logs"

OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

today = datetime.now().strftime("%Y%m%d")
CSV_FILE = OUT_DIR / f"industry_categories_{today}.csv"
OUT_FILE = OUT_DIR / f"industry_categories_{today}_with_meta_companies.csv"

# =====================================================
# 로깅 설정
# =====================================================
log_path = LOG_DIR / f"bigfinance_{today}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# =====================================================
# 기본 설정
# =====================================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

BASE_URL = os.getenv("BASE_URL", "https://bigfinance.co.kr").rstrip("/")
LOGIN_PAGE = os.getenv("LOGIN_PAGE", "/login")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
HEADLESS = os.getenv("HEADLESS", "false").lower() in ("1", "true", "yes")
KEEP_TEMP = os.getenv("KEEP_TEMP", "false").lower() in ("1", "true", "yes")
API_PATH = "/api/industry/categories"

# =====================================================
# Selenium 설정
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
chrome_opts.add_argument("--disable-software-rasterizer")
chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
chrome_opts.add_argument("--remote-debugging-port=9222")

try:
    driver = webdriver.Chrome(service=Service(), options=chrome_opts)
except Exception as e:
    log.error(f"❌ Chrome 실행 실패: {e}")
    sys.exit(1)

# =====================================================
# 1️⃣ 로그인 & 쿠키 수집
# =====================================================
def selenium_login(driver):
    log.info("[*] 로그인 시도 중 ...")
    url = urljoin(BASE_URL, LOGIN_PAGE)
    driver.get(url)
    time.sleep(2)

    try:
        enterprise_radio = driver.find_element(By.ID, "enterprise-users")
        driver.execute_script("arguments[0].click();", enterprise_radio)
        time.sleep(0.5)
    except Exception:
        log.warning("⚠️ 기업 사용자 라디오버튼을 찾지 못했습니다. 기본 사용자로 진행합니다.")

    id_input = driver.find_element(By.XPATH, "//input[@type='text']")
    pw_input = driver.find_element(By.XPATH, "//input[@type='password']")
    id_input.clear()
    id_input.send_keys(USERNAME)
    pw_input.clear()
    pw_input.send_keys(PASSWORD)

    login_btn = driver.find_element(By.XPATH, "//button[contains(text(),'로그인')]")
    driver.execute_script("arguments[0].click();", login_btn)
    time.sleep(3)

    cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
    log.info("[*] 로그인 완료. 주요 쿠키:")
    for k in ("XSRF-TOKEN", "SESSION", "account_id", "account_type"):
        if k in cookies:
            log.info(f"   {k}={cookies[k][:40]}...")
    return cookies

# =====================================================
# 2️⃣ Session 생성
# =====================================================
def make_requests_session(cookies):
    sess = requests.Session()
    xsrf = cookies.get("XSRF-TOKEN")
    sess.headers.update({
        "accept": "application/json, text/plain, */*",
        "user-agent": "Mozilla/5.0",
        "referer": BASE_URL,
        "origin": BASE_URL,
        "x-xsrf-token": xsrf,
    })
    for name, value in cookies.items():
        sess.cookies.set(name, value, domain="bigfinance.co.kr", path="/")
    return sess

# =====================================================
# 3️⃣ API 호출
# =====================================================
def fetch_api(sess, path):
    url = urljoin(BASE_URL, path)
    resp = sess.get(url, verify=False, timeout=30)
    log.info(f"[*] GET {url} → {resp.status_code}")
    if resp.status_code == 200:
        return resp.json()
    else:
        log.error(f"❌ API 실패: {resp.text[:200]}")
        return None

# =====================================================
# 4️⃣ JSON 평탄화
# =====================================================
def flatten_categories(json_data):
    rows = []
    categories = json_data.get("categories", [])
    for main in categories:
        main_code = main.get("code")
        main_name = main.get("name")
        for group in main.get("groups", []):
            group_id = group.get("groupId")
            group_name = group.get("groupName")
            for sub in group.get("subCategories", []):
                sub_code = sub.get("subCode")
                sub_name = sub.get("subName")
                update_date = sub.get("updateDate")
                data_type = sub.get("industryDataType")
                for cat in sub.get("dataCategories", []):
                    rows.append({
                        "main_code": main_code,
                        "main_name": main_name,
                        "group_id": group_id,
                        "group_name": group_name,
                        "sub_code": sub_code,
                        "sub_name": sub_name,
                        "update_date": update_date,
                        "data_type": data_type,
                        "data_code": cat.get("dataCode"),
                        "data_name": cat.get("dataName"),
                        "last_update": cat.get("lastUpdateDatetime"),
                    })
    return rows

def save_to_csv(rows, out_path):
    if not rows:
        log.warning("❗ 저장할 데이터가 없습니다.")
        return False
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"✅ 1차 CSV 저장 완료: {out_path} ({len(rows)} rows)")
    return True

# =====================================================
# 5️⃣ header + companies 병합
# =====================================================
def safe_get_json(sess, url, retries=3, timeout=(5,25)):
    for attempt in range(retries):
        try:
            resp = sess.get(url, timeout=timeout, verify=False)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        time.sleep(1 + attempt + random.random())
    return None

def fetch_header_meta(sess, main_code, sub_code):
    url = f"{BASE_URL}/api/industry/header/codes/{main_code}/subCodes/{sub_code}"
    data = safe_get_json(sess, url)
    if not isinstance(data, dict):
        return {"frequency": None, "unit": None, "source": None, "footnote": None, "yoyFlag": None, "updateDate": None}
    return {
        "frequency": data.get("frequency"),
        "unit": data.get("unit"),
        "source": data.get("source"),
        "footnote": data.get("footnote"),
        "yoyFlag": data.get("yoyFlag"),
        "updateDate": data.get("updateDate"),
    }

def fetch_companies(sess, main_code, sub_code):
    url = f"{BASE_URL}/api/industry/codes/{main_code}/subCodes/{sub_code}/companies"
    data = safe_get_json(sess, url)
    if not data:
        return []
    if isinstance(data, list):
        return [{"code": d.get("companyCode"), "name": d.get("companyName")} for d in data if isinstance(d, dict)]
    elif isinstance(data, dict) and "companies" in data:
        return [{"code": d.get("companyCode"), "name": d.get("companyName")} for d in data["companies"] if isinstance(d, dict)]
    return []

def enrich_with_meta(sess, csv_path, out_path, max_workers=4):
    df = pd.read_csv(csv_path)
    for col in ["frequency", "unit", "source", "footnote", "yoyFlag", "updateDate", "companies"]:
        df[col] = None

    sub_map = df.groupby(["main_code", "sub_code"]).size().index.tolist()
    cache_header, cache_companies = {}, {}

    def task(main_code, sub_code):
        meta = fetch_header_meta(sess, main_code, sub_code)
        comps = fetch_companies(sess, main_code, sub_code)
        cache_header[(main_code, sub_code)] = meta
        cache_companies[(main_code, sub_code)] = comps
        time.sleep(random.uniform(0.3, 1.0))
        return (main_code, sub_code)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(task, m, s) for (m, s) in sub_map]
        for fut in tqdm(as_completed(futures), total=len(futures), ncols=90, desc="header+companies 수집 중"):
            try:
                fut.result()
            except Exception as e:
                log.error(f"[ERR] {e}")

    for i, row in df.iterrows():
        key = (row["main_code"], row["sub_code"])
        meta = cache_header.get(key, {})
        comps = cache_companies.get(key, [])
        if meta:
            for k, v in meta.items():
                df.at[i, k] = v
        df.at[i, "companies"] = json.dumps(comps, ensure_ascii=False)

    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    log.info(f"✅ 최종 CSV 저장 완료: {out_path} (총 {len(df)}행)")

# =====================================================
# 6️⃣ 메인 실행
# =====================================================
def main():
    try:
        log.info("[*] BigFinance 로그인 중 ...")
        cookies = selenium_login(driver)
        sess = make_requests_session(cookies)

        log.info("[*] 산업 카테고리 데이터 수집 중 ...")
        data = fetch_api(sess, API_PATH)
        if not data:
            log.error("❌ API 응답 없음. 종료합니다.")
            sys.exit(1)

        rows = flatten_categories(data)
        if not save_to_csv(rows, CSV_FILE):
            sys.exit(1)

        log.info("[*] header + companies 병합 중 ...")
        enrich_with_meta(sess, CSV_FILE, OUT_FILE, max_workers=4)

        if KEEP_TEMP:
            log.info(f"🗂 중간 파일 보존 (.env KEEP_TEMP=true): {CSV_FILE.name}")
        else:
            if CSV_FILE.exists():
                CSV_FILE.unlink()
                log.info(f"🧹 중간 파일 삭제 완료: {CSV_FILE.name}")

    except Exception as e:
        log.exception(f"❌ 실행 중 오류 발생: {e}")
        sys.exit(1)
    finally:
        driver.quit()
        log.info("[*] Chrome 세션 종료 완료")

if __name__ == "__main__":
    main()
