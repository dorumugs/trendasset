#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RISE ETF 구성내역 크롤러 (Prefect 파이프라인 대응 버전)
------------------------------------------------
- ETF Finder 페이지에서 목록 및 각 ETF 보유 종목(tab3) 크롤링
- 구성내역 JSON → flatten CSV 변환
- .env 기반 KEEP_TEMP 설정 지원 (중간파일 자동삭제)
- 경로 구조: project-root/out/riseETF/, project-root/logs/
"""

import os, csv, json, time, logging, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import urllib3

# =====================================================
# 경로 설정 (Prefect 환경 호환)
# =====================================================
BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "out" / "riseETF"
LOG_DIR = BASE_DIR / "logs"

OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# =====================================================
# 로깅 설정
# =====================================================
log_path = LOG_DIR / f"riseetf_{time.strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# =====================================================
# 환경변수 로드 (.env)
# =====================================================
load_dotenv()
KEEP_TEMP = os.getenv("KEEP_TEMP", "false").lower() in ("1", "true", "yes")

# =====================================================
# 기본 상수 설정
# =====================================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = "https://riseetf.co.kr"
URL = f"{BASE}/prod/finder"
HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "connection": "keep-alive",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
}

# =====================================================
# ① ETF 기본 목록 수집
# =====================================================
def scrape_rise_finder() -> Path:
    log.info("[*] ETF Finder 페이지 수집 중 ...")
    session = requests.Session()
    try:
        r = session.get(URL, headers=HEADERS, timeout=20, verify=False)
        r.raise_for_status()
    except Exception as e:
        log.exception(f"❌ RISE ETF 페이지 요청 실패: {e}")
        sys.exit(1)

    soup = BeautifulSoup(r.text, "html.parser")
    rows = []
    for tr in soup.select("table tbody tr"):
        th = tr.select_one("th")
        if not th:
            continue
        name = th.get_text(strip=True)
        onclick = th.get("onclick", "")
        detail_path = onclick.split("'")[1] if "'" in onclick else ""
        detail_url = urljoin(BASE, detail_path)

        tds = tr.select("td")
        if len(tds) >= 2:
            price = tds[0].get_text(strip=True)
            change_tag = tds[1]
            direction = change_tag.select_one("span.blind")
            direction_text = direction.get_text(strip=True) if direction else ""
            change_val = change_tag.get_text(strip=True).replace(direction_text, "")
            change = f"{direction_text} {change_val}".strip()
        else:
            price = change = ""

        rows.append({
            "name": name,
            "price": price,
            "change": change,
            "detail_url": detail_url
        })

    today = datetime.now().strftime("%Y%m%d")
    out_csv = OUT_DIR / f"rise_finder_{today}.csv"

    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    log.info(f"✅ {len(rows)}개 종목 저장 완료 → {out_csv}")
    return out_csv

# =====================================================
# ② ETF 구성내역(tab3) 수집
# =====================================================
def fetch_holdings(detail_url: str):
    """상세 페이지의 tab3 구성내역을 리스트[dict]로 반환"""
    url = detail_url if "?" in detail_url else detail_url + "?searchFlag=viewtab3"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15, verify=False)
        r.raise_for_status()
    except Exception as e:
        log.warning(f"⚠️ 요청 실패: {url} ({e})")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    tbody = soup.select_one('tbody[data-class="tab3PdfList"]')
    if not tbody:
        return []

    holdings = []
    for tr in tbody.select("tr"):
        th = tr.select_one("th")
        tds = tr.select("td")
        if len(tds) == 5:
            holdings.append({
                "번호": th.get_text(strip=True) if th else "",
                "종목명": tds[0].get_text(strip=True),
                "종목코드": tds[1].get_text(strip=True),
                "기준가": tds[2].get_text(strip=True),
                "비중(%)": tds[3].get_text(strip=True),
                "평가액": tds[4].get_text(strip=True),
            })
    return holdings

# =====================================================
# ③ ThreadPoolExecutor 병렬 크롤링
# =====================================================
def enrich_with_holdings_threaded(csv_path: Path, max_workers: int = 10) -> Path:
    out_csv = OUT_DIR / (csv_path.stem + "_with_holdings.csv")

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    log.info(f"[*] ETF 구성내역 수집 시작 ({len(rows)}개 종목) ...")

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_row = {executor.submit(fetch_holdings, row["detail_url"]): row for row in rows}
        for future in tqdm(as_completed(future_to_row), total=len(rows), desc="Fetching holdings"):
            row = future_to_row[future]
            try:
                holdings = future.result()
                row["holdings"] = json.dumps(holdings, ensure_ascii=False)
            except Exception as e:
                row["holdings"] = "[]"
                log.warning(f"⚠️ {row['name']} 실패: {e}")
            results.append(row)
            time.sleep(0.1)

    fieldnames = list(results[0].keys())
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    log.info(f"💾 구성내역 수집 완료 → {out_csv}")
    return out_csv

# =====================================================
# ④ holdings 풀어서 flatten CSV 생성
# =====================================================
def flatten_holdings(input_csv: Path) -> Path:
    out_csv = OUT_DIR / (input_csv.stem + "_flattened.csv")

    with open(input_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    flat_rows = []
    for row in rows:
        holdings_raw = row.get("holdings", "")
        try:
            holdings = json.loads(holdings_raw)
        except json.JSONDecodeError:
            holdings = []
        for h in holdings:
            flat_rows.append({
                "name": row["name"],
                "price": row["price"],
                "change": row["change"],
                "detail_url": row["detail_url"],
                "number": h.get("번호", ""),
                "item_name": h.get("종목명", ""),
                "item_code": h.get("종목코드", ""),
                "base_price": h.get("기준가", ""),
                "ratio": h.get("비중(%)", ""),
                "value": h.get("평가액", ""),
            })

    fieldnames = ["name","price","change","detail_url","number","item_name","item_code","base_price","ratio","value"]
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flat_rows)

    log.info(f"✅ Flattened CSV 생성 완료 → {out_csv} ({len(flat_rows)}행)")
    return out_csv

# =====================================================
# ⑤ 메인 실행 (KEEP_TEMP 기반 중간 파일 정리)
# =====================================================
if __name__ == "__main__":
    try:
        log.info("🚀 RISE ETF 크롤링 시작")

        csv_path = scrape_rise_finder()                         # ① 기본 ETF 리스트
        enriched_csv = enrich_with_holdings_threaded(csv_path)   # ② holdings 추가
        final_csv = flatten_holdings(enriched_csv)               # ③ flatten 최종본 생성

        if KEEP_TEMP:
            log.info("🗂 중간 파일 보존 (.env KEEP_TEMP=true)")
        else:
            for fp in [csv_path, enriched_csv]:
                try:
                    if Path(fp).exists():
                        Path(fp).unlink()
                        log.info(f"🧹 중간 파일 삭제 완료: {Path(fp).name}")
                except Exception as e:
                    log.warning(f"[WARN] 중간 파일 삭제 실패: {fp} ({e})")

        log.info(f"✅ RISE ETF 파이프라인 완료 → {Path(final_csv).name}")

    except Exception as e:
        log.exception(f"❌ 실행 중 오류 발생: {e}")
        sys.exit(1)
