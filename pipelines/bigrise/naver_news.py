#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Naver Finance 뉴스(101/258) 섹션 다건 수집기 (Prefect 파이프라인 대응 버전)
------------------------------------------------
- 섹션: 401(시황), 402(기업), 403(해외), 404(채권), 406(공시), 429(환율)
- 기능: HTML 저장 → CSV 집계 → 기사 본문(contents) 추가
- 경로 구조: project-root/out/naver/, project-root/logs/, project-root/html_dump/
- 제어: .env에서 KEEP_TEMP=true 설정 시 중간 CSV 보존
"""

import os, re, csv, html, time, random, shutil, logging, sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, urljoin, urlparse, parse_qs
from typing import List, Optional, Tuple

import requests
import argparse
from bs4 import BeautifulSoup
from tqdm import tqdm
from dotenv import load_dotenv
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =====================================================
# 경로 설정 (Prefect 환경 호환)
# =====================================================
BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR = BASE_DIR / "out" / "naver"
LOG_DIR = BASE_DIR / "logs"
HTML_DUMP_DIR = BASE_DIR / "html_dump"

OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# =====================================================
# 로깅 설정
# =====================================================
log_path = LOG_DIR / f"naver_news_{time.strftime('%Y%m%d')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# =====================================================
# 환경설정 로드
# =====================================================
load_dotenv()
KEEP_TEMP = os.getenv("KEEP_TEMP", "false").lower() in ("1", "true", "yes")

# =====================================================
# 기본 상수
# =====================================================
BASE = "https://finance.naver.com"
PATH = "/news/news_list.naver"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://finance.naver.com/",
    "Accept-Language": "ko,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

SECTION3_MAP = {
    401: "시황", 402: "기업", 403: "해외",
    404: "채권", 406: "공시", 429: "환율",
}

PATTERN = re.compile(
    r'<dd class="articleSubject">\s*'
    r'<a href="([^"]+)"[^>]*title="([^"]+)">[^<]+</a>\s*</dd>\s*'
    r'<dd class="articleSummary">[\s\S]*?'
    r'<span class="press">([^<]+)</span>[\s\S]*?'
    r'<span class="wdate">([^<]+)</span>',
    re.DOTALL
)

# =====================================================
# 주요 유틸 함수
# =====================================================
def decode_euckr(content: bytes) -> str:
    return content.decode("cp949", errors="replace")

def build_url(date: str, page: int, section3: int) -> str:
    return f"{BASE}{PATH}?{urlencode({'mode':'LSS3D','section_id':'101','section_id2':'258','section_id3':str(section3),'date':date,'page':page})}"

def parse_max_page(html_text: str) -> int:
    soup = BeautifulSoup(html_text, "lxml")
    nums = [int(m.group(1)) for a in soup.find_all("a", href=re.compile(r"page=\d+"))
            if (m := re.search(r"page=(\d+)", a.get("href", "")))]
    return max(set(nums)) if nums else 1

def normalize_news_url(raw_href: str) -> Tuple[str, str, str]:
    href = html.unescape(raw_href or "")
    m = re.search(r"/mnews/article/(\d{3})/(\d+)", href)
    if m:
        oid, aid = m.group(1), m.group(2)
        return oid, aid, f"https://n.news.naver.com/mnews/article/{oid}/{aid}"
    p = urlparse(href)
    qs = parse_qs(p.query)
    for oid, aid in [
        (qs.get("oid", [None])[0], qs.get("aid", [None])[0]),
        (qs.get("office_id", [None])[0], qs.get("article_id", [None])[0]),
    ]:
        if oid and aid:
            return oid, aid, f"https://n.news.naver.com/mnews/article/{oid}/{aid}"
    return "", "", urljoin(BASE, href)

# =====================================================
# HTML 수집
# =====================================================
def fetch_one(date: str, page: int, section3: int, timeout=(5, 15), verify=False, retries: int = 3) -> str:
    url = build_url(date, page, section3)
    backoff = 1.0
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout, verify=verify)
            if resp.status_code in (429,) or 500 <= resp.status_code < 600:
                time.sleep(backoff + random.random())
                backoff *= 2
                continue
            resp.raise_for_status()
            return decode_euckr(resp.content)
        except Exception as e:
            log.warning(f"[WARN] attempt {attempt+1} fail {url}: {e}")
            if attempt == retries - 1:
                raise
            time.sleep(backoff + random.random())
            backoff *= 2

def save_html(html_text: str, date: str, page: int, section3: int, out_dir: Path = HTML_DUMP_DIR) -> str:
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"naver_news_list_{date}_s{section3}_p{page}.html"
    path = out_dir / fname
    path.write_text(html_text, encoding="euc-kr", errors="replace")
    return str(path)

# =====================================================
# HTML 저장 및 CSV 집계
# =====================================================
def save_all_with_sleep_multi(date: str, section3_list: List[int], out_dir: Path = HTML_DUMP_DIR, concurrency: int = 4):
    out_dir.mkdir(parents=True, exist_ok=True)
    saved_all = []
    max_pages_map = {}
    for s in section3_list:
        html_text = fetch_one(date, 1, s)
        max_page = parse_max_page(html_text)
        save_html(html_text, date, 1, section3=s)
        max_pages_map[s] = max_page
        log.info(f"Section {s}({SECTION3_MAP.get(s)}) → {max_page} pages")

    tasks = [(s, p) for s in section3_list for p in range(2, max_pages_map[s] + 1)]
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = {ex.submit(fetch_one, date, p, s): (s, p) for s, p in tasks}
        for fut in tqdm(as_completed(futs), total=len(futs), desc="fetch pages"):
            s, p = futs[fut]
            try:
                html_text = fut.result()
                saved_all.append(save_html(html_text, date, p, section3=s))
            except Exception as e:
                log.warning(f"[WARN] s{s} p{p} fail: {e}")
    return saved_all

def parse_one_file(path: Path) -> list[dict]:
    m = re.search(r"_s(\d+)_p(\d+)\.html$", path.name)
    section3 = int(m.group(1)) if m else None
    section_name = SECTION3_MAP.get(section3, "")
    text = path.read_text(encoding="euc-kr", errors="replace")
    items = []
    for href, title, press, wdate in PATTERN.findall(text):
        oid, aid, norm_url = normalize_news_url(href)
        items.append({
            "section_name": section_name, "section_id3": section3,
            "office_id": oid, "article_id": aid, "url": norm_url,
            "title": html.unescape(title).strip(), "press": html.unescape(press).strip(),
            "wdate": html.unescape(wdate).strip(), "source_file": path.name,
        })
    return items

def aggregate_news_multi(date: str, in_dir: Path, out_dir: Path) -> str:
    out_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(in_dir.glob(f"naver_news_list_{date}_s*_p*.html"))
    all_rows = []
    for fp in tqdm(files, desc="parse html", unit="file"):
        all_rows.extend(parse_one_file(fp))
    deduped = { (r["url"], r["title"]): r for r in all_rows }.values()
    out_path = out_dir / f"naver_news_{date}.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "section_name","section_id3","office_id","article_id","url",
            "title","press","wdate","source_file"
        ])
        writer.writeheader()
        writer.writerows(deduped)
    log.info(f"[DONE] {len(deduped)} rows → {out_path}")
    return str(out_path)

# =====================================================
# 기사 본문 수집 + 정리
# =====================================================
def fetch_article_text(url: str, retries=3) -> str:
    for _ in range(retries):
        try:
            res = requests.get(url, headers=HEADERS, timeout=(5, 15), verify=False)
            if res.status_code >= 400:
                return ""
            res.encoding = res.apparent_encoding or "utf-8"
            soup = BeautifulSoup(res.text, "lxml")
            dic = soup.select_one("div#dic_area") or soup.find("article")
            return dic.get_text(" ", strip=True) if dic else ""
        except Exception:
            time.sleep(1)
    return ""

def enrich_csv_with_contents_threaded(input_csv: str) -> str:
    in_path = Path(input_csv)
    out_path = in_path.with_name(in_path.stem + "_with_contents.csv")
    rows = list(csv.DictReader(in_path.open("r", encoding="utf-8")))
    for r in rows:
        r["contents"] = ""
    with ThreadPoolExecutor(max_workers=6) as ex:
        fut_map = {ex.submit(fetch_article_text, r["url"]): i for i, r in enumerate(rows)}
        for fut in tqdm(as_completed(fut_map), total=len(fut_map), desc="fetch articles"):
            i = fut_map[fut]
            rows[i]["contents"] = fut.result()
            time.sleep(random.uniform(0.3, 1.0))
    fieldnames = list(rows[0].keys())
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"[DONE] 본문 포함 CSV 완료 → {out_path}")
    return str(out_path)

def cleanup_html_dump(dump_dir: Path = HTML_DUMP_DIR):
    if dump_dir.exists():
        shutil.rmtree(dump_dir)
        log.info(f"[CLEANUP] Deleted folder: {dump_dir}")

# =====================================================
# 메인 실행
# =====================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=time.strftime("%Y%m%d"))
    args = parser.parse_args()
    try:
        target_date = args.date
        sections = [401, 402, 403, 404, 406, 429]
        save_all_with_sleep_multi(target_date, sections)
        csv_path = aggregate_news_multi(target_date, in_dir=HTML_DUMP_DIR, out_dir=OUT_DIR)
        final_csv = enrich_csv_with_contents_threaded(csv_path)
        cleanup_html_dump()

        # ✅ 중간 CSV 삭제/보존 제어
        if KEEP_TEMP:
            log.info(f"🗂 중간 파일 보존 (.env KEEP_TEMP=true): {Path(csv_path).name}")
        else:
            os.remove(csv_path)
            log.info(f"🧹 중간 파일 삭제 완료: {Path(csv_path).name}")

        log.info(f"[✅] Naver 뉴스 파이프라인 완료 ({target_date})")

    except Exception as e:
        log.exception(f"❌ 실행 중 오류 발생: {e}")
        sys.exit(1)
