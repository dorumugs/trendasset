# -*- coding: utf-8 -*-
"""
Naver Finance 뉴스(101/258) 섹션 다건 수집기
- 섹션: 401(시황), 402(기업), 403(해외), 404(채권), 406(공시), 429(환율)
- 기능: HTML 저장 -> CSV 집계 -> 기사 본문(contents) 추가
- 안전: 랜덤 sleep, 동시성 제한, 429/5xx 재시도(지수 백오프), tqdm 진행률
필요패키지: requests, beautifulsoup4, lxml, tqdm
"""

import os, re, csv, html, time, random, shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, urljoin, urlparse, parse_qs
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -----------------------------
# 상수 및 공통 설정
# -----------------------------
BASE = "https://finance.naver.com"
PATH = "/news/news_list.naver"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://finance.naver.com/",
    "Accept-Language": "ko,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# 섹션 코드 → 한글 이름 (표시용)
SECTION3_MAP = {
    401: "시황",
    402: "기업",
    403: "해외",
    404: "채권",
    406: "공시",
    429: "환율",
}

# 뉴스 리스트 HTML에서 기사 항목 정규식
PATTERN = re.compile(
    r'<dd class="articleSubject">\s*'
    r'<a href="([^"]+)"[^>]*title="([^"]+)">[^<]+</a>\s*</dd>\s*'
    r'<dd class="articleSummary">[\s\S]*?'
    r'<span class="press">([^<]+)</span>[\s\S]*?'
    r'<span class="wdate">([^<]+)</span>',
    re.DOTALL
)

# -----------------------------
# 유틸 함수
# -----------------------------
def decode_euckr(content: bytes) -> str:
    return content.decode("cp949", errors="replace")

def build_url(date: str, page: int, section3: int) -> str:
    return f"{BASE}{PATH}?{urlencode({'mode':'LSS3D','section_id':'101','section_id2':'258','section_id3':str(section3),'date':date,'page':page})}"

def parse_max_page(html_text: str) -> int:
    soup = BeautifulSoup(html_text, "lxml")
    nums = []
    for a in soup.find_all("a", href=re.compile(r"page=\d+")):
        m = re.search(r"page=(\d+)", a.get("href",""))
        if m:
            nums.append(int(m.group(1)))
        t = (a.get_text() or "").strip()
        if t.isdigit():
            nums.append(int(t))
    return max(set(nums)) if nums else 1

def normalize_news_url(raw_href: str) -> tuple[str, str, str]:
    href = html.unescape(raw_href or "")
    m = re.search(r"/mnews/article/(\d{3})/(\d+)", href)
    if m:
        oid, aid = m.group(1), m.group(2)
        return oid, aid, f"https://n.news.naver.com/mnews/article/{oid}/{aid}"

    p = urlparse(href)
    qs = parse_qs(p.query)
    candidates = [
        (qs.get("office_id", [None])[0], qs.get("article_id", [None])[0]),
        (qs.get("oid", [None])[0],       qs.get("aid", [None])[0]),
        (qs.get("officeId", [None])[0],  qs.get("articleId", [None])[0]),
    ]
    for oid, aid in candidates:
        if oid and aid:
            return oid, aid, f"https://n.news.naver.com/mnews/article/{oid}/{aid}"

    m2 = re.search(r"[?&](?:oid|office_id|officeId)=(\d{3}).*?[&](?:aid|article_id|articleId)=(\d+)", href)
    if m2:
        oid, aid = m2.group(1), m2.group(2)
        return oid, aid, f"https://n.news.naver.com/mnews/article/{oid}/{aid}"

    abs_url = urljoin(BASE, href)
    return "", "", abs_url

# -----------------------------
# 네트워크 요청
# -----------------------------
def fetch_one(date: str, page: int, section3: int, timeout=(5, 15), verify=False, retries: int = 3) -> str:
    url = build_url(date, page, section3)
    backoff = 1.0
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout, verify=verify)
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                time.sleep(backoff + random.uniform(0, 0.5))
                backoff *= 2
                continue
            resp.raise_for_status()
            html_text = decode_euckr(resp.content)
            return html_text
        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(backoff + random.uniform(0, 0.5))
            backoff *= 2

def save_html(html_text: str, date: str, page: int, out_dir="html_dump", encoding="euc-kr", section3: Optional[int] = None) -> str:
    os.makedirs(out_dir, exist_ok=True)
    if section3 is None:
        fname = f"naver_news_list_{date}_p{page}.html"
    else:
        fname = f"naver_news_list_{date}_s{section3}_p{page}.html"
    path = os.path.join(out_dir, fname)
    with open(path, "w", encoding=encoding, errors="replace") as f:
        f.write(html_text)
    return path

def save_all_with_sleep_multi(date: str, section3_list: List[int], out_dir="html_dump", concurrency=4, verify_ssl=False, sleep_min=0.5, sleep_max=1.5):
    os.makedirs(out_dir, exist_ok=True)
    saved_all = []
    max_pages_map = {}

    for s in section3_list:
        first_html = fetch_one(date, 1, s, verify=verify_ssl)
        max_page = parse_max_page(first_html)
        max_pages_map[s] = max_page
        save_html(first_html, date, 1, out_dir, section3=s)
        print(f"[INFO] section {s} ({SECTION3_MAP.get(s,'-')}): total pages = {max_page}")

    tasks = [(s, p) for s in section3_list for p in range(2, max_pages_map[s] + 1)]

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = []
        for s, p in tasks:
            fut = ex.submit(fetch_one, date, p, s, (5, 15), verify_ssl)
            futs.append((s, p, fut))
            time.sleep(random.uniform(sleep_min, sleep_max))

        for s, p, fut in tqdm(futs, desc="fetch pages", unit="page"):
            try:
                html_text = fut.result()
                saved_all.append(save_html(html_text, date, p, out_dir, section3=s))
            except Exception as e:
                print(f"[WARN] s{s} p{p} fail: {e}")

    print(f"[DONE] saved {len(saved_all)} pages")
    return saved_all, max_pages_map

# -----------------------------
# HTML 파싱 및 CSV
# -----------------------------
def parse_one_file(path: Path) -> list[dict]:
    m = re.search(r"_s(\d+)_p(\d+)\.html$", path.name)
    section3 = int(m.group(1)) if m else None
    section_name = SECTION3_MAP.get(section3, "") if section3 else ""

    with path.open("r", encoding="euc-kr", errors="replace") as f:
        text = f.read()

    items = []
    for href, title, press, wdate in PATTERN.findall(text):
        oid, aid, norm_url = normalize_news_url(href)
        items.append({
            "section_name": section_name,
            "section_id3": section3,
            "office_id": oid,
            "article_id": aid,
            "url": norm_url,
            "title": html.unescape(title).strip(),
            "press": html.unescape(press).strip(),
            "wdate": html.unescape(wdate).strip(),
            "source_file": str(path.name),
        })
    return items

def aggregate_news_multi(date: str, in_dir="html_dump", out_csv_dir="out/naver"):
    in_dir = Path(in_dir)
    out_dir = Path(out_csv_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(in_dir.glob(f"naver_news_list_{date}_s*_p*.html"))
    all_rows = []
    for fp in tqdm(files, desc="parse html", unit="file"):
        all_rows.extend(parse_one_file(fp))

    seen = set()
    deduped = []
    for row in all_rows:
        key = (row["url"], row["title"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    out_path = out_dir / f"naver_news_{date}.csv"
    fieldnames = [
        "section_name","section_id3","office_id","article_id","url",
        "title","press","wdate","source_file"
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(deduped)

    print(f"[DONE] {len(deduped)} rows → {out_path}")
    return str(out_path)

# -----------------------------
# 기사 본문 수집 (contents 포함)
# -----------------------------
def fetch_article_text(url: str, retries: int = 3) -> str:
    backoff = 1.0
    for attempt in range(retries):
        try:
            res = requests.get(url, headers=HEADERS, timeout=(5, 15), verify=False)
            if res.status_code in (429,) or 500 <= res.status_code < 600:
                time.sleep(backoff + random.uniform(0, 0.5))
                backoff *= 2
                continue
            if res.status_code >= 400:
                return ""
            res.encoding = res.apparent_encoding or "utf-8"
            soup = BeautifulSoup(res.text, "lxml")
            dic = soup.select_one("div#dic_area") or soup.find("article") or soup.select_one("div#newsct_article, div#newsct_article_content")
            return dic.get_text(" ", strip=True) if dic else ""
        except requests.RequestException:
            if attempt == retries - 1:
                return ""
            time.sleep(backoff + random.uniform(0, 0.5))
            backoff *= 2

def enrich_csv_with_contents_threaded(input_csv: str, output_csv: str = None, concurrency: int = 6, sleep_min: float = 0.3, sleep_max: float = 1.0):
    in_path = Path(input_csv)
    if output_csv is None:
        output_csv = str(in_path.with_name(in_path.stem + "_with_contents.csv"))

    with open(in_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []
        if "contents" not in fieldnames:
            fieldnames.append("contents")

    results = ["" for _ in range(len(rows))]
    tasks = [(i, row["url"]) for i, row in enumerate(rows) if row.get("url")]

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        fut_map = {}
        for i, url in tasks:
            fut = ex.submit(fetch_article_text, url)
            fut_map[fut] = i
            time.sleep(random.uniform(sleep_min, sleep_max))

        for fut in tqdm(as_completed(fut_map), total=len(fut_map), desc="fetch articles", unit="art"):
            i = fut_map[fut]
            results[i] = fut.result()

    for i, row in enumerate(rows):
        row["contents"] = results[i]

    # 👇 최종 컬럼 순서 재정렬
    reordered_fields = [
        "section_name","section_id3","office_id","article_id","url",
        "title","press","wdate","source_file","contents"
    ]
    with open(output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=reordered_fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[DONE] {len(rows)} rows → {output_csv}")
    return output_csv

def cleanup_html_dump(dump_dir: str = "html_dump", backup: bool = False) -> None:
    """
    html_dump 폴더 정리 함수
    - backup=True → zip으로 백업 후 삭제
    - backup=False → 바로 삭제
    """
    dump_path = Path(dump_dir)

    if not dump_path.exists():
        print(f"[INFO] No folder found: {dump_path}")
        return

    if backup:
        zip_path = shutil.make_archive(str(dump_path), "zip", root_dir=dump_path)
        print(f"[BACKUP] Folder zipped to: {zip_path}")

    try:
        shutil.rmtree(dump_path)
        print(f"[CLEANUP] Deleted folder: {dump_path}")
    except Exception as e:
        print(f"[WARN] Failed to delete {dump_path}: {e}")

# -----------------------------
# 메인 실행
# -----------------------------
if __name__ == "__main__":
    target_date = "20251106"
    sections = [401, 402, 403, 404, 406, 429]

    save_all_with_sleep_multi(target_date, sections)
    csv_path = aggregate_news_multi(target_date)
    enrich_csv_with_contents_threaded(csv_path)
    # 단순 삭제
    cleanup_html_dump()