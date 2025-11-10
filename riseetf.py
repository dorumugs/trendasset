#!/usr/bin/env python3
import csv, json, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from datetime import datetime
from pathlib import Path
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = "https://riseetf.co.kr"
URL = f"{BASE}/prod/finder"

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "connection": "keep-alive",
    "cookie": "_ga=GA1.1.1323944014.1760681288; ETF_SESSIONID1=6396318a-935e-4814-a10c-517956f719f8; JSESSIONID=Jf6mpXTainfji2kJo1vVcOul7mCALRDerPYTa41CD1oo8XP1tnRtNBkfmnSv0P1t.amV1c19kb21haW4vZXRm; _ga_83VQBEQXZ2=GS2.1.s1762493466$o9$g1$t1762494131$j60$l0$h0",
    "host": "riseetf.co.kr",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
}

# -----------------------------
# ① ETF 기본 목록 수집
# -----------------------------
def scrape_rise_finder():
    session = requests.Session()
    r = session.get(URL, headers=HEADERS, timeout=20, verify=False)
    r.raise_for_status()
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
    out_dir = Path("out/riseETF")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"rise_finder_{today}.csv"

    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ {len(rows)}개 종목 저장 완료 → {out_csv}")
    return out_csv


# -----------------------------
# ② ETF 구성 내역 수집 (tab3)
# -----------------------------
def fetch_holdings(detail_url: str):
    """상세 페이지의 tab3 구성내역을 리스트[dict]로 반환"""
    url = detail_url if "?" in detail_url else detail_url + "?searchFlag=viewtab3"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15, verify=False)
        r.raise_for_status()
    except Exception as e:
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


# -----------------------------
# ③ ThreadPoolExecutor 병렬 크롤링
# -----------------------------
def enrich_with_holdings_threaded(csv_path: Path, max_workers: int = 15):
    out_dir = csv_path.parent
    out_csv = out_dir / (csv_path.stem + "_with_holdings.csv")

    # 원본 CSV 읽기
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    # 병렬 실행
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
                print(f"⚠️ {row['name']} 실패: {e}")
            results.append(row)

    # CSV 저장
    fieldnames = list(results[0].keys())
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\n💾 완성 → {out_csv}")
    return out_csv


# -----------------------------
# ④ holdings 풀어서 flatten CSV 생성
# -----------------------------
def flatten_holdings(input_csv: Path):
    out_csv = input_csv.with_name(input_csv.stem + "_flattened.csv")

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

    # 저장
    fieldnames = ["name", "price", "change", "detail_url",
                  "number", "item_name", "item_code", "base_price", "ratio", "value"]

    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flat_rows)

    print(f"✅ Flattened CSV 생성 완료 → {out_csv}")
    print(f"총 {len(flat_rows)}행 변환됨")
    return out_csv


# -----------------------------
# 실행
# -----------------------------
if __name__ == "__main__":
    csv_path = scrape_rise_finder()                                # ① 기본 ETF 리스트
    enriched_csv = enrich_with_holdings_threaded(csv_path)          # ② holdings 추가
    flatten_holdings(enriched_csv)                                  # ③ flatten CSV 생성
