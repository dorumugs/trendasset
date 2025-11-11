# 🧭 TrendAsset: Automated Financial Data Pipeline (Final)

**TrendAsset**은 금융시장 데이터(ETF 구성, 산업별 메타정보, 뉴스)를 자동 수집·가공하는 **Prefect 기반 데이터 파이프라인 프로젝트**입니다.  
Naver Finance, BigFinance, Rise ETF 데이터를 정기적으로 수집하고 CSV로 집계합니다.

---

## 📦 1. 프로젝트 개요

| 구분          | 내용                                                         |
| ------------- | ------------------------------------------------------------ |
| **목적**      | 금융시장 관련 뉴스·산업·ETF 데이터를 자동 수집 및 가공       |
| **핵심 기술** | Python · BeautifulSoup · Requests · Prefect · ThreadPoolExecutor |
| **출력 형식** | CSV (UTF-8)                                                  |
| **출력 경로** | `./out/`                                                     |
| **로그 경로** | `./logs/`                                                    |

---

## ⚙️ 2. 파이프라인 구성

### 🧩 (1) BigRise Main Pipeline

- **파일:** `pipelines/bigrise/bigrise.py`
- **기능:**  
  - 전체 데이터 파이프라인을 Prefect Flow로 통합 실행  
  - Naver 뉴스 → Rise ETF → BigFinance → BigRise Industry Matching 순서로 수행  
  - Prefect 스케줄러 기반 자동화 배치 지원  
  - 기준일(`target_date`)은 Flow Run 시간 기준 전일로 자동 계산  

---

### 💹 (2) Rise ETF Pipeline

- **파일:** `pipelines/bigrise/riseetf.py`

- **기능:**  

  - RISE ETF Finder 페이지에서 ETF 목록 및 보유종목 크롤링  
  - 보유내역 JSON → 평탄화(`flatten`) 후 CSV 저장  
  - Prefect Task 및 tqdm 기반 병렬 수집  

- **출력 파일 구조**

  ```
  out/riseETF/
  ├── rise_finder_YYYYMMDD.csv # KEEP_TEMP = True
  ├── rise_finder_YYYYMMDD_with_holdings.csv # KEEP_TEMP = True
  └── rise_finder_YYYYMMDD_with_holdings_flattened.csv
  ```

---

### 📰 (3) Naver Finance News Pipeline

- **파일:** `pipelines/bigrise/naver_news.py`

- **기능:**  

  - 네이버 금융 뉴스(시황, 기업, 해외, 채권, 공시, 환율) 크롤링  
  - HTML 저장 → CSV 집계 → 기사 본문(`contents`) 추가  
  - ThreadPoolExecutor + tqdm으로 병렬 수집  

- **출력 파일 구조**

  ```
  out/naver/
  ├── naver_news_YYYYMMDD.csv # KEEP_TEMP = True
  └── naver_news_YYYYMMDD_with_contents.csv
  ```

---

### 🧮 (4) BigFinance Industry Pipeline

- **파일:** `pipelines/bigrise/bigfinance.py`

- **기능:**  

  - BigFinance API(`/api/industry/categories`) 호출로 산업·기업 메타정보 수집  
  - `frequency`, `source`, `companies` 등 메타 필드 포함  
  - 환경 변수 `KEEP_TEMP` 값이 `true`이면 임시 CSV(`industry_categories_YYYYMMDD.csv`)를 보존  

- **출력 파일 구조**

  ```
  out/bigfinance/
  ├── industry_categories_YYYYMMDD.csv # KEEP_TEMP = True
  └── industry_categories_YYYYMMDD_with_meta_companies.csv
  ```

---

### 🧠 (5) BigRise Industry Matching Pipeline (신규)

- **파일:** `pipelines/bigrise/bigrise_pre.py`

- **기능:**  

  - RISE ETF 구성내역(`rise_finder_*_flattened.csv`)과 BigFinance 산업기업(`industry_*_meta_companies.csv`)을 매칭  
  - 각 ETF 구성종목에 `industry_info`, `industry_frequency`, `industry_source`, `industry_update_date` 추가  
  - 최근 7일 이내 업데이트된 산업이 포함된 ETF만 별도로 저장  

- **출력 파일 구조**

  ```
  out/bigRise/
  ├── bigrise_YYYYMMDD.csv 
  └── bigrise_recent_YYYYMMDD.csv
  ```

---

## ⚡ 3. 실행 및 배포

```bash
cp .env_sample .env
prefect server start
prefect work-pool create default
prefect worker start --pool default
python pipelines/deploy_all.py
prefect deployment run "BigRise Pipeline"
```

---

## 🧩 4. 환경 변수(.env_sample)

| 변수명            | 설명                                   | 예시                        |
| ----------------- | -------------------------------------- | --------------------------- |
| `BASE_URL`        | BigFinance API 주소                    | `https://bigfinance.co.kr`  |
| `LOGIN_PAGE`      | 로그인 페이지 경로                     | `/login`                    |
| `USERNAME`        | 계정 아이디                            | `user@example.com`          |
| `PASSWORD`        | 계정 비밀번호                          | `yourpassword`              |
| `HEADLESS`        | Selenium 헤드리스 여부                 | `true`                      |
| `PREFECT_API_URL` | Prefect 서버 API 엔드포인트            | `http://127.0.0.1:4200/api` |
| `KEEP_TEMP`       | 임시 데이터 보존 여부 (`true`/`false`) | `false`                     |

---

## 📊 5. 출력 데이터 스키마 요약

| 경로              | 설명                         | 주요 컬럼                                   |
| ----------------- | ---------------------------- | ------------------------------------------- |
| `out/riseETF/`    | RISE ETF 리스트 및 구성 종목 | etf_name · holding_name · weight            |
| `out/bigfinance/` | 산업 메타데이터              | main_name · sub_name · companies            |
| `out/naver/`      | 뉴스 기사 데이터             | title · press · contents                    |
| `out/bigRise/`    | ETF–산업 매칭 결과           | item_name · industry_info · industry_source |

---

## 📑 6. 라이선스

이 프로젝트는 [Apache License 2.0](./LICENSE)을 따릅니다.

---

> 📌 **최종 업데이트:** 2025-11-11  
> 🧠 **Maintainer:** PearlCow  
> 💬 **문의:** dorumugs@gmail.com