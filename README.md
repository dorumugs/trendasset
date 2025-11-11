# 🧭 TrendAsset: Automated Financial Data Pipeline (Updated)

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
  - Naver 뉴스 → Rise ETF → BigFinance 순서로 병렬·직렬 조합  
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
  ├── rise_finder_YYYYMMDD.csv
  ├── rise_finder_YYYYMMDD_with_holdings.csv
  └── rise_finder_YYYYMMDD_with_holdings_flattened.csv
  ```

- **CSV 스키마 예시**

  | 컬럼명         | 설명                          |
  | -------------- | ----------------------------- |
  | `etf_name`     | ETF 이름                      |
  | `etf_code`     | ETF 코드                      |
  | `category`     | ETF 유형                      |
  | `provider`     | 운용사                        |
  | `num_holdings` | 보유 종목 수                  |
  | `as_of`        | 기준일                        |
  | `holding_name` | 보유 종목명                   |
  | `holding_code` | 종목 코드                     |
  | `weight`       | 비중(%)                       |
  | `market`       | 시장 구분 (KOSPI / NASDAQ 등) |

---

### 📰 (3) Naver Finance News Pipeline

- **파일:** `pipelines/bigrise/naver_news.py`

- **기능:**  

  - 네이버 금융 뉴스(섹션: 시황, 기업, 해외, 채권, 공시, 환율) 크롤링  
  - HTML 저장 → CSV 집계 → 기사 본문(`contents`) 추가  
  - ThreadPoolExecutor + tqdm으로 병렬 수집 및 로깅  

- **출력 파일 구조**

  ```
  out/naver/
  ├── naver_news_YYYYMMDD.csv
  └── naver_news_YYYYMMDD_with_contents.csv
  ```

- **CSV 스키마 예시**

  | 컬럼명     | 설명                                      |
  | ---------- | ----------------------------------------- |
  | `title`    | 뉴스 제목                                 |
  | `summary`  | 요약문                                    |
  | `url`      | 기사 URL                                  |
  | `press`    | 언론사                                    |
  | `date`     | 게시일 (YYYY-MM-DD)                       |
  | `section`  | 뉴스 섹션 (시황/기업/해외 등)             |
  | `contents` | 기사 본문 텍스트 (본문 수집 완료 시 추가) |

---

### 🧮 (4) BigFinance Industry Pipeline

- **파일:** `pipelines/bigrise/bigfinance.py`

- **기능:**  

  - BigFinance API(`/api/industry/categories`) 호출로 산업·기업 메타정보 수집  
  - `frequency`, `source`, `companies` 등 메타 필드 포함  
  - RISE ETF 산업 매칭 로직과 연계 가능  

- **출력 파일 구조**

  ```
  out/bigfinance/
  ├── industry_categories_YYYYMMDD.csv
  └── industry_categories_YYYYMMDD_with_meta_companies.csv
  ```

- **CSV 스키마 예시**

  | 컬럼명          | 설명                          |
  | --------------- | ----------------------------- |
  | `main_name`     | 산업 대분류                   |
  | `sub_name`      | 산업 소분류                   |
  | `data_name`     | 데이터 항목명                 |
  | `frequency`     | 데이터 주기 (월/분기/연간 등) |
  | `source`        | 데이터 출처                   |
  | `companies`     | 산업에 속한 기업명 리스트     |
  | `category_code` | 카테고리 코드 (API 반환값)    |
  | `last_updated`  | 데이터 기준일                 |

---

### 🧰 (5) 공통 유틸 및 배포

| 파일                        | 설명                                               |
| --------------------------- | -------------------------------------------------- |
| `pipelines/common/tasks.py` | Prefect Task 공통 정의 (`run_script`, `notify` 등) |
| `pipelines/deploy_all.py`   | 모든 Flow를 Prefect에 자동 등록 및 배포            |

---

## 🧱 3. 디렉토리 구조

```
trendasset/
├── pipelines/
│   ├── bigrise/
│   │   ├── bigrise_pre.py
│   │   ├── bigrise.py
│   │   ├── riseetf.py
│   │   ├── bigfinance.py
│   │   └── naver_news.py
│   ├── common/
│   │   └── tasks.py
│   └── deploy_all.py
├── out/
│   ├── riseETF/
│   ├── naver/
│   └── bigfinance/
├── logs/
│   ├── bigrise_YYYYMMDD.log
│   ├── bigfinance_YYYYMMDD.log
│   └── naver_news_YYYYMMDD.log
├── out_sample/
├── prefect.yaml
├── prefect_config.toml
├── .env_sample
└── README.md
```

---

## ⚡ 4. 실행 및 배포

### (1) 환경 설정

```bash
cp .env_sample .env
# .env 내부에 계정 정보 및 BASE_URL 입력
```

### (2) Prefect 서버 실행

```bash
prefect server start
```

### (3) 워커 등록

```bash
prefect work-pool create default
prefect worker start --pool default
```

### (4) 파이프라인 배포

```bash
python pipelines/deploy_all.py
```

### (5) 개별 Flow 실행

```bash
prefect deployment run "BigRise Pipeline"
```

---

## 🧩 5. 환경 변수(.env_sample)

| 변수명       | 설명                   | 예시                       |
| ------------ | ---------------------- | -------------------------- |
| `BASE_URL`   | BigFinance API 주소    | `https://bigfinance.co.kr` |
| `LOGIN_PAGE` | 로그인 페이지 경로     | `/login`                   |
| `USERNAME`   | 계정 아이디            | `user@example.com`         |
| `PASSWORD`   | 계정 비밀번호          | `yourpassword`             |
| `HEADLESS`   | Selenium 헤드리스 여부 | `true`                     |

---

## 📊 6. 출력 데이터 스키마 요약

| 경로              | 설명                         | 주요 컬럼                        |
| ----------------- | ---------------------------- | -------------------------------- |
| `out/riseETF/`    | RISE ETF 리스트 및 구성 종목 | etf_name · holding_name · weight |
| `out/bigfinance/` | 산업 메타데이터              | main_name · sub_name · companies |
| `out/naver/`      | 뉴스 기사 데이터             | title · press · contents         |

---

## 📑 7. 라이선스

이 프로젝트는 [Apache License 2.0](./LICENSE)을 따릅니다.

---

> 📌 **최종 업데이트:** 2025-11-11  
> 🧠 **Maintainer:** PearlCow  
> 💬 **문의:** dorumugs@gmail.com