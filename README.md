# 🧠 trendasset

**ETF · 산업데이터 · 뉴스 자동수집 및 매칭 파이프라인**

`trendasset`은  

- **네이버 금융 뉴스**,  
- **RISE ETF 구성내역**,  
- **BigFinance 산업 데이터**를  
  자동으로 수집하고 상호 매칭하여  
  **ETF-산업-기업 연계 데이터셋을 구축**하는 프로젝트입니다.

---

## 📁 디렉토리 구조

```
trendasset/
├── naver_news.py        # 네이버 금융 뉴스 크롤러
├── riseetf.py           # RISE ETF 구성내역 크롤러
├── bigfinance.py        # BigFinance 산업데이터 수집기
├── bigrise_pre.py       # ETF-산업 매칭 스크립트
├── .env                 # BigFinance 로그인정보
└── out/
    ├── naver/           # 뉴스 CSV 결과
    ├── riseETF/         # ETF 목록/구성 결과
    ├── bigfinance/      # 산업데이터 결과
    └── bigrise/         # 매칭 결과
```

---

## ⚙️ 주요 기능 요약

| 모듈                 | 기능                                                  | 출력 파일                                                    |
| -------------------- | ----------------------------------------------------- | ------------------------------------------------------------ |
| **`naver_news.py`**  | 네이버 금융뉴스 HTML → CSV + 기사 본문 추가           | `out/naver/naver_news_YYYYMMDD_with_contents.csv`            |
| **`riseetf.py`**     | RISE ETF 전체 목록 + 구성종목(tab3) + flatten         | `out/riseETF/rise_finder_YYYYMMDD_with_holdings_flattened.csv` |
| **`bigfinance.py`**  | BigFinance 산업 카테고리 + 메타정보 + 기업리스트 수집 | `out/bigfinance/industry_categories_YYYYMMDD_with_meta_companies.csv` |
| **`bigrise_pre.py`** | ETF 구성종목 ↔ 산업 기업명 매칭 후 메타 병합          | `out/bigrise_YYYYMMDD.csv`                                   |

---

## 🪄 실행 순서

```bash
# 1. 네이버 뉴스 수집
python naver_news.py

# 2. RISE ETF 데이터 수집
python riseetf.py

# 3. BigFinance 산업 데이터 수집
python bigfinance.py

# 4. ETF-산업 매칭
python bigrise_pre.py
```

---

## 🧾 결과 파일별 컬럼 설명

### 📰 `naver_news_YYYYMMDD_with_contents.csv`

| 컬럼           | 설명                                              |
| -------------- | ------------------------------------------------- |
| `section_name` | 뉴스 섹션 이름 (시황·기업·해외·채권·공시·환율 등) |
| `section_id3`  | 네이버 금융 3단계 섹션 코드                       |
| `office_id`    | 언론사 ID                                         |
| `article_id`   | 기사 ID                                           |
| `url`          | 뉴스 원문 URL                                     |
| `title`        | 기사 제목                                         |
| `press`        | 언론사 이름                                       |
| `wdate`        | 게재일시                                          |
| `source_file`  | HTML 원본 파일명                                  |
| `contents`     | 기사 본문 텍스트                                  |

---

### 💹 `rise_finder_YYYYMMDD_with_holdings_flattened.csv`

| 컬럼         | 설명                 |
| ------------ | -------------------- |
| `name`       | ETF 이름             |
| `price`      | 현재가               |
| `change`     | 전일 대비 변동 (▲/▼) |
| `detail_url` | ETF 상세 페이지 URL  |
| `number`     | 구성내역 순번        |
| `item_name`  | 구성종목명           |
| `item_code`  | 종목코드             |
| `base_price` | 기준가               |
| `ratio`      | 비중(%)              |
| `value`      | 평가액               |

---

### 🏭 `industry_categories_YYYYMMDD_with_meta_companies.csv`

| 컬럼                     | 설명                                     |
| ------------------------ | ---------------------------------------- |
| `main_code`, `main_name` | 산업 대분류 코드 및 이름                 |
| `group_id`, `group_name` | 산업 그룹 ID 및 이름                     |
| `sub_code`, `sub_name`   | 산업 세부분류 코드 및 이름               |
| `update_date`            | 업데이트 일자                            |
| `data_type`              | 데이터 타입 (예: 시계열/통계 등)         |
| `data_code`, `data_name` | 데이터 세부코드 및 이름                  |
| `last_update`            | 최종 갱신일                              |
| `frequency`              | 데이터 갱신주기 (월, 분기 등)            |
| `unit`                   | 단위 (예: %, 억원 등)                    |
| `source`                 | 데이터 출처                              |
| `footnote`               | 각주/비고                                |
| `yoyFlag`                | 전년대비 여부 플래그                     |
| `updateDate`             | 헤더 정보 기준 업데이트일                |
| `companies`              | 산업에 속한 기업 리스트 (JSON 배열 형식) |

---

### 🔗 `bigrise_YYYYMMDD.csv`

| 컬럼                                                         | 설명                                        |
| ------------------------------------------------------------ | ------------------------------------------- |
| (이전 동일) `name`, `price`, `change`, `detail_url`, `number`, `item_name`, `item_code`, `base_price`, `ratio`, `value` | ETF 기본 및 구성정보                        |
| `industry_info`                                              | 매칭된 산업정보 (`sub_name-data_name` 형식) |
| `industry_frequency`                                         | 해당 산업데이터의 갱신주기                  |
| `industry_source`                                            | 데이터 출처 (예: 통계청, 산업통상자원부 등) |

---

## 🧰 필요 패키지

```bash
pip install requests beautifulsoup4 lxml tqdm pandas selenium python-dotenv
```

> **주의:**  
>
> - ChromeDriver 설치 필요  
> - `.env` 파일에 BigFinance 로그인 정보 저장 필요  

---

## 📊 예시 출력 경로

```
out/
├── naver/naver_news_20251106_with_contents.csv
├── riseETF/rise_finder_20251107_with_holdings_flattened.csv
├── bigfinance/industry_categories_20251107_with_meta_companies.csv
└── bigrise_20251107.csv
```

---

## 👤 Author
 
- **Maintainer:** Kayser So  
- **GitHub:** [dorumugs/trendasset](https://github.com/dorumugs/trendasset)