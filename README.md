
# 자동화된 투자 지표 생성 파이프라인 (Automated Investment Indicator Pipeline)

## 📖 프로젝트 소개 (Overview)
Apache Airflow를 기반으로 구축된 자동화 데이터 파이프라인 프로젝트입니다.  
분산된 여러 데이터 소스(국내외 금 시세, 네이버 블로그)로부터 데이터를 수집하고, LangGraph 기반의 LLM 에이전트를 통해 텍스트 데이터에서 투자 인사이트를 추출합니다.  
최종적으로 생성된 차익거래(Arbitrage) 관련 지표와 인사이트는 데이터베이스에 저장되어, 투자 전략 수립에 필요한 데이터를 체계적으로 제공하는 것을 목표로 합니다.

### ✨ 주요 기능 (Key Features)

#### 📈 실시간 금 가격 차익거래 지표 생성

KRX 금 시장의 1Kg 단위 금 가격(99.99%)을 주기적으로 크롤링합니다.

국제 금 시세 API를 통해 실시간 글로벌 가격을 수집합니다.

두 가격 간의 차이(프리미엄/디스카운트)를 계산하여 차익거래 기회를 포착할 수 있는 지표를 생성하고 저장합니다.


#### 🤖 LangGraph 기반 투자 인사이트 추출

수집된 텍스트 데이터(예: 네이버 블로그)를 LangGraph로 구축된 LLM 에이전트에 전달합니다.

텍스트의 핵심 투자 아이디어를 분석하고 , 추천 액션(Buy/Hold/Sell)을 분석하여 투자 결정에 참고할 수 있는 정성적 데이터를 생성합니다.


#### 📰 네이버 블로그 콘텐츠 수집

관심 키워드에 대한 네이버 블로그 RSS 피드를 주기적으로 확인하여 신규 게시물을 감지합니다.

신규 포스트가 감지되면 해당 URL의 본문 텍스트 전체를 크롤링하여 분석용 데이터로 저장합니다.

### 🛠️ 기술 스택 (Tech Stack)
Orchestration: Apache Airflow

Data Collection: Python, Requests, Playwright

AI / LLM: LangGraph, Gemini

Database: MySQL

Containerization: Docker

### 💾 데이터베이스 스키마 (Database Schema)

본 프로젝트의 데이터베이스는 **두 가지 주요 데이터 흐름**을 중심으로 설계되었습니다.

1.  **금 시세 차익거래 지표 생성**: 국내(KRX) 및 해외(KB) 금 시세 데이터를 각각 수집하고, 두 가격의 차이를 계산하여 최종 차익거래 지표를 생성합니다.
2.  **블로그 기반 투자 인사이트 추출**: 네이버 블로그의 신규 포스트를 감지(`blog_posts_rss`, `crawl_check`)하고, 본문을 수집(`crawl_result`)한 뒤, LangGraph 에이전트를 통해 분석하여 로그(`processing_logs`)와 최종 투자 시그널(`investment_signals`)을 단계적으로 생성합니다.

아래는 전체 테이블의 관계를 나타낸 ERD입니다.


<p align="center">
  <img width="434" height="469" alt="Database Schema Diagram" src="https://github.com/user-attachments/assets/e683a262-43d4-46bd-ad42-ace849ef4511" />
</p>

#### 주요 테이블 상세 설명

  * **`krx_gold_price_minute`**: KRX 금 시장의 분 단위 시세 데이터를 저장합니다.

      * **관련 DAG**: `krx_gold_price_extraction.py`
      * **설명**: Airflow에 의해 주기적으로 실행되며, KRX 데이터 API를 호출하여 국내 금 시세 데이터를 가져옵니다. `INSERT IGNORE` 구문을 사용하여 새로운 데이터만 효율적으로 추가합니다.

  * **`kb_global_gold_price`**: KB 국민은행 웹사이트에서 고시하는 국제 금 시세 및 환율 데이터를 저장합니다.

      * **관련 DAG**: `kb_gold_price_extraction.py`
      * **설명**: KB 국민은행의 골드바 가격 고시 페이지를 주기적으로 크롤링하여, 국제 금 가격(USD/트로이온스)과 원달러 환율 정보를 수집합니다. 이 데이터는 국내 금 가격과의 비교를 위한 기준으로 사용됩니다.

  * **`gold_price_premium`**: 국내외 금 가격을 종합하여 계산된 프리미엄/디스카운트 지표를 저장합니다.

      * **관련 DAG**: (별도 계산 DAG에서 사용)
      * **설명**: `krx_gold_price_minute`의 국내 가격과 `kb_global_gold_price`의 국제 가격 및 환율을 사용하여, 국내 금 시장의 프리미엄(또는 디스카운트)을 계산한 최종 결과가 저장되는 테이블입니다. 이 테이블의 데이터가 실질적인 차익거래 기회 포착을 위한 핵심 지표가 됩니다.

  * **`blog_posts_rss`** & **`crawl_check`**: RSS 피드를 통해 수집한 블로그 포스트의 메타데이터를 저장하고, 본문 크롤링 여부를 1:1 관계로 추적합니다.

      * **관련 DAG**: `naver_blog_blog_rss_checker.py`
      * **설명**: `naver_blog_list` 테이블(관리용)에 등록된 블로그의 RSS 피드를 주기적으로 확인합니다. 새로운 게시물이 감지되면, 게시물 정보는 `blog_posts_rss`에, 크롤링 대기 상태는 `crawl_check`에 하나의 트랜잭션으로 동시에 저장됩니다. `crawl_check`는 크롤링 파이프라인의 대기열(Queue) 역할을 수행합니다.

  * **`crawl_result`**: Playwright를 이용해 크롤링한 블로그 본문 텍스트 원본과 처리 상태를 저장하는 테이블입니다.

      * **관련 DAG**: `naver_blog_crawling.py`
      * **설명**: `crawl_check` 테이블에서 아직 크롤링되지 않은(`crawled = FALSE`) URL 목록을 가져옵니다. Playwright를 사용하여 해당 URL의 본문 텍스트 전체를 비동기적으로 스크래핑하고, 성공/실패 여부와 함께 원본 텍스트를 이 테이블에 저장합니다. 크롤링 성공 시 `crawl_check` 테이블의 상태를 `TRUE`로 업데이트합니다.

  * **`processing_logs`**: `crawl_result`의 텍스트를 LangGraph 기반 LLM 에이전트로 분석한 기록(성공/실패)을 저장합니다.

      * **관련 DAG**: `investment_signal_extraction.py`
      * **설명**: 아직 처리되지 않은(`is_processed = 0`) `crawl_result` 데이터를 대상으로 LLM 분석을 수행합니다. 분석 시작 시 'failed' 상태로 로그를 먼저 생성하고, 모든 과정이 성공적으로 완료되면 'success'로 상태를 업데이트합니다. 에러 발생 시 상세한 오류 메시지를 기록하여 추적을 용이하게 합니다.

  * **`investment_signals`**: LLM 분석을 통해 최종적으로 추출된 투자 시그널(티커, 추천 액션, 근거, 신뢰도 점수)을 저장합니다.

      * **관련 DAG**: `investment_signal_extraction.py`
      * **설명**: `processing_logs`의 분석이 성공했을 때, LangGraph 파이프라인이 Pydantic 모델 스키마에 맞춰 추출한 구조화된 투자 시그널 데이터를 저장합니다. 하나의 분석(`log_id`)에서 여러 개의 투자 시그널이 나올 수 있는 1:N 관계를 가집니다.




#### 향후 추가 예정 부분
- 프로젝트 구조 (Architecture)
