
# 자동화된 투자 지표 생성 파이프라인 

## 📖 프로젝트 소개 
Apache Airflow를 기반으로 구축된 자동화 데이터 파이프라인 프로젝트입니다.  
분산된 여러 데이터 소스(국내외 금 시세, 네이버 블로그)로부터 데이터를 수집하고, LangGraph 기반의 LLM 에이전트를 통해 텍스트 데이터에서 투자 인사이트를 추출합니다.  
최종적으로 생성된 차익거래(Arbitrage) 관련 지표와 인사이트는 데이터베이스에 저장되어, 투자 전략 수립에 필요한 데이터를 체계적으로 제공하는 것을 목표로 합니다.

<hr style="height: 1px; border: 0; background-color: #e0e0e0;" />


### ✨ 주요 기능 

#### 📈 실시간 금 가격 차익거래 지표 생성

KRX 금 시장의 1Kg 단위 금 가격(99.99%)을 주기적으로 크롤링합니다.

국제 금 시세 API를 통해 실시간 글로벌 가격을 수집합니다.

두 가격 간의 차이(프리미엄/디스카운트)를 계산하여 차익거래 기회를 포착할 수 있는 지표를 생성하고 저장합니다.


#### 🤖 LangGraph 기반 투자 인사이트 추출

수집된 텍스트 데이터(예: 네이버 블로그)를 LangGraph로 구축된 LLM 에이전트에 전달합니다.

텍스트의 핵심 투자 아이디어를 분석하고 , 추천 액션(Buy/Hold/Sell)을 분석하여 투자 결정에 참고할 수 있는 정성적 데이터를 생성합니다.

기본 분석 모델은 `gemini-3-flash-preview`이며, `BLOG_LLM_MODEL` 환경변수 또는 Airflow Variable `blog_llm_model`로 변경할 수 있습니다.

실제 분석을 실행하려면 Airflow 컨테이너에 `GOOGLE_API_KEY`가 전달되어야 합니다.


#### 📰 네이버 블로그 콘텐츠 수집

관심 키워드에 대한 네이버 블로그 RSS 피드를 주기적으로 확인하여 신규 게시물을 감지합니다.

신규 포스트가 감지되면 해당 URL의 본문 텍스트 전체를 크롤링하여 분석용 데이터로 저장합니다.

블로그를 처음 등록하면 최근 2일치 글을 1회 백필하고, 이후에는 신규 글만 수집합니다.

운영 중 Docker가 내려가 있었다가 다시 올라오면, 블로그별 마지막 성공 RSS 수집 시각 기준으로 복구 구간을 다시 확인해 아직 RSS에 남아 있는 신규 글을 재수집합니다.

#### 🧭 블로그 검증 대시보드

FastAPI + Jinja2 + HTMX 기반의 로컬 대시보드를 통해 다음 작업을 수행할 수 있습니다.

- 블로그 URL 등록 및 비활성화
- 블로그별 수집 글 목록 조회
- 마지막 성공 RSS 수집 시각 확인
- 분석 로그 및 투자 시그널 확인
- 원문 링크를 직접 열어 LLM 결과 검증

<hr style="height: 1px; border: 0; background-color: #e0e0e0;" />

### 🛠️ 기술 스택
Orchestration: Apache Airflow

Data Collection: Python, Requests, Playwright

AI / LLM: LangGraph, Gemini

Database: MySQL, PostgreSQL 18

Containerization: Docker

<hr style="height: 1px; border: 0; background-color: #e0e0e0;" />

### 💾 데이터베이스 스키마 

본 프로젝트의 데이터베이스는 **두 가지 주요 데이터 흐름**을 중심으로 설계되었습니다.

1.  **금 시세 차익거래 지표 생성**: 국내(KRX) 및 해외(KB) 금 시세 데이터를 각각 수집하고, 두 가격의 차이를 계산하여 최종 차익거래 지표를 생성합니다.
2.  **블로그 기반 투자 인사이트 추출**: 팔로우 하는 네이버 블로그의 신규 포스트를 감지하고, 본문을 수집한 뒤, LangGraph 에이전트를 통해 분석하여 로그와 최종 투자 시그널을 단계적으로 생성합니다.

아래는 전체 테이블의 관계를 나타낸 ERD입니다.

<p align="center">
  <img width="434" height="469" alt="Database Schema Diagram" src="https://github.com/user-attachments/assets/e683a262-43d4-46bd-ad42-ace849ef4511" />
</p>

#### 주요 테이블 상세 설명

  * **krx\_gold\_price\_minute**: KRX 금 시장의 분 단위 시세 데이터를 저장합니다.

      * **관련 DAG**: krx\_gold\_price\_extraction.py
      * **설명**: Airflow에 의해 주기적으로 실행되며, KRX 데이터 API를 호출하여 국내 금 시세 데이터를 가져옵니다. INSERT IGNORE 구문을 사용하여 새로운 데이터만 효율적으로 추가합니다.

  * **kb\_global\_gold\_price**: KB 국민은행 웹사이트에서 고시하는 국제 금 시세 및 환율 데이터를 저장합니다.

      * **관련 DAG**: kb\_gold\_price\_extraction.py
      * **설명**: KB 국민은행의 골드바 가격 고시 페이지를 주기적으로 크롤링하여, 국제 금 가격(USD/트로이온스)과 원달러 환율 정보를 수집합니다. 이 데이터는 국내 금 가격과의 비교를 위한 기준으로 사용됩니다.

  * **gold\_price\_premium**: 국내외 금 가격을 종합하여 계산된 프리미엄/디스카운트 지표를 저장합니다.

      * **관련 DAG**: (별도 계산 DAG에서 사용)
      * **설명**: krx\_gold\_price\_minute의 국내 가격과 kb\_global\_gold\_price의 국제 가격 및 환율을 사용하여, 국내 금 시장의 프리미엄(또는 디스카운트)을 계산한 최종 결과가 저장되는 테이블입니다. 이 테이블의 데이터가 실질적인 차익거래 기회 포착을 위한 핵심 지표가 됩니다.

  * **blog\_posts\_rss** & **crawl\_check**: RSS 피드를 통해 수집한 블로그 포스트의 메타데이터를 저장하고, 본문 크롤링 여부를 1:1 관계로 추적합니다.

      * **관련 DAG**: naver\_blog\_blog\_rss\_checker.py
      * **설명**: naver\_blog\_list 테이블(관리용)에 등록된 블로그의 RSS 피드를 주기적으로 확인합니다. 새로운 게시물이 감지되면, 게시물 정보는 blog\_posts\_rss에, 크롤링 대기 상태는 crawl\_check에 하나의 트랜잭션으로 동시에 저장됩니다. crawl\_check는 크롤링 파이프라인의 대기열(Queue) 역할을 수행합니다.

  * **crawl\_result**: Playwright를 이용해 크롤링한 블로그 본문 텍스트 원본과 처리 상태를 저장하는 테이블입니다.

      * **관련 DAG**: naver\_blog\_crawling.py
      * **설명**: crawl\_check 테이블에서 아직 크롤링되지 않은(crawled = FALSE) URL 목록을 가져옵니다. Playwright를 사용하여 해당 URL의 본문 텍스트 전체를 비동기적으로 스크래핑하고, 성공/실패 여부와 함께 원본 텍스트를 이 테이블에 저장합니다. 크롤링 성공 시 crawl\_check 테이블의 상태를 TRUE로 업데이트합니다.

  * **processing\_logs**: crawl\_result의 텍스트를 LangGraph 기반 LLM 에이전트로 분석한 기록(성공/실패)을 저장합니다.

      * **관련 DAG**: investment\_signal\_extraction.py
      * **설명**: 아직 처리되지 않은(is\_processed = 0) crawl\_result 데이터를 대상으로 LLM 분석을 수행합니다. 분석 시작 시 'failed' 상태로 로그를 먼저 생성하고, 모든 과정이 성공적으로 완료되면 'success'로 상태를 업데이트합니다. 에러 발생 시 상세한 오류 메시지를 기록하여 추적을 용이하게 합니다.

  * **investment\_signals**: LLM 분석을 통해 최종적으로 추출된 투자 시그널(티커, 추천 액션, 근거, 신뢰도 점수)을 저장합니다.

      * **관련 DAG**: investment\_signal\_extraction.py
      * **설명**: processing\_logs의 분석이 성공했을 때, LangGraph 파이프라인이 Pydantic 모델 스키마에 맞춰 추출한 구조화된 투자 시그널 데이터를 저장합니다. 하나의 분석(log\_id)에서 여러 개의 투자 시그널이 나올 수 있는 1:N 관계를 가집니다.


#### 향후 추가 예정 부분
- 프로젝트 구조 (Architecture)

### 블로그 파이프라인(PostgreSQL 18) 초기화

블로그 수집/크롤링/분석 파이프라인과 대시보드는 로컬 PostgreSQL 18의 `insight_extraction` DB를 사용합니다.

```bash
createdb insight_extraction
psql -d insight_extraction -f sql/init_blog_pipeline.sql
```

`.env`에는 최소한 아래 블로그 파이프라인 설정이 필요합니다.

```bash
BLOG_DB_USER=your_local_postgres_user
BLOG_DB_HOST=host.docker.internal
BLOG_DB_PORT=5432
BLOG_DB_NAME=insight_extraction
BLOG_LLM_MODEL=gemini-3-flash-preview
GOOGLE_API_KEY=your_gemini_api_key
```

Airflow 블로그 파이프라인 Connection ID는 `insight_extraction_pg`입니다.

기본 연결 문자열:

```bash
postgresql://kanghyoseung@host.docker.internal:5432/insight_extraction
```

Airflow UI에서 직접 등록하려면 아래 값으로 생성합니다.

1. Conn Type: `Postgres`
2. Conn Id: `insight_extraction_pg`
3. Host: `host.docker.internal`
4. Port: `5432`
5. Schema: `insight_extraction`
6. Login: 로컬 PostgreSQL 사용자명

### 대시보드 실행

대시보드는 기본적으로 `http://localhost:8090/blogs`에서 확인할 수 있습니다.

- `/blogs/{blog_id}`: 글별 최신 분석 결과와 재분석 버튼을 확인할 수 있습니다.
- `/signals`: 최근 7일, 최근 1달, 전체 기간 기준으로 BUY/SELL 종목 집계를 확인할 수 있습니다.
- `/posts/{guid}`: 최신 분석 결과, 전체 로그 이력, 원문 링크, 크롤링 본문을 함께 확인할 수 있습니다.
- 재분석 버튼은 기존 로그를 지우지 않고 `crawl_result`를 다시 큐에 넣은 뒤 Airflow 분석 DAG를 즉시 호출합니다.
- 대표 결과는 항상 각 글의 가장 최근 분석 시도 1건 기준으로 표시됩니다.

```bash
docker compose up --build dashboard
```

Airflow까지 포함해 전체 환경을 띄우려면:

```bash
docker compose up --build
```

동작 방식:

1. 시작 직후 `blog-pipeline-bootstrap` 서비스가 블로그 DAG 3개를 자동 `unpause`하고 `rss_blog_post_collector`를 즉시 1회 실행합니다.
2. 이후에는 매시간 RSS 수집이 돌고, 수집 완료 후 크롤링 DAG, 크롤링 완료 후 분석 DAG가 자동으로 이어집니다.
3. Docker가 꺼져 있는 동안에는 수집/분석이 멈추지만, 블로그 운영 데이터는 로컬 PostgreSQL `insight_extraction`에 유지됩니다.
4. Docker를 다시 켜면 bootstrap이 다시 실행되고, 각 블로그는 `last_successful_rss_checked_at - 1 hour`부터 RSS를 다시 확인해 놓친 글을 복구합니다.
5. 복구는 RSS feed에 아직 남아 있는 게시물 범위 안에서만 가능합니다.

참고:

1. Airflow 메타데이터용 컨테이너 Postgres는 호스트 포트 `5433`을 사용하도록 바꿔 로컬 PostgreSQL 18과 충돌하지 않게 했습니다.
2. Docker 컨테이너가 로컬 PostgreSQL 18에 접근하려면 PostgreSQL이 `host.docker.internal`에서 오는 연결을 허용해야 합니다.
3. 블로그 운영 데이터는 로컬 PostgreSQL `insight_extraction`에 저장되고, Airflow 컨테이너 Postgres는 메타데이터 전용입니다.
