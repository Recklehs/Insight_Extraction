
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

#### 향후 추가 예정 부분
- 프로젝트 구조 (Architecture)
- DB 스키마 구조 
