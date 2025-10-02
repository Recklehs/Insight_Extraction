import os
import re
import json
import traceback
from datetime import datetime, timezone
from typing import List, Literal, Optional

# .env 파일 관리를 위한 라이브러리
from dotenv import load_dotenv

# MySQL 연동을 위한 라이브러리
import mysql.connector

# LangChain 및 Google Generative AI 관련 라이브러리
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field

# LangGraph 관련 라이브러리
from langgraph.graph import StateGraph, END

# --- 0. 환경 설정 ---
# .env 파일에서 환경 변수를 로드합니다.
load_dotenv()

# 환경 변수 확인
required_env_vars = ["GOOGLE_API_KEY", "DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
for var in required_env_vars:
    if var not in os.environ:
        raise ValueError(f"{var}가 .env 파일에 설정되지 않았습니다. 확인해주세요.")

# 사용할 LLM 모델 이름
LLM_MODEL_NAME = "gemini-1.5-flash"


# --- 1. 최종 출력 및 LLM 응답 스키마 정의 (Pydantic 모델) ---
class Signal(BaseModel):
    """추출된 개별 투자 시그널 정보를 담는 데이터 구조"""
    ticker: str = Field(description="식별된 주식 또는 코인 티커 (예: 'AAPL', 'BTC')")
    action: Literal["buy", "hold", "sell"] = Field(description="추천 액션: 'buy', 'hold', 'sell' 중 하나")
    evidence: str = Field(description="추천 액션의 근거가 되는 원본 텍스트 내의 핵심 문장")
    confidence_score: float = Field(
        description="추천 액션에 대한 LLM의 신뢰도 점수 (0.0 ~ 1.0)",
        ge=0.0,
        le=1.0
    )

class CoreIdeasResult(BaseModel):
    """텍스트의 핵심 투자 아이디어 분석 결과를 담는 데이터 구조"""
    is_idea_present: bool = Field(
        description="텍스트에 저자가 주장하는 명확한 핵심 투자 아이디어가 존재하는지 여부 (True/False)"
    )
    core_ideas: List[Signal] = Field(
        description="추출된 모든 핵심 투자 시그널의 리스트. is_idea_present가 false이면 빈 리스트입니다."
    )

# --- 2. LangGraph 상태(State) 객체 정의 ---
class GraphState(dict):
    """그래프의 각 노드 간에 전달되고 업데이트되는 데이터 구조"""
    original_text: str
    cleaned_text: Optional[str] = None
    results: Optional[List[dict]] = None

# --- 3. 노드(Node) 및 엣지(Edge) 함수 설계 ---
# Gemini LLM 객체를 초기화합니다.
llm = ChatGoogleGenerativeAI(model=LLM_MODEL_NAME, temperature=0)

def preprocess_text(state: GraphState) -> GraphState:
    """원본 텍스트에서 불필요한 공백, HTML 태그 등을 제거합니다."""
    print("--- 1. 텍스트 전처리 중 ---")
    original_text = state['original_text']
    cleaned = re.sub(r'<[^>]+>', '', original_text)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return {"cleaned_text": cleaned}

def extract_core_ideas(state: GraphState) -> GraphState:
    """LLM을 사용하여 텍스트의 핵심 투자 아이디어 여러 개를 추출합니다."""
    print("--- 2. 핵심 투자 아이디어 추출 중 ---")
    parser = PydanticOutputParser(pydantic_object=CoreIdeasResult)
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", "당신은 월스트리트의 최고 투자 전략가입니다. 당신의 임무는 긴 글에서 저자가 가장 강력하게 주장하는 '핵심 투자 아이디어들'을 모두 정확하게 포착하는 것입니다.\n주어진 텍스트 전체의 논지를 파악하여 다음 항목을 추출해주세요:\n1. is_idea_present: 글에 명확한 핵심 투자 아이디어가 있는지 여부 (True/False)\n2. core_ideas: 아이디어가 있다면, 그 대상(ticker), 추천 액션(action), 가장 강력한 근거 문장(evidence), 그리고 당신의 판단에 대한 신뢰도 점수(confidence_score)를 포함하는 객체들의 '리스트'.\n본문의 핵심 결론으로 제시된 주식이나 섹터에 대한 추천만 포함하고, 단순 비교 대상이나 배경 설명으로 언급된 종목은 제외하세요. 만약 명확한 핵심 아이디어가 없다면 'is_idea_present'를 false로 설정하고 'core_ideas'를 빈 리스트로 만드세요.\n반드시 지정된 JSON 형식으로만 답변해야 합니다.\n\n{format_instructions}"),
            ("human", "분석할 텍스트:\n```{text}```"),
        ]
    ).partial(format_instructions=parser.get_format_instructions())

    chain = prompt | llm | parser
    cleaned_text = state['cleaned_text']
    response = chain.invoke({"text": cleaned_text})

    if response.is_idea_present and response.core_ideas:
        print(f"   > ✅ 핵심 아이디어 {len(response.core_ideas)}개 발견!")
        results_as_dicts = [idea.model_dump() for idea in response.core_ideas]
        print(f"   > 추출된 시그널: {json.dumps(results_as_dicts, indent=2, ensure_ascii=False)}")
        return {"results": results_as_dicts}
    else:
        print("   > ❌ 명확한 핵심 투자 아이디어를 찾을 수 없습니다.")
        return {"results": []}

# --- 4. 그래프 구성 및 컴파일 ---
workflow = StateGraph(GraphState)
workflow.add_node("preprocess_text", preprocess_text)
workflow.add_node("extract_core_ideas", extract_core_ideas)
workflow.set_entry_point("preprocess_text")
workflow.add_edge("preprocess_text", "extract_core_ideas")
workflow.add_edge("extract_core_ideas", END)
app = workflow.compile()


# --- 5. 데이터베이스 연동 및 처리 로직 ---
def get_db_connection():
    """데이터베이스 커넥션을 생성하고 반환합니다."""
    try:
        conn = mysql.connector.connect(
            host=os.environ["DB_HOST"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            database=os.environ["DB_NAME"]
        )
        print("✅ 데이터베이스 연결 성공")
        return conn
    except mysql.connector.Error as e:
        print(f"❌ 데이터베이스 연결 실패: {e}")
        raise

def process_and_store_item(conn, crawl_item):
    """단일 크롤링 아이템을 분석하고 결과를 DB에 저장합니다."""
    cursor = conn.cursor()
    crawl_guid = crawl_item[0]
    crawl_text = crawl_item[1]
    log_id = None

    try:
        # 1. processing_logs에 분석 시작 기록 삽입
        log_insert_query = """
        INSERT INTO processing_logs (crawl_result_guid, status, llm_model, processed_at)
        VALUES (%s, %s, %s, %s)
        """
        # 초기 상태는 'failed'로 설정하고 성공 시 'success'로 업데이트
        cursor.execute(log_insert_query, (crawl_guid, 'failed', LLM_MODEL_NAME, datetime.now(timezone.utc)))
        log_id = cursor.lastrowid
        print(f"📘 [{crawl_guid}] 분석 시작. Log ID: {log_id}")

        # 2. LangGraph 파이프라인 실행
        inputs = {"original_text": crawl_text}
        final_state = app.invoke(inputs)
        signals = final_state.get("results", [])

        # 3. investment_signals에 결과 저장
        if signals:
            signal_insert_query = """
            INSERT INTO investment_signals (log_id, ticker, action, evidence, confidence_score)
            VALUES (%s, %s, %s, %s, %s)
            """
            signal_data = [
                (log_id, s['ticker'], s['action'], s['evidence'], s['confidence_score'])
                for s in signals
            ]
            cursor.executemany(signal_insert_query, signal_data)
            print(f"   > 📈 시그널 {len(signals)}개 저장 완료.")

        # 4. processing_logs 상태 'success'로 업데이트
        cursor.execute("UPDATE processing_logs SET status = 'success' WHERE log_id = %s", (log_id,))

        # 5. crawl_result 상태 'is_processed = 1'로 업데이트
        cursor.execute("UPDATE crawl_result SET is_processed = 1, processed_at = %s WHERE guid = %s", (datetime.now(timezone.utc), crawl_guid))
        
        conn.commit()
        print(f"✅ [{crawl_guid}] 모든 처리 성공적으로 완료.")

    except Exception as e:
        print(f"❌ [{crawl_guid}] 처리 중 에러 발생: {e}")
        # 에러 메시지를 processing_logs에 기록
        if log_id:
            error_message = traceback.format_exc()
            cursor.execute("UPDATE processing_logs SET error_message = %s WHERE log_id = %s", (error_message, log_id))
        conn.rollback() # 에러 발생 시 모든 변경사항 롤백
    finally:
        cursor.close()

def main_processor():
    """
    Airflow에서 호출할 메인 함수.
    DB에서 처리되지 않은 데이터를 가져와 분석하고 저장합니다.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # is_processed가 0인 데이터 조회
        cursor.execute("SELECT guid, crawled_text FROM crawl_result WHERE is_processed = 0")
        items_to_process = cursor.fetchall()
        
        print(f"🚀 총 {len(items_to_process)}개의 새로운 항목을 처리합니다.")
        
        for item in items_to_process:
            process_and_store_item(conn, item)
            
        cursor.close()
        print("🎉 모든 작업 완료.")

    except Exception as e:
        print(f"🔥 메인 프로세서에서 심각한 오류 발생: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("🚪 데이터베이스 연결 종료.")


# --- 6. 직접 실행을 위한 스크립트 ---
if __name__ == "__main__":
    main_processor()
