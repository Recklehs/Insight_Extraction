import json
import os
import re
import traceback
from datetime import UTC, datetime
from typing import List, Literal, Optional

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field


BLOG_DB_CONN_ID = "insight_extraction_pg"
DEFAULT_LLM_MODEL_NAME = "gemini-3-flash-preview"
ALLOWED_ACTIONS = {"buy", "hold", "sell"}
MAX_SIGNALS_PER_POST = 3


def resolve_llm_model_name() -> str:
    env_default = os.getenv("BLOG_LLM_MODEL", DEFAULT_LLM_MODEL_NAME)
    try:
        return Variable.get("blog_llm_model", default_var=env_default)
    except Exception:
        return env_default


def require_env_var(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    raise ValueError(f"{name}가 .env 파일에 설정되지 않았습니다. 확인해주세요.")


def normalize_text_fragment(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def build_core_ideas_system_prompt() -> str:
    return (
        "당신은 긴 글에서 저자가 명시적으로 주장한 핵심 투자 아이디어만 추출하는 분석가입니다.\n"
        "다음 규칙을 반드시 지키세요.\n"
        "1. 저자가 명시적으로 주장한 투자 아이디어만 남기고, 단순 배경 설명, 비교 대상, 시장 뉴스, 거시 코멘트는 제외합니다.\n"
        "2. ticker는 실제 추천 대상 자산만 적고, 식별이 모호하면 추측하지 말고 제외합니다.\n"
        "3. evidence는 본문에 실제로 존재하는 1~2문장 근거 문장 그대로 사용합니다.\n"
        "4. 근거 문장이 해당 자산 또는 명확한 별칭을 지지하지 않으면 제외합니다.\n"
        "5. 중복 자산은 하나로 합치고, confidence_score가 가장 높은 결과만 남깁니다.\n"
        "6. 최종 결과는 confidence_score 내림차순 최대 3개만 반환합니다.\n"
        "7. 명확한 투자 아이디어가 없으면 is_idea_present는 false, core_ideas는 빈 리스트입니다.\n"
        "반드시 지정된 JSON 형식으로만 답변해야 합니다.\n\n"
        "{format_instructions}"
    )


def post_process_signals(cleaned_text: str, raw_signals: list[dict] | None) -> list[dict]:
    normalized_text = normalize_text_fragment(cleaned_text)
    deduped: dict[str, dict] = {}

    for raw_signal in raw_signals or []:
        ticker = normalize_text_fragment(str(raw_signal.get("ticker", "")))
        action = normalize_text_fragment(str(raw_signal.get("action", ""))).lower()
        evidence = normalize_text_fragment(str(raw_signal.get("evidence", "")))
        confidence_score = raw_signal.get("confidence_score")

        try:
            confidence = float(confidence_score)
        except (TypeError, ValueError):
            continue

        if not ticker or not evidence or action not in ALLOWED_ACTIONS:
            continue
        if evidence not in normalized_text:
            continue

        candidate = {
            "ticker": ticker,
            "action": action,
            "evidence": evidence,
            "confidence_score": confidence,
        }
        dedupe_key = ticker.casefold()
        existing = deduped.get(dedupe_key)
        if existing is None or candidate["confidence_score"] > existing["confidence_score"]:
            deduped[dedupe_key] = candidate

    ordered = sorted(
        deduped.values(),
        key=lambda signal: (-signal["confidence_score"], signal["ticker"].casefold()),
    )
    return ordered[:MAX_SIGNALS_PER_POST]


class Signal(BaseModel):
    ticker: str = Field(description="식별된 주식, 섹터 또는 코인 티커")
    action: Literal["buy", "hold", "sell"] = Field(description="추천 액션")
    evidence: str = Field(description="추천 액션의 근거가 되는 핵심 문장")
    confidence_score: float = Field(description="추천 액션에 대한 신뢰도 점수", ge=0.0, le=1.0)


class CoreIdeasResult(BaseModel):
    is_idea_present: bool = Field(description="텍스트에 명확한 핵심 투자 아이디어가 존재하는지 여부")
    core_ideas: List[Signal] = Field(description="추출된 핵심 투자 시그널 리스트")


class GraphState(dict):
    original_text: str
    cleaned_text: Optional[str] = None
    results: Optional[List[dict]] = None
    llm_model_name: str


def preprocess_text(state: GraphState) -> GraphState:
    print("--- 1. 텍스트 전처리 중 ---")
    original_text = state["original_text"]
    cleaned = re.sub(r"<[^>]+>", "", original_text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return {"cleaned_text": cleaned}


def extract_core_ideas(state: GraphState) -> GraphState:
    print("--- 2. 핵심 투자 아이디어 추출 중 ---")
    parser = PydanticOutputParser(pydantic_object=CoreIdeasResult)
    llm = ChatGoogleGenerativeAI(model=state["llm_model_name"], temperature=0)
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                build_core_ideas_system_prompt(),
            ),
            ("human", "분석할 텍스트:\n```{text}```"),
        ]
    ).partial(format_instructions=parser.get_format_instructions())

    chain = prompt | llm | parser
    response = chain.invoke({"text": state["cleaned_text"]})

    if response.is_idea_present and response.core_ideas:
        results_as_dicts = post_process_signals(
            state["cleaned_text"],
            [idea.model_dump() for idea in response.core_ideas],
        )
        print(f"   > ✅ 핵심 아이디어 {len(results_as_dicts)}개 발견!")
        print(f"   > 추출된 시그널: {json.dumps(results_as_dicts, indent=2, ensure_ascii=False)}")
        if not results_as_dicts:
            print("   > ❌ 후처리 결과 저장 가능한 시그널이 없습니다.")
        return {"results": results_as_dicts}

    print("   > ❌ 명확한 핵심 투자 아이디어를 찾을 수 없습니다.")
    return {"results": []}


workflow = StateGraph(GraphState)
workflow.add_node("preprocess_text", preprocess_text)
workflow.add_node("extract_core_ideas", extract_core_ideas)
workflow.set_entry_point("preprocess_text")
workflow.add_edge("preprocess_text", "extract_core_ideas")
workflow.add_edge("extract_core_ideas", END)
app = workflow.compile()


def get_db_connection():
    try:
        hook = PostgresHook(postgres_conn_id=BLOG_DB_CONN_ID)
        conn = hook.get_conn()
        print("✅ 데이터베이스 연결 성공 (via Airflow Hook)")
        return conn
    except Exception as exc:
        print(f"❌ 데이터베이스 연결 실패 (via Airflow Hook): {exc}")
        raise


def process_and_store_item(conn, crawl_item):
    crawl_guid = crawl_item[0]
    crawl_text = crawl_item[1]
    llm_model_name = resolve_llm_model_name()
    log_id = None

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO processing_logs (crawl_result_guid, status, llm_model, processed_at)
            VALUES (%s, %s, %s, %s)
            RETURNING log_id;
            """,
            (crawl_guid, "failed", llm_model_name, datetime.now(UTC)),
        )
        log_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        print(f"📘 [{crawl_guid}] 분석 시작. Log ID: {log_id}")

        inputs = {"original_text": crawl_text, "llm_model_name": llm_model_name}
        final_state = app.invoke(inputs)
        signals = final_state.get("results", [])

        cursor = conn.cursor()
        if signals:
            cursor.executemany(
                """
                INSERT INTO investment_signals (log_id, ticker, action, evidence, confidence_score)
                VALUES (%s, %s, %s, %s, %s);
                """,
                [
                    (log_id, signal["ticker"], signal["action"], signal["evidence"], signal["confidence_score"])
                    for signal in signals
                ],
            )
            print(f"   > 📈 시그널 {len(signals)}개 저장 완료.")

        cursor.execute(
            "UPDATE processing_logs SET status = %s, processed_at = %s WHERE log_id = %s",
            ("success", datetime.now(UTC), log_id),
        )
        cursor.execute(
            "UPDATE crawl_result SET is_processed = TRUE, processed_at = %s WHERE guid = %s",
            (datetime.now(UTC), crawl_guid),
        )
        conn.commit()
        print(f"✅ [{crawl_guid}] 모든 처리 성공적으로 완료.")
    except Exception as exc:
        print(f"❌ [{crawl_guid}] 처리 중 에러 발생: {exc}")
        conn.rollback()
        if log_id is not None:
            error_cursor = conn.cursor()
            error_cursor.execute(
                """
                UPDATE processing_logs
                SET status = %s, error_message = %s, processed_at = %s
                WHERE log_id = %s;
                """,
                ("failed", traceback.format_exc(), datetime.now(UTC), log_id),
            )
            conn.commit()
            error_cursor.close()
    finally:
        try:
            cursor.close()
        except Exception:
            pass


def main_processor():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT guid, crawled_text
            FROM crawl_result
            WHERE is_processed = FALSE
              AND http_status_code = 200
              AND crawled_text IS NOT NULL
            ORDER BY crawled_at ASC NULLS LAST;
            """
        )
        items_to_process = cursor.fetchall()
        cursor.close()

        print(f"🚀 총 {len(items_to_process)}개의 새로운 항목을 처리합니다.")
        if items_to_process:
            require_env_var("GOOGLE_API_KEY")

        for item in items_to_process:
            process_and_store_item(conn, item)

        print("🎉 모든 작업 완료.")
    except Exception as exc:
        print(f"🔥 메인 프로세서에서 심각한 오류 발생: {exc}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("🚪 데이터베이스 연결 종료.")


if __name__ == "__main__":
    main_processor()
