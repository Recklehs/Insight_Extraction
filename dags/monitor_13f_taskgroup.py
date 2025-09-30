# dags/monitor_13f_taskgroup.py
from __future__ import annotations

import os
import time
import requests
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
import json

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import Variable
from airflow.utils.timezone import datetime
from airflow.exceptions import AirflowSkipException,AirflowException
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook  # Airflow 2.3+ / 3.x


DAG_ID = "monitor_13f_taskgroup_hourly"
DAG_FOLDER = os.path.dirname(__file__)  

MANAGERS: List[Dict[str, str]] = [
    {"name": "Berkshire Hathaway", "cik": "0001067983"},
    {"name": "Bridgewater Associates", "cik": "0001350694"},
    {"name": "Citadel Advisors", "cik": "0001423053"},
    {"name": "Tiger Global Management", "cik": "0001167483"},
    {"name": "BlackRock Inc.", "cik": "0001364742"},
    {"name": "Pershing Square Capital", "cik": "0001336528"},
    {"name": "National Pension Service (Korea)", "cik": "0001608046"},
]

# SEC User-Agent: 사전 등록 절차 없음. 문자열만 필수로 포함
SEC_USER_AGENT = "My13FMonitor/1.0 (contact: kanghyoseung@gmail.com)"

INCLUDE_FORMS = {"13F-HR", "13F-HR/A", "13F-NT", "13F-NT/A"}



def _sec_get_json(url: str) -> dict:
    headers = {"User-Agent": SEC_USER_AGENT, "Accept-Encoding": "gzip, deflate"}
    time.sleep(0.3)
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def _build_edgar_folder_url(cik: str, accession: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/"

def _latest_13f_record(manager: Dict[str, str]) -> Optional[Dict[str, str]]:
    cik = manager["cik"]
    j = _sec_get_json(f"https://data.sec.gov/submissions/CIK{cik}.json")

    recent = j.get("filings", {}).get("recent", {})
    forms = recent.get("form", []) or []
    dates = recent.get("filingDate", []) or []
    accessions = recent.get("accessionNumber", []) or []

    candidate: Optional[Dict[str, str]] = None
    for form, date, acc in zip(forms, dates, accessions):
        if not form:
            continue
        if form.upper() not in INCLUDE_FORMS:
            continue
        item = {"form": form.upper(), "date": date, "accession": acc}
        if candidate is None or (item["date"], item["accession"]) > (candidate["date"], candidate["accession"]):
            candidate = item

    if not candidate:
        return None

    candidate.update(
        name=manager["name"],
        cik=cik,
        edgar_url=_build_edgar_folder_url(cik, candidate["accession"]),
    )
    return candidate

def _notify_webhook(text: str) -> None:
    hook = HttpHook(method='POST', http_conn_id='slack_webhook_default')
    # endpoint를 Connection에 안 넣고 Host에 전체 URL을 넣는 패턴2라면 endpoint 인자를 주지 않습니다.
    resp = hook.run(json={"text": text})
    # HttpHook.run은 보통 requests.Response 유사 객체를 반환
    code = getattr(resp, "status_code", None)
    body = getattr(resp, "text", "")
    if code is None or code >= 300:
        raise AirflowException(f"Slack webhook failed: {code} {body}")


def get_13f_summary_from_url(edgar_url: str) -> str:
    """EDGAR URL에서 13F filing의 주요 텍스트를 추출하는 함수"""
    try:
        # 정보 테이블(information table) 파일 URL 찾기
        response = requests.get(edgar_url, headers={"User-Agent": SEC_USER_AGENT})
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 일반적으로 'form13fInfoTable.xml' 또는 유사한 이름의 파일을 찾음
        info_table_link = None
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if 'infotable.xml' in href.lower() or 'form13f.xml' in href.lower():
                info_table_link = f"https://www.sec.gov{href}"
                break
        
        # 2. 대체 방법: 기본 방법 실패 시 테이블에서 가장 큰 값의 링크를 검색
        if not info_table_link:
            tbody = soup.find('tbody')
            if tbody:
                max_value = -1  # 최댓값을 추적하기 위한 변수, 음수로 시작
                target_tr = None  # 최댓값을 가진 tr 태그를 저장할 변수

                # tbody 내의 모든 tr 태그를 가져와 두 번째 tr부터 순회
                # all_trs[1:]는 리스트의 두 번째 요소부터 끝까지를 의미
                all_trs = tbody.find_all('tr')
                for tr in all_trs[1:]:
                    tds = tr.find_all('td')
                    
                    # 행에 최소 두 개의 td가 있는지 확인
                    if len(tds) > 1:
                        # 두 번째 td의 텍스트를 가져오고, 쉼표(,)를 제거
                        value_text = tds[1].get_text(strip=True).replace(',', '')
                        
                        # 텍스트가 비어있지 않은 경우에만 처리
                        if value_text:
                            try:
                                # 텍스트를 실수(float)로 변환
                                current_value = float(value_text)
                                # 현재 값이 저장된 최댓값보다 크면 정보 업데이트
                                if current_value > max_value:
                                    max_value = current_value
                                    target_tr = tr
                            except ValueError:
                                # 텍스트가 숫자로 변환되지 않으면 해당 행을 건너뜀
                                continue

                # 최댓값을 가진 행(target_tr)을 찾았다면 링크 추출
                if target_tr:
                    # 첫 번째 td에서 'a' 태그를 찾음
                    link_tag = target_tr.find('td').find('a')
                    if link_tag and link_tag.get('href'):
                        href = link_tag.get('href')
                        info_table_link = f"https://www.sec.gov{href}"
                # 최종 결과 출력
            print(f"찾은 정보 테이블 링크: {info_table_link}")

        if not info_table_link:
            return "정보 테이블 파일을 찾을 수 없습니다."

        # 실제 파일 내용 가져오기 (이 부분은 실제 XML 구조에 따라 파싱 로직이 더 복잡해질 수 있습니다)
        filing_response = requests.get(info_table_link, headers={"User-Agent": SEC_USER_AGENT})
        filing_soup = BeautifulSoup(filing_response.text, "xml")
        
        # 간단히 텍스트만 모두 추출 (실제로는 특정 태그를 지정해야 함)
        # 내용이 너무 길 수 있으므로 일부만 잘라서 사용
        content_text = filing_soup.get_text(separator="\n", strip=True)
        return content_text[:4000] # Gemini API에 보낼 텍스트 길이 조절

    except Exception as e:
        print(f"Error fetching 13F content: {e}")
        return "13F 내용을 가져오는 데 실패했습니다."
    
def _get_gemini_api_key(conn_id: str = "google_ai_studio_default") -> str:
    """
    Airflow Connection에서 Gemini API Key를 읽는다.
    - Conn Type: Generic (추천)
    - 저장 위치: Password 칸 또는 Extra JSON의 {"api_key": "..."}
    """
    conn = BaseHook.get_connection(conn_id)
    api_key = (conn.password or conn.extra_dejson.get("api_key"))
    if not api_key:
        raise AirflowException(
            f"{conn_id}에 API 키가 없습니다. Password 칸이나 Extra.api_key에 넣어주세요."
        )
    return api_key

## NEW: JSON 파일을 읽어 프롬프트 딕셔너리를 로드하는 헬퍼 함수
def _load_prompts(file_path: str) -> Dict[str, str]:
    """지정된 경로의 JSON 파일을 읽어 딕셔너리로 반환합니다."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        raise AirflowException(f"프롬프트 파일({file_path})을 로드하는 데 실패했습니다: {e}")




with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 8, 1, 0, 0),
    schedule="0 * * * *",  # 매시간 정각
    catchup=False,
    tags=["sec", "13f", "edgar", "taskgroup"],
    default_args={"owner": "sec-monitor", "retries": 1, "email_on_failure": False},
) as dag:

    @task_group(group_id="monitor_13f")
    def monitor_13f_group(managers: List[Dict[str, str]]):

        @task(task_id="extract")
        def extract(manager: Dict[str, str]) -> Optional[Dict[str, str]]:
            try:
                return _latest_13f_record(manager)
            except Exception as e:
                raise AirflowSkipException(f"extract failed for {manager['cik']}: {e}")

        @task
        def compare(record):
            if not record:
                raise AirflowSkipException("extract가 13F 후보를 못 찾음")
            cik = record["cik"]
            key = f"sec_13f_last_accession_{cik}"
            last_seen = Variable.get(key, default_var="")
            current = record["accession"]
            if current != last_seen:
                Variable.set(key, current)
                return record
            raise AirflowSkipException(f"변화 없음: last_seen={last_seen} current={current}")


        @task(task_id="alert")
        def alert(change: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
            if not change:
                return None

            raw_content = get_13f_summary_from_url(change['edgar_url'])

            try:
                ## --- CHANGED: 프롬프트 동적 로딩 및 선택 ---
                # 1) JSON 파일에서 프롬프트 템플릿 로드
                prompt_file = os.path.join(DAG_FOLDER, "..", "prompts", "13f_analyze.json")
                prompts = _load_prompts(prompt_file)

                # 2) 공시 form 종류에 맞는 프롬프트 선택 (없으면 default 사용)
                form_type = change['form']
                prompt_template = prompts.get(form_type, prompts["default"])
                
                # 3) 프롬프트 템플릿에 실제 값 채우기
                final_prompt = prompt_template.format(
                    name=change['name'],
                    raw_content=raw_content,
                    form=form_type  # default 프롬프트를 위해 form 값도 전달
                )
                ## --- END CHANGED ---

                # 4) Gemini 요약 (Connection에서 API Key 로드)
                api_key = _get_gemini_api_key("google_ai_studio_default")
                llm = ChatGoogleGenerativeAI(
                    model="gemini-2.5-flash",  # 모델명은 최신으로 유지하는 것이 좋습니다.
                    google_api_key=api_key,
                )
                messages = [HumanMessage(content=final_prompt)] # 변경된 프롬프트 사용
                result = llm.invoke(messages)
                summary = (result.content or "").strip()
                if not summary:
                    raise AirflowException("Gemini가 빈 요약을 반환했습니다.")
            except Exception as e:
                raise AirflowException(f"Gemini 요약 실패: {e}")

            msg = (
                f"🔔 New 13F detected: {change['name']}\n"
                f"- CIK     : {change['cik']}\n"
                f"- Form    : {change['form']}\n"
                f"- Date    : {change['date']}\n"
                f"- EDGAR   : {change['edgar_url']}\n\n"
                f"🤖 **Gemini 요약:**\n{summary}"
            )
            _notify_webhook(msg)

            return change




        extracted = extract.expand(manager=managers)
        compared = compare.expand(record=extracted)
        alert.expand(change=compared)

    monitor_13f_group(MANAGERS)
