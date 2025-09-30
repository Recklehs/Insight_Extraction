# docker-compose.yaml 파일에 정의된 기본 Airflow 이미지를 그대로 사용
FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.0.3}

# 1) root로 파일만 복사
USER root
COPY requirements.txt /tmp/requirements.txt

# 2) airflow 유저로 파이썬 패키지 설치 (playwright 포함되어 있어야 함)
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 3) 브라우저 실행에 필요한 리눅스 의존성은 root로 설치
USER root
RUN apt-get update \
    && playwright install-deps chromium \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 4) 실제 브라우저 바이너리는 airflow 유저 홈 캐시에 설치
USER airflow
RUN playwright install chromium
