#!/usr/bin/env bash

set -euo pipefail

readonly AIRFLOW_API_URL="http://airflow-apiserver:8080/api/v2/version"
readonly TARGET_DAGS=(
  "rss_blog_post_collector"
  "naver_blog_playwright_scraper_from_db_v5"
  "investment_signal_extraction_dag"
)

run_airflow() {
  /entrypoint airflow "$@"
}

echo "[blog-bootstrap] waiting for airflow api..."
for attempt in $(seq 1 60); do
  if curl --silent --fail "${AIRFLOW_API_URL}" >/dev/null; then
    echo "[blog-bootstrap] airflow api is ready"
    break
  fi
  sleep 2
done

if ! curl --silent --fail "${AIRFLOW_API_URL}" >/dev/null; then
  echo "[blog-bootstrap] airflow api did not become ready in time" >&2
  exit 1
fi

for dag_id in "${TARGET_DAGS[@]}"; do
  echo "[blog-bootstrap] unpausing ${dag_id}"
  run_airflow dags unpause -y "${dag_id}"
done

run_id="blog-bootstrap-$(date -u +%Y%m%dT%H%M%S)-$$"
echo "[blog-bootstrap] triggering rss_blog_post_collector with run_id=${run_id}"
run_airflow dags trigger -r "${run_id}" rss_blog_post_collector
