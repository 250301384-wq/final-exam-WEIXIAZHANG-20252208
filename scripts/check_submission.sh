#!/usr/bin/env bash
set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FAILURES=0
if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="${PYTHON_BIN:-python3}"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="${PYTHON_BIN:-python}"
else
  PYTHON_BIN=""
fi

pass() {
  echo "PASS - $1"
}

fail() {
  echo "FAIL - $1"
  FAILURES=$((FAILURES + 1))
}

check_file() {
  local path="$1"
  if [[ -e "$path" ]]; then
    pass "required file exists: $path"
  else
    fail "required file missing: $path"
  fi
}

echo "== Syntax =="
if [[ -z "$PYTHON_BIN" ]]; then
  fail "python3 or python is available"
elif "$PYTHON_BIN" -m compileall -q src api; then
  pass "python -m compileall src api"
else
  fail "python -m compileall src api"
fi

echo
echo "== Required files =="
for path in \
  README.md \
  MAX_GRADE_CHECKLIST.md \
  docker-compose.yml \
  requirements.txt \
  docs/architecture.md \
  docs/fault_tolerance.md \
  docs/analytics.md \
  docs/reflection.md \
  docs/api_examples.md \
  src/producer.py \
  src/consumer.py \
  src/spark_pipeline.py \
  src/analytics.py \
  api/app.py \
  api/kafka_utils.py \
  api/lake_utils.py \
  tests/test_curl_commands.sh \
  scripts/run_full_demo.sh; do
  check_file "$path"
done

echo
echo "== Requirements pinning =="
if awk 'NF && $0 !~ /^#/ && $0 !~ /==/ { bad=1 } END { exit bad }' requirements.txt; then
  pass "all requirements are pinned with =="
else
  fail "requirements.txt contains unpinned dependencies"
fi

echo
echo "== Path hygiene =="
if grep -RInE '(/home/|/Users/|C:\\Users|C:/Users|Desktop)' \
  --exclude-dir=.git --exclude-dir=.venv --exclude-dir=evidence --exclude='*.zip' --exclude='check_submission.sh' . >/tmp/submission_abs_paths.txt; then
  cat /tmp/submission_abs_paths.txt
  fail "absolute user-specific paths detected"
else
  pass "no absolute user-specific paths detected"
fi

echo
echo "== Generated analytics outputs =="
for path in \
  outputs/analytics/top_anomaly_hours \
  outputs/analytics/sensor_statistics \
  outputs/analytics/temperature_daily_evolution \
  outputs/analytics/partition_pruning_demo; do
  if compgen -G "${path}/part-*.csv" >/dev/null; then
    pass "analytics CSV exists: $path"
  else
    fail "analytics CSV missing: $path (run scripts/run_full_demo.sh or spark-submit src/analytics.py after generating Parquet data)"
  fi
done

for path in outputs/analytics/summary.md outputs/analytics/partition_pruning_explain.txt; do
  if [[ -s "$path" ]]; then
    pass "analytics output exists: $path"
  else
    fail "analytics output missing: $path (run scripts/run_full_demo.sh or spark-submit src/analytics.py after generating Parquet data)"
  fi
done

echo
echo "== Result =="
if [[ "$FAILURES" -eq 0 ]]; then
  echo "PASS - submission checklist completed"
  exit 0
fi

echo "FAIL - ${FAILURES} check(s) failed"
exit 1
