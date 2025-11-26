#!/usr/bin/env bash
set -u

# Use PYTHON_CMD to change the Python invocation (e.g., "poetry run python" or "python3")
PYTHON_CMD="${PYTHON_CMD:-python3}"

scripts=(
  "./src/0_get_aidev_csv.py"
  "./src/1_get_all_projects.py"
  "./src/2_get_last_merged_commit_per_project.py"
  "./src/3_compute_clone_density.py"
  "./src/4_get_genealogy.py"
)

now_ns() {
  # GNU date supports %N. On macOS it may not; fall back to Python's time_ns().
  local n
  n="$(date +%s%N 2>/dev/null || true)"
  if [[ "$n" =~ ^[0-9]+$ ]] && [[ ${#n} -ge 13 ]]; then
    echo "$n"
  else
    "$PYTHON_CMD" - <<'PY'
import time
print(time.time_ns())
PY
  fi
}

fmt_ns() {
  # Print seconds with millisecond precision
  "$PYTHON_CMD" - <<PY
ns = int("$1")
print(f"{ns/1e9:.3f}s")
PY
}

declare -a durations_ns=()
total_ns=0

echo "==> Running pipeline with: $PYTHON_CMD"
echo

pipeline_start="$(now_ns)"

for i in "${!scripts[@]}"; do
  s="${scripts[$i]}"

  if [[ ! -f "$s" ]]; then
    echo "❌ File not found: $s"
    exit 1
  fi

  echo "----------------------------------------"
  echo "[$((i+1))/${#scripts[@]}] Running: $s"
  start="$(now_ns)"

  # Execute script
  $PYTHON_CMD "$s"
  status=$?

  end="$(now_ns)"
  dur=$(( end - start ))
  durations_ns+=("$dur")
  total_ns=$(( total_ns + dur ))

  echo "✔ Finished: $s | time: $(fmt_ns "$dur") | exit code: $status"

  if [[ $status -ne 0 ]]; then
    echo
    echo "❌ Stopping because '$s' failed."
    break
  fi
done

pipeline_end="$(now_ns)"
pipeline_ns=$(( pipeline_end - pipeline_start ))

echo
echo "========================================"
echo "TIMING SUMMARY"
for i in "${!scripts[@]}"; do
  if [[ $i -lt ${#durations_ns[@]} ]]; then
    echo "- ${scripts[$i]}: $(fmt_ns "${durations_ns[$i]}")"
  else
    echo "- ${scripts[$i]}: (not executed)"
  fi
done
echo "----------------------------------------"
echo "Total time (sum of executed scripts): $(fmt_ns "$total_ns")"
echo "Total time (wall-clock):             $(fmt_ns "$pipeline_ns")"
echo "========================================"
