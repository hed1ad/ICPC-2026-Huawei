#!/usr/bin/env bash
# Прогон всех тестов из tests/. Ненулевой код возврата, если хоть один упал.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FAILED=0

for t in "$ROOT"/tests/*.txt; do
  name="$(basename "${t%.txt}")"
  echo "=== $name"
  if ! "$ROOT/harness/run.sh" "$t" "$@"; then
    FAILED=$((FAILED + 1))
  fi
  echo
done

if [[ $FAILED -gt 0 ]]; then
  echo "ПРОВАЛЕНО тестов: $FAILED" >&2
  exit 1
fi
echo "все тесты пройдены"
