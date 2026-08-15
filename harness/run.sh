#!/usr/bin/env bash
# Прогон одного теста через локальный интерактор.
#   ./harness/run.sh tests/example1.txt        — прогнать и напечатать сводку
#   ./harness/run.sh tests/example1.txt --bless — записать сводку в tests/example1.expected
# Если .expected существует, сводка сверяется с ним; расхождение => ненулевой код возврата.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${SCHED_BIN:-$ROOT/build/v7}"
TEST="${1:?usage: run.sh <test.txt> [--bless] [--trace]}"
shift || true

BLESS=0
EXTRA=()
for a in "$@"; do
  case "$a" in
    --bless) BLESS=1 ;;
    *) EXTRA+=("$a") ;;
  esac
done

if [[ ! -x "$BIN" ]]; then
  echo "нет бинаря $BIN — сначала: g++ -O2 -std=c++23 -o build/main src/main.cpp" >&2
  exit 2
fi

NAME="$(basename "${TEST%.txt}")"
EXPECTED="$ROOT/tests/$NAME.expected"
ACTUAL="$(mktemp)"
trap 'rm -f "$ACTUAL"' EXIT

if ! python3 "$ROOT/harness/interactor.py" "$TEST" ${EXTRA[@]+"${EXTRA[@]}"} -- "$BIN" >"$ACTUAL"; then
  echo "FAIL $NAME (интерактор сообщил о нарушении)" >&2
  cat "$ACTUAL" >&2
  exit 1
fi

cat "$ACTUAL"

if [[ $BLESS -eq 1 ]]; then
  cp "$ACTUAL" "$EXPECTED"
  echo "blessed -> tests/$NAME.expected"
  exit 0
fi

if [[ -f "$EXPECTED" ]]; then
  if ! diff -u "$EXPECTED" "$ACTUAL"; then
    echo "FAIL $NAME (сводка разошлась с tests/$NAME.expected)" >&2
    exit 1
  fi
fi

echo "OK $NAME"
