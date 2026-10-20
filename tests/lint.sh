#!/usr/bin/env bash
# Static checks: Python (ruff), SQL (sqlfluff), YAML (yamllint) and shell (shellcheck).
# Run with `docker compose run --rm lint`; runs every check, then exits non-zero if any failed.
set -uo pipefail

failed=()
check() {  # check <name> <command...>
  echo "== $1"
  if "${@:2}"; then echo "ok"; else failed+=("$1"); fi
}

check "ruff check"   ruff check --no-cache .
check "ruff format"  ruff format --no-cache --check .
check "sqlfluff"     bash -c "cd dbt && sqlfluff lint models tests"
check "yamllint"     yamllint .
check "shellcheck"   bash -c "shellcheck warehouse-db/init/*.sh warehouse-db/tests/*.sh tests/*.sh"

if (( ${#failed[@]} )); then
  echo "failed: ${failed[*]}"
  exit 1
fi
echo "all checks passed"
