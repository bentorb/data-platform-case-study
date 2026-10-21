#!/usr/bin/env bash
# Checks that each warehouse user can do what it should, and nothing more.
#
# Usage (with the stack running):  ./warehouse-db/tests/test_permissions.sh
#
# Connects to the container by its service name rather than localhost, so
# password authentication applies (loopback connections are trusted by the image).
# Test tables are prefixed with _test_ and dropped on exit.
set -uo pipefail

cd "$(dirname "$0")/../.." || exit 1

# Use the same values as docker-compose.yml: .env if present, otherwise the defaults.
# shellcheck source=/dev/null  # .env is optional and local
if [[ -f .env ]]; then set -a; source .env; set +a; fi
DB=${WAREHOUSE_DB_NAME:-warehouse}
ADMIN=${WAREHOUSE_DB_USER:-admin};      ADMIN_PW=${WAREHOUSE_DB_PASSWORD:-postgres}
I=ingestion_service;                    IP=${WAREHOUSE_INGESTION_PASSWORD:-ingestion}
T=transformation_service;               TP=${WAREHOUSE_TRANSFORMATION_PASSWORD:-transformation}
A=analytics_service;                    AP=${WAREHOUSE_ANALYTICS_PASSWORD:-analytics}
R=analytics_restricted_service;         RP=${WAREHOUSE_ANALYTICS_RESTRICTED_PASSWORD:-analytics_restricted}
M=maintenance_service;                  MP=${WAREHOUSE_MAINTENANCE_PASSWORD:-maintenance}

ERR=$(mktemp)
pass=0; fail=0

run_sql() {  # run_sql <user> <password> <sql>
  docker compose exec -T -e PGPASSWORD="$2" warehouse-db \
    psql -h warehouse-db -U "$1" -d "$DB" -v ON_ERROR_STOP=1 -qAt -c "$3" >/dev/null 2>"$ERR"
}

check() {  # check <ok|deny> <user> <password> <description> <sql>
  local expect=$1 user=$2 pw=$3 desc=$4 sql=$5 result
  if run_sql "$user" "$pw" "$sql"; then result=ok; else result=deny; fi
  if [[ $result == "$expect" ]]; then
    printf '  ✅ %-24s %-5s %s\n' "$user" "$expect" "$desc"; ((pass++))
  else
    printf '  ❌ %-24s %-5s %s (got %s)\n' "$user" "$expect" "$desc" "$result"
    [[ -s $ERR ]] && printf '     %s\n' "$(head -1 "$ERR")"
    ((fail++))
  fi
}

cleanup() {
  run_sql "$ADMIN" "$ADMIN_PW" "
    drop table if exists raw._test_t, raw._test_y, staging._test_s, staging._test_x,
      core._test_c, core._test_x, core._test_y, live._test_l, live._test_x, live._test_y,
      audit._test_load_log, audit._test_dq, audit._test_m,
      public._test_x;
    drop schema if exists _test_schema;" || echo "warning: cleanup failed"
  rm -f "$ERR"
}

if ! docker compose ps --status running --services 2>/dev/null | grep -qx warehouse-db; then
  echo "warehouse-db is not running. Start it with: docker compose up -d --wait"
  rm -f "$ERR"
  exit 1
fi
trap cleanup EXIT

echo "== authentication"
check ok   "$ADMIN" "$ADMIN_PW" "admin can connect"          "select 1"
check deny "$I"     wrong       "wrong password is rejected" "select 1"

echo "== ingestion_service"
check ok   "$I" "$IP" "create + write in raw"   "create table raw._test_t (id int); insert into raw._test_t values (1)"
check ok   "$I" "$IP" "create + write in audit" "create table audit._test_load_log (id int); insert into audit._test_load_log values (1)"
check deny "$I" "$IP" "create in staging"       "create table staging._test_x (id int)"
check deny "$I" "$IP" "create in core"          "create table core._test_x (id int)"
check deny "$I" "$IP" "create in live"          "create table live._test_x (id int)"
check deny "$I" "$IP" "create in public"        "create table public._test_x (id int)"
check deny "$I" "$IP" "create a schema"         "create schema _test_schema"

echo "== transformation_service"
check ok   "$T" "$TP" "read raw (default privileges)"   "select * from raw._test_t"
check deny "$T" "$TP" "write to raw"                    "insert into raw._test_t values (2)"
check deny "$T" "$TP" "create in raw"                   "create table raw._test_y (id int)"
check ok   "$T" "$TP" "create in staging"               "create table staging._test_s (id int)"
check ok   "$T" "$TP" "create + write in core"          "create table core._test_c (id int); insert into core._test_c values (1)"
check ok   "$T" "$TP" "create + write in live"          "create table live._test_l (id int); insert into live._test_l values (1)"
check ok   "$T" "$TP" "read ingestion's audit tables"   "select * from audit._test_load_log"
check ok   "$T" "$TP" "create in audit"                 "create table audit._test_dq (id int)"
check deny "$T" "$TP" "create a schema"                 "create schema _test_schema"

echo "== ingestion_service (audit cross-read)"
check ok   "$I" "$IP" "read transformation's audit tables" "select * from audit._test_dq"

echo "== analytics_service"
check deny "$A" "$AP" "read a live table before its publish grants it" "select * from live._test_l"
check deny "$A" "$AP" "write to live"                  "insert into live._test_l values (2)"
check deny "$A" "$AP" "create in live"                 "create table live._test_y (id int)"
check deny "$A" "$AP" "read core"                      "select * from core._test_c"
check deny "$A" "$AP" "read raw"                       "select * from raw._test_t"
check deny "$A" "$AP" "read staging"                   "select * from staging._test_s"
check deny "$A" "$AP" "read audit"                     "select * from audit._test_load_log"

echo "== analytics_restricted_service"
check ok   "$R" "$RP" "read live (default privileges)"  "select * from live._test_l"
check deny "$R" "$RP" "write to live"                   "insert into live._test_l values (2)"
check ok   "$R" "$RP" "read the data-quality history (inherited)" "select * from audit.dq_load_summary"
check deny "$R" "$RP" "read core"                       "select * from core._test_c"

echo "== maintenance_service"
check ok   "$M" "$MP" "expire audit entries"            "delete from audit.dbt_run_results where false"
check ok   "$M" "$MP" "record an expiry"                "insert into audit.retention_log (target_table, cutoff, rows_deleted) select 'test', now(), 0 where false"
check deny "$M" "$MP" "add load log entries"            "insert into audit.load_log (target_table, source_file, status, started_at) values ('t', 'f', 'running', now())"
check deny "$M" "$MP" "create in audit"                 "create table audit._test_m (id int)"
check deny "$M" "$MP" "read raw"                        "select * from raw._test_t"
check deny "$M" "$MP" "read core"                       "select * from core._test_c"
check deny "$M" "$MP" "read live"                       "select * from live._test_l"

echo
echo "passed: $pass  failed: $fail"
[[ $fail -eq 0 ]]
