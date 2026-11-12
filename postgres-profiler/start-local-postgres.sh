#!/usr/bin/env bash
# Starts a PostgreSQL server used only for profiling, on this machine (so the profiler can also measure the
# database's CPU and disk use), from the PostgreSQL binaries already installed. No root needed.
#
#   ./start-local-postgres.sh                 # create (first time) and start: 127.0.0.1:5433, database wayangprof
#   ./start-local-postgres.sh stop | status
#   ./start-local-postgres.sh url             # start if needed, then print only the connection URL (for scripts)
#   ./start-local-postgres.sh --port 5434 --shared-buffers 1GB --pgdata /local/disk/pgdata
#
# Settings chosen for clean measurements:
#   - listens on 127.0.0.1 only, password-less login for local TCP connections (trust): nothing outside can connect
#   - autovacuum off: no background vacuum process starts in the middle of a run (the profiler VACUUM ANALYZEs
#     its tables itself after loading them)
#   - the OS user owns the server, so the profiler (same user) may read the server processes' /proc counters
#   - everything else PostgreSQL's defaults (shared_buffers 128MB, work_mem 4MB, jit on, parallel workers 2), so
#     the data describes a stock server; the profiler records the settings in every row
#   - C locale: works on minimal VMs without UTF-8 locales installed (and sorts bytes, the same everywhere)
# Installing PostgreSQL if it is missing: ../server/setup-vm.sh (Debian/Ubuntu/RHEL, with sudo), or by hand:
# "sudo apt install postgresql", "sudo dnf install postgresql-server", "brew install postgresql@16".
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACTION=start
PORT=5433
PGDATA_DIR="$HERE/pgdata"
SHARED_BUFFERS=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    start|stop|status|url) ACTION="$1"; shift ;;
    --port) PORT="$2"; shift 2 ;;
    --pgdata) PGDATA_DIR="$2"; shift 2 ;;
    --shared-buffers) SHARED_BUFFERS="$2"; shift 2 ;;
    -h|--help) sed -n '2,/^[^#]/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
done

find_bin() {
  # PGBIN, then the portable PostgreSQL that ../server/prepare.sh downloads (no root needed), then installed ones.
  if [[ -n "${PGBIN:-}" && -x "$PGBIN/pg_ctl" ]]; then echo "$PGBIN"; return; fi
  if [[ -x "$HERE/../.deps/postgres/bin/pg_ctl" ]]; then (cd "$HERE/../.deps/postgres/bin" && pwd); return; fi
  if command -v pg_ctl >/dev/null; then dirname "$(command -v pg_ctl)"; return; fi
  local d
  for d in $(ls -d /usr/lib/postgresql/*/bin /usr/pgsql-*/bin /opt/homebrew/opt/postgresql@*/bin \
                   /usr/local/opt/postgresql@*/bin /usr/local/pgsql/bin 2>/dev/null | sort -V -r); do
    [[ -x "$d/pg_ctl" ]] && { echo "$d"; return; }
  done
}
BIN=$(find_bin)
[[ -n "$BIN" ]] || { echo "PostgreSQL binaries (pg_ctl, initdb) not found. Install PostgreSQL (see the top of this script)."; exit 1; }
PGUSER_NAME=$(id -un)

case "$ACTION" in
  stop) "$BIN/pg_ctl" -D "$PGDATA_DIR" stop -m fast; exit 0 ;;
  status) "$BIN/pg_ctl" -D "$PGDATA_DIR" status; exit $? ;;
esac

if [[ ! -s "$PGDATA_DIR/PG_VERSION" ]]; then
  echo "creating a new database cluster in $PGDATA_DIR ($("$BIN/postgres" --version))" >&2
  "$BIN/initdb" -D "$PGDATA_DIR" -U "$PGUSER_NAME" --auth=trust -E UTF8 --locale=C >/dev/null
  {
    echo ""
    echo "# --- wayangprof: settings for profiling (see start-local-postgres.sh) ---"
    echo "listen_addresses = '127.0.0.1'"
    echo "port = $PORT"
    echo "unix_socket_directories = ''"
    echo "autovacuum = off"
    [[ -n "$SHARED_BUFFERS" ]] && echo "shared_buffers = '$SHARED_BUFFERS'"
  } >>"$PGDATA_DIR/postgresql.conf"
  # The database, created in single-user mode before the first start: works without psql (the portable build has none).
  echo "CREATE DATABASE wayangprof;" | "$BIN/postgres" --single -D "$PGDATA_DIR" postgres >/dev/null 2>&1 \
    || echo "note: could not create the database in single-user mode; trying psql after the start" >&2
fi
if "$BIN/pg_ctl" -D "$PGDATA_DIR" status >/dev/null 2>&1; then
  [[ "$ACTION" == url ]] || echo "already running"
else
  "$BIN/pg_ctl" -D "$PGDATA_DIR" -l "$PGDATA_DIR/server.log" -w start >/dev/null \
    || { tail -20 "$PGDATA_DIR/server.log"; exit 1; }
fi
PORT=$(awk -F= '/^port =/ {gsub(/ /, "", $2); p=$2} END {print p}' "$PGDATA_DIR/postgresql.conf")
if [[ -x "$BIN/psql" ]] && ! "$BIN/psql" -h 127.0.0.1 -p "$PORT" -U "$PGUSER_NAME" -d postgres -Atc "SELECT 1 FROM pg_database WHERE datname = 'wayangprof'" | grep -q 1; then
  "$BIN/psql" -h 127.0.0.1 -p "$PORT" -U "$PGUSER_NAME" -d postgres -qc "CREATE DATABASE wayangprof"
fi
if [[ "$ACTION" == url ]]; then echo "postgresql://127.0.0.1:$PORT/wayangprof"; exit 0; fi
echo "$("$BIN/postgres" --version) running:"
echo "  --url postgresql://127.0.0.1:$PORT/wayangprof --user $PGUSER_NAME     (data: $PGDATA_DIR, log: $PGDATA_DIR/server.log)"
