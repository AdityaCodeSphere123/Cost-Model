# Sourced by prepare.sh, test.sh and collect.sh: finds Java 17, Wayang, the Spark jars and PostgreSQL, and exports
# JAVA_HOME, WAYANG_HOME, SPARK_JARS, PGBIN, PYTHON for every runner. Nothing here downloads anything.
#
# Search order (first found wins):
#   Java 17     bundle jdk/linux-<arch>  >  Cost-Model/.deps/jdk (prepare.sh)  >  $JAVA_HOME  >  /usr/lib/jvm/java-17*  >  java on PATH
#   Wayang      $WAYANG_HOME  >  bundle wayang/  >  ../wayang/wayang-assembly/target/*-dist/wayang-*  >  .deps/wayang-home
#   Spark jars  $SPARK_JARS  >  bundle spark-jars/  >  .deps/spark/jars (prepare.sh)  >  $SPARK_HOME/jars  >  /opt/spark/jars
#   PostgreSQL  $PGBIN  >  .deps/postgres/bin (prepare.sh, portable)  >  pg_ctl on PATH  >  /usr/lib/postgresql/*/bin ...
CM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUNDLE_DIR="$(cd "$CM_DIR/.." && pwd)"
DEPS="$CM_DIR/.deps"
export PYTHON="${PYTHON:-python3}"
case "$(uname -m)" in x86_64|amd64) ENV_ARCH=x64 ;; aarch64|arm64) ENV_ARCH=aarch64 ;; *) ENV_ARCH=other ;; esac

java_major() { "$1" -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/'; }
_jh=""
for d in "$BUNDLE_DIR/jdk/linux-$ENV_ARCH" "$DEPS/jdk" "${JAVA_HOME:-}" /usr/lib/jvm/java-17-openjdk* /usr/lib/jvm/java-17 /usr/lib/jvm/temurin-17* \
         /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home; do
  [[ -n "$d" && -x "$d/bin/java" && -x "$d/bin/javac" ]] || continue
  [[ "$(uname -s)" == Linux || "$d" != "$BUNDLE_DIR/jdk/"* ]] || continue
  (( $(java_major "$d/bin/java") >= 17 )) 2>/dev/null || continue
  _jh="$d"; break
done
if [[ -z "$_jh" ]] && command -v javac >/dev/null; then
  _jh="$(cd "$(dirname "$(readlink -f "$(command -v javac)" 2>/dev/null || command -v javac)")/.." && pwd)"
fi
[[ -n "$_jh" ]] && export JAVA_HOME="$_jh"
unset _jh

_wh=""
for d in "${WAYANG_HOME:-}" "$BUNDLE_DIR/wayang" $(ls -d "$CM_DIR"/../wayang/wayang-assembly/target/*-dist/wayang-* 2>/dev/null) "$DEPS/wayang-home"; do
  [[ -n "$d" && -d "$d/jars" ]] && ls "$d"/jars/wayang-core-*.jar >/dev/null 2>&1 && { _wh="$(cd "$d" && pwd)"; break; }
done
[[ -n "$_wh" ]] && export WAYANG_HOME="$_wh"
unset _wh

if [[ -z "${SPARK_JARS:-}" ]]; then
  for d in "$BUNDLE_DIR/spark-jars" "$DEPS/spark/jars" "${SPARK_HOME:+$SPARK_HOME/jars}" /opt/spark/jars; do
    # Wayang is built against Spark 3.5 / Scala 2.12: other versions are skipped.
    [[ -n "$d" ]] && ls "$d"/spark-core_2.12-3.5.*.jar >/dev/null 2>&1 && { export SPARK_JARS="$d/*"; break; }
  done
fi

if [[ -z "${PGBIN:-}" || ! -x "${PGBIN:-}/pg_ctl" ]]; then
  PGBIN=""
  for d in "$DEPS/postgres/bin" "$(dirname "$(command -v pg_ctl 2>/dev/null || echo /nonexistent/x)")" \
           $(ls -d /usr/lib/postgresql/*/bin /usr/pgsql-*/bin /opt/homebrew/opt/postgresql@*/bin 2>/dev/null | sort -V -r); do
    [[ -x "$d/pg_ctl" && -x "$d/initdb" ]] && { PGBIN="$d"; break; }
  done
fi
export PGBIN

env_report() {  # one line per component, "MISSING" where something is not found
  local j="${JAVA_HOME:+$JAVA_HOME/bin/}java"
  echo "  java       : $( [[ -n "${JAVA_HOME:-}" ]] && echo "$JAVA_HOME ($("$j" -version 2>&1 | head -1))" || echo MISSING)"
  echo "  wayang     : ${WAYANG_HOME:-MISSING}$( [[ -n "${WAYANG_HOME:-}" ]] && echo " ($(ls "$WAYANG_HOME"/jars/wayang-core-*.jar | xargs -n1 basename))")"
  if [[ -n "${SPARK_JARS:-}" ]]; then
    echo "  spark jars : $SPARK_JARS ($(ls ${SPARK_JARS%/\*}/spark-core_2.12-*.jar | xargs -n1 basename))"
  elif [[ -s "$CM_DIR/spark-profiler/build/spark-classpath.txt" ]]; then
    echo "  spark jars : from Maven (spark-profiler/build/spark-classpath.txt)"
  else
    echo "  spark jars : MISSING"
  fi
  echo "  postgres   : ${PGBIN:-MISSING}$( [[ -n "${PGBIN:-}" ]] && echo " ($("$PGBIN/postgres" --version 2>&1 | head -1))")"
  echo "  python     : $(command -v "$PYTHON" || echo MISSING) ($("$PYTHON" --version 2>&1))"
  echo "  taskset    : $(command -v taskset || echo "not found (lanes are not pinned to their CPUs)")"
}
env_ok() {
  [[ -n "${JAVA_HOME:-}" && -n "${WAYANG_HOME:-}" && -n "${PGBIN:-}" ]] && command -v "$PYTHON" >/dev/null \
    && { [[ -n "${SPARK_JARS:-}" ]] || [[ -s "$CM_DIR/spark-profiler/build/spark-classpath.txt" ]]; }
}
