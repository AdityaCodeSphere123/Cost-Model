#!/usr/bin/env bash
# Step 1 on the server, the ONLY step that needs the internet. No root needed. Afterwards everything runs offline.
#
#   ./prepare.sh                 # ~5-20 min (depends on what is missing); log: ../prepare.log
#   ./prepare.sh --no-model      # skip the Python packages for training (torch, numpy)
#
# What it does (each part only if it is missing; everything goes into Cost-Model/.deps/, nothing system-wide):
#   1. Java 17 JDK          (bundle's own, an installed one, or Eclipse Temurin 17 downloaded; SHA-256 checked)
#   2. Wayang               (bundle's own jars, $WAYANG_HOME, or the Wayang source next to Cost-Model built with
#                            its Maven wrapper: ../wayang -> wayang-assembly/target/...-dist)
#   3. Spark 3.5.7 jars     (bundle's own, or spark-3.5.7-bin-hadoop3 from apache.org; SHA-512 checked)
#   4. PostgreSQL           (an installed one, or portable binaries from Maven Central (io.zonky embedded-postgres);
#                            SHA-1 checked; runs as your user: no root, no system service)
#   5. Python packages      for training the model (venv .deps/venv: numpy + CPU torch), unless --no-model
#   6. compiles the three profilers, and checks the portable PostgreSQL starts (on a scratch port)
# Then: ./test.sh (offline; ~15 min), then ./collect.sh start (offline; days).
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CM="$(cd "$HERE/.." && pwd)"
MODEL=1
SPARK_VERSION=3.5.7
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-model) MODEL=0; shift ;;
    -h|--help) sed -n '2,/^[^#]/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
done
LOG="$CM/prepare.log"
exec > >(tee -a "$LOG") 2>&1
say() { echo "[$(date '+%F %T')] $*"; }
fail() { say "ERROR: $*"; say "prepare FAILED (full log: $LOG)"; exit 1; }
DEPS="$CM/.deps"; mkdir -p "$DEPS"
command -v curl >/dev/null || fail "curl is needed for the downloads"
command -v python3 >/dev/null || fail "python3 is needed"
case "$(uname -m)" in x86_64|amd64) ARCH=x64; ZARCH=amd64 ;; aarch64|arm64) ARCH=aarch64; ZARCH=arm64v8 ;; *) fail "unsupported CPU $(uname -m)" ;; esac
[[ "$(uname -s)" == Linux ]] || say "NOTE: not Linux: downloads are skipped where they would be Linux binaries"
say "prepare on $(hostname): $(uname -srm), $(nproc 2>/dev/null || sysctl -n hw.ncpu) CPUs, $(awk '/MemTotal/ {printf "%.0f GB RAM", $2/1048576}' /proc/meminfo 2>/dev/null)"
say "free disk in $CM: $(df -Ph "$CM" | awk 'NR==2 {print $4}')"
curl -sfI --max-time 15 https://repo1.maven.org/maven2/ >/dev/null || say "WARNING: repo1.maven.org not reachable: downloads may fail (proxy? set https_proxy)"

# shellcheck source=env.sh
source "$HERE/env.sh"

# ---- 1. Java 17 -----------------------------------------------------------------------------------
if [[ -z "${JAVA_HOME:-}" && "$(uname -s)" == Linux ]]; then
  say "Java 17: none found, downloading Eclipse Temurin 17 ($ARCH)"
  meta=$(curl -sf --max-time 60 "https://api.adoptium.net/v3/assets/latest/17/hotspot?architecture=$ARCH&image_type=jdk&os=linux&vendor=eclipse") \
    || fail "could not reach api.adoptium.net"
  read -r link sum name < <(python3 -c 'import json,sys; b=json.load(sys.stdin)[0]["binary"]["package"]; print(b["link"], b["checksum"], b["name"])' <<<"$meta")
  curl -sfL --retry 3 --max-time 1800 -o "$DEPS/$name" "$link" || fail "JDK download failed"
  [[ "$(sha256sum "$DEPS/$name" | cut -d' ' -f1)" == "$sum" ]] || { rm -f "$DEPS/$name"; fail "JDK checksum mismatch (deleted; run again)"; }
  rm -rf "$DEPS/jdk"; mkdir -p "$DEPS/jdk"
  tar -xzf "$DEPS/$name" -C "$DEPS/jdk" --strip-components=1 && rm -f "$DEPS/$name"
  source "$HERE/env.sh"
fi
[[ -n "${JAVA_HOME:-}" ]] || fail "no Java 17 JDK"
say "Java: $JAVA_HOME ($("$JAVA_HOME/bin/java" -version 2>&1 | head -1))"

# ---- 2. Wayang --------------------------------------------------------------------------------------
if [[ -z "${WAYANG_HOME:-}" ]]; then
  SRC=""
  for d in "${WAYANG_SRC:-}" "$CM/../wayang" "$HOME/wayang" "$HOME/incubator-wayang"; do
    [[ -n "$d" && -x "$d/mvnw" && -d "$d/wayang-assembly" ]] && { SRC="$(cd "$d" && pwd)"; break; }
  done
  [[ -n "$SRC" ]] || fail "Wayang not found: copy the bundle (it has Wayang's jars), or set WAYANG_HOME=<built dist with jars/ libs/>,
       or WAYANG_SRC=<Wayang source folder> to build it here"
  say "Wayang: building the distribution from $SRC (Maven downloads its dependencies now; 10-30 min)"
  ( cd "$SRC" && JAVA_HOME="$JAVA_HOME" ./mvnw -q clean package -pl :wayang-assembly -am -Pdistribution -DskipTests ) \
    || fail "Wayang build failed (see above)"
  source "$HERE/env.sh"
  [[ -n "${WAYANG_HOME:-}" ]] || fail "Wayang was built but its distribution was not found under $SRC/wayang-assembly/target"
fi
say "Wayang: $WAYANG_HOME ($(ls "$WAYANG_HOME"/jars/wayang-core-*.jar | xargs -n1 basename))"

# ---- 3. Spark jars ---------------------------------------------------------------------------------
if [[ -z "${SPARK_JARS:-}" && ! -s "$CM/spark-profiler/build/spark-classpath.txt" ]]; then
  name="spark-$SPARK_VERSION-bin-hadoop3"
  say "Spark: downloading $name.tgz (~400 MB)"
  got=""
  for base in "https://dlcdn.apache.org/spark" "https://archive.apache.org/dist/spark"; do
    if curl -fsSL --retry 3 --max-time 3600 -o "$DEPS/$name.tgz" "$base/spark-$SPARK_VERSION/$name.tgz" \
       && curl -fsSL --retry 3 --max-time 60 -o "$DEPS/$name.tgz.sha512" "$base/spark-$SPARK_VERSION/$name.tgz.sha512"; then
      got=1; break
    fi
  done
  [[ -n "$got" ]] || fail "could not download Spark $SPARK_VERSION"
  want=$(sed 's/^[^:]*: //' "$DEPS/$name.tgz.sha512" | tr -d ' \n' | grep -oiE '[0-9a-f]{128}' | head -1 | tr 'A-F' 'a-f')
  [[ -n "$want" && "$want" == "$(sha512sum "$DEPS/$name.tgz" | cut -d' ' -f1)" ]] || { rm -f "$DEPS/$name.tgz"*; fail "Spark checksum mismatch (deleted; run again)"; }
  rm -rf "$DEPS/spark"; mkdir -p "$DEPS/spark"
  tar -xzf "$DEPS/$name.tgz" -C "$DEPS/spark" --strip-components=1 && rm -f "$DEPS/$name.tgz"*
  source "$HERE/env.sh"
  [[ -n "${SPARK_JARS:-}" ]] || fail "Spark extracted but spark-core_2.12-$SPARK_VERSION.jar not found in $DEPS/spark/jars"
fi
say "Spark jars: ${SPARK_JARS:-from Maven (spark-profiler/build/spark-classpath.txt)}"

# ---- 4. PostgreSQL ---------------------------------------------------------------------------------
if [[ -z "${PGBIN:-}" && "$(uname -s)" == Linux ]]; then
  base="https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-linux-$ZARCH"
  ver=$(curl -sf --max-time 60 "$base/maven-metadata.xml" | grep -oE '<version>1[67]\.[0-9]+\.[0-9]+</version>' \
        | sed -E 's#</?version>##g' | sort -V | tail -1)
  [[ -n "$ver" ]] || fail "could not list the portable PostgreSQL versions on Maven Central"
  jar="embedded-postgres-binaries-linux-$ZARCH-$ver.jar"
  say "PostgreSQL: none installed, downloading portable PostgreSQL $ver ($jar)"
  curl -sfL --retry 3 --max-time 900 -o "$DEPS/$jar" "$base/$ver/$jar" || fail "PostgreSQL download failed"
  sha1=$(curl -sf --max-time 60 "$base/$ver/$jar.sha1" | grep -oE '^[0-9a-f]{40}')
  [[ -n "$sha1" && "$sha1" == "$(sha1sum "$DEPS/$jar" | cut -d' ' -f1)" ]] || { rm -f "$DEPS/$jar"; fail "PostgreSQL checksum mismatch (deleted; run again)"; }
  rm -rf "$DEPS/postgres"; mkdir -p "$DEPS/postgres"
  python3 - "$DEPS/$jar" "$DEPS/postgres" <<'PY' || fail "could not unpack PostgreSQL"
import io, sys, tarfile, zipfile
jar, dest = sys.argv[1:3]
z = zipfile.ZipFile(jar)
txz = [n for n in z.namelist() if n.endswith(".txz")]
assert txz, "no .txz inside the jar"
with tarfile.open(fileobj=io.BytesIO(z.read(txz[0])), mode="r:xz") as t:
    t.extractall(dest)
PY
  rm -f "$DEPS/$jar"
  chmod +x "$DEPS/postgres/bin/"* 2>/dev/null
  source "$HERE/env.sh"
  [[ -n "${PGBIN:-}" ]] || fail "PostgreSQL unpacked but $DEPS/postgres/bin/pg_ctl not found"
fi
[[ -n "${PGBIN:-}" ]] || fail "no PostgreSQL (install it, or run this on Linux to get the portable one)"
"$PGBIN/postgres" --version >/dev/null 2>&1 || fail "$PGBIN/postgres does not run here: $("$PGBIN/postgres" --version 2>&1 | head -3)"
say "PostgreSQL: $PGBIN ($("$PGBIN/postgres" --version))"
# A real start on a scratch port: initdb + start + database + stop, so test.sh / collect.sh will not fail on it.
tmp="$DEPS/pg-smoke"; rm -rf "$tmp"
if "$CM/postgres-profiler/start-local-postgres.sh" --port 5599 --pgdata "$tmp" >/dev/null 2>&1; then
  "$CM/postgres-profiler/start-local-postgres.sh" stop --pgdata "$tmp" >/dev/null 2>&1
  say "PostgreSQL: start/stop test ok"
else
  tail -20 "$tmp/server.log" 2>/dev/null
  "$CM/postgres-profiler/start-local-postgres.sh" stop --pgdata "$tmp" >/dev/null 2>&1
  fail "the PostgreSQL server did not start (log above)"
fi
rm -rf "$tmp"

# ---- 5. Python packages for the model -----------------------------------------------------------------
if (( MODEL )); then
  if [[ -x "$DEPS/venv/bin/python" ]] && "$DEPS/venv/bin/python" -c "import torch, numpy" 2>/dev/null; then
    say "model packages: already in $DEPS/venv"
  else
    say "model packages: venv $DEPS/venv with numpy and torch (CPU build, ~200 MB)"
    if python3 -m venv "$DEPS/venv" 2>/dev/null; then
      "$DEPS/venv/bin/pip" install -q --upgrade pip >/dev/null 2>&1
      "$DEPS/venv/bin/pip" install -q numpy || say "WARNING: numpy install failed"
      "$DEPS/venv/bin/pip" install -q torch --index-url https://download.pytorch.org/whl/cpu \
        || "$DEPS/venv/bin/pip" install -q torch || say "WARNING: torch install failed (training needs it)"
      "$DEPS/venv/bin/python" -c "import torch, numpy; print('  torch', torch.__version__, 'numpy', numpy.__version__)" \
        || say "WARNING: model packages incomplete"
    else
      say "WARNING: python3 -m venv not available (python3-venv); install numpy + torch yourself for training"
    fi
  fi
fi

# ---- 6. compile the profilers --------------------------------------------------------------------------
JAVAC="$JAVA_HOME/bin/javac"
WCP="$WAYANG_HOME/jars/*:$WAYANG_HOME/libs/*"
SPARK_CP="${SPARK_JARS:-$(cat "$CM/spark-profiler/build/spark-classpath.txt" 2>/dev/null)}"
for p in java spark postgres; do
  P="$(tr '[:lower:]' '[:upper:]' <<<"${p:0:1}")${p:1}"
  mkdir -p "$CM/$p-profiler/build"
  cp="$WCP"; [[ $p == spark ]] && cp="$WCP:$SPARK_CP"
  "$JAVAC" -nowarn -d "$CM/$p-profiler/build" -cp "$cp" "$CM/$p-profiler/src/wayangprof/${P}OperatorProfiler.java" \
    || fail "$p profiler does not compile against this Wayang ($WAYANG_HOME): use the bundle's Wayang jars"
  say "compiled: $p profiler"
done

{ echo "prepared $(date '+%F %T') on $(hostname)"; env_report; } >"$DEPS/READY"
say "environment:"; env_report
say "prepare DONE. Everything needed is local now; the internet can be disconnected."
say "next: ./test.sh   (~15 min, checks every part on one small operator per platform)"
