#!/usr/bin/env bash
# Packs everything the server needs into one self-contained archive: no internet, Maven, Wayang source or Java
# needed on the server, only python3 (Java 17 is included).
#
#   ./make-bundle.sh                 -> ../dist/cost-model-server.tar.gz, with Java 17 for x64 and ARM Linux
#   ./make-bundle.sh --jdk x64       only the x64 (Intel/AMD) JDK: ~190 MB smaller
#   ./make-bundle.sh --jdk none      no JDK: the server must have Java 17
#
# Contents: the Wayang distribution (without jars the profilers never load), the Spark 3.5 jars, a Java 17 JDK
# (Eclipse Temurin, checksum-verified), the Java, Spark and PostgreSQL profilers, this server/ folder and the model
# code. PostgreSQL itself is not included (postgres-profiler/start-local-postgres.sh uses the server's binaries). No measured data:
# the server starts with empty results.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
ROOT="$(cd "$REPO/.." && pwd)"
DIST="${WAYANG_HOME:-$ROOT/wayang/wayang-assembly/target/apache-wayang-assembly-1.1.2-SNAPSHOT-dist/wayang-1.1.2-SNAPSHOT}"
[[ -d "$DIST/jars" ]] || DIST="$ROOT/wayang/wayang-assembly/target/apache-wayang-assembly-1.1.2-SNAPSHOT-dist/wayang-1.1.2-SNAPSHOT"
SPARK_CP_FILE="$REPO/spark-profiler/build/spark-classpath.txt"
OUT="$REPO/dist"
JDK=both
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT="$2"; shift 2 ;;
    --jdk) JDK="$2"; shift 2 ;;
    *) OUT="$1"; shift ;;   # old form: ./make-bundle.sh DIR
  esac
done
B="$OUT/cost-model-server"

[[ -d "$DIST/jars" ]] || { echo "Wayang distribution not found at $DIST"; exit 1; }
[[ -s "$SPARK_CP_FILE" ]] || { echo "Run spark-profiler/run-profiling.sh once first (it writes $SPARK_CP_FILE)"; exit 1; }

rm -rf "$B"
mkdir -p "$B/wayang/jars" "$B/wayang/libs" "$B/spark-jars"

echo "Wayang jars"
cp "$DIST"/jars/*.jar "$B/wayang/jars/"
# Not used by the Java/Spark profilers: TensorFlow natives (~760 MB), AWS SDK (~300 MB), RocksDB, Sedona.
for f in "$DIST"/libs/*.jar; do
  case "$(basename "$f")" in
    tensorflow-*|aws-java-sdk-bundle-*|rocksdbjni-*|sedona-*) ;;
    *) cp "$f" "$B/wayang/libs/" ;;
  esac
done

echo "Spark jars"
tr ':' '\n' <"$SPARK_CP_FILE" | while read -r j; do [[ -f "$j" ]] && cp "$j" "$B/spark-jars/"; done

# Java 17 for Linux (Eclipse Temurin), so the server needs nothing installed and runs the same Java everywhere.
fetch_jdk() {  # arch (x64 | aarch64)
  local arch="$1" cache="$OUT/jdk-cache/$1" meta link sum name
  mkdir -p "$cache"
  meta=$(curl -sf --max-time 60 "https://api.adoptium.net/v3/assets/latest/17/hotspot?architecture=$arch&image_type=jdk&os=linux&vendor=eclipse") \
    || { echo "Could not reach api.adoptium.net (use --jdk none, or try again)"; exit 1; }
  read -r link sum name < <(python3 -c 'import json,sys; b=json.load(sys.stdin)[0]["binary"]["package"]; print(b["link"], b["checksum"], b["name"])' <<<"$meta")
  if [[ ! -f "$cache/$name" ]]; then
    echo "  downloading $name"
    curl -sfL --max-time 900 -o "$cache/$name.part" "$link" && mv "$cache/$name.part" "$cache/$name"
  fi
  local got; got=$(shasum -a 256 "$cache/$name" 2>/dev/null || sha256sum "$cache/$name"); got=${got%% *}
  [[ "$got" == "$sum" ]] || { rm -f "$cache/$name"; echo "Checksum mismatch for $name: deleted, try again"; exit 1; }
  echo "  $name: checksum ok"
  mkdir -p "$B/jdk/linux-$arch"
  tar -xzf "$cache/$name" -C "$B/jdk/linux-$arch" --strip-components=1
}
case "$JDK" in
  none) ;;
  x64|aarch64) echo "Java 17 ($JDK)"; fetch_jdk "$JDK" ;;
  both) echo "Java 17 (x64 + aarch64)"; fetch_jdk x64; fetch_jdk aarch64 ;;
  *) echo "--jdk must be none, x64, aarch64 or both"; exit 1 ;;
esac

echo "Code"
mkdir -p "$B/Cost-Model"
for p in java-profiler spark-profiler postgres-profiler; do
  mkdir -p "$B/Cost-Model/$p/src"
  cp -R "$REPO/$p/src/wayangprof" "$B/Cost-Model/$p/src/"
  if [[ -f "$REPO/$p/native/pidrusage.c" ]]; then
    mkdir -p "$B/Cost-Model/$p/native" && cp "$REPO/$p/native/pidrusage.c" "$B/Cost-Model/$p/native/"
  fi
  for f in run-profiling.sh analyze.py wayang_estimates.py log4j2-quiet.xml README.md start-local-postgres.sh; do
    [[ -f "$REPO/$p/$f" ]] && cp "$REPO/$p/$f" "$B/Cost-Model/$p/"
  done
done
mkdir -p "$B/Cost-Model/server" "$B/Cost-Model/model"
cp "$HERE"/*.sh "$HERE"/*.py "$HERE"/*.md "$HERE"/*.slurm "$B/Cost-Model/server/" 2>/dev/null || true
cp "$REPO"/model/*.py "$REPO/model/requirements.txt" "$REPO/model/README.md" "$B/Cost-Model/model/"
mkdir -p "$B/Cost-Model/common" && cp "$REPO"/common/*.sh "$REPO"/common/*.py "$B/Cost-Model/common/"
cp "$REPO/README.md" "$REPO/plan.md" "$B/Cost-Model/"

echo "Archive"
tar -C "$OUT" -czf "$OUT/cost-model-server.tar.gz" cost-model-server
du -sh "$B" "$OUT/cost-model-server.tar.gz"
echo "Copy to the server:  scp $OUT/cost-model-server.tar.gz user@server:"
echo "Then on the server:  tar xzf cost-model-server.tar.gz && cd cost-model-server/Cost-Model/server && ./prepare.sh && ./test.sh"
