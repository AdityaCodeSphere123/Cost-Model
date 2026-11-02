#!/usr/bin/env bash
# Runs the multi-operator job benchmark and writes results/job_runs.csv, job_tasks.csv, job_operators.csv.
#   ./run-jobs.sh                 all jobs, 1 warm-up + 3 measured runs each
#   ./run-jobs.sh 0 1 "map-sort"  quick test of one job
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
DEFAULT_DIST="$ROOT/wayang/wayang-assembly/target/apache-wayang-assembly-1.1.2-SNAPSHOT-dist/wayang-1.1.2-SNAPSHOT"
WAYANG_HOME="${WAYANG_HOME:-$DEFAULT_DIST}"
[[ -d "$WAYANG_HOME/jars" ]] || WAYANG_HOME="$DEFAULT_DIST"  # must be the built distribution
if [[ -z "${JAVA_HOME:-}" && -d /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ]]; then
  JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
fi
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"; JAVAC="${JAVA_HOME:+$JAVA_HOME/bin/}javac"
SPARK_CP="$(cat "$HERE/../spark-profiler/build/spark-classpath.txt")"  # made by spark-profiler/run-profiling.sh
CP="$WAYANG_HOME/jars/*:$WAYANG_HOME/libs/*:$SPARK_CP"
mkdir -p "$HERE/build" "$HERE/results"
if [[ ! -f "$HERE/build/wayangprof/JobBench.class" || "$HERE/src/wayangprof/JobBench.java" -nt "$HERE/build/wayangprof/JobBench.class" ]]; then
  "$JAVAC" -nowarn -d "$HERE/build" -cp "$CP" "$HERE/src/wayangprof/JobBench.java"
fi
OPENS=()
for p in java.lang java.lang.invoke java.lang.reflect java.io java.net java.nio java.util java.util.concurrent \
         java.util.concurrent.atomic sun.nio.ch sun.nio.cs sun.security.action sun.util.calendar; do
  OPENS+=("--add-opens=java.base/$p=ALL-UNNAMED")
done
# Same JVM setting as the profilers' main resource (4g heap, G1, all cores).
"$JAVA" -Xms4g -Xmx4g -XX:+UseG1GC "${OPENS[@]}" -Dlog4j.configurationFile="file://$HERE/../spark-profiler/log4j2-quiet.xml" \
  -Dspark.local.dir="$HERE/results/tmp-spark" -cp "$HERE/build:$CP" wayangprof.JobBench \
  "$HERE/../java-profiler/data" "${OUT:-$HERE/results}" "${1:-1}" "${2:-3}" ${3:+"$3"} 2>&1 | grep "\[jobbench\]\|Exception\|Error"
rm -rf "$HERE/results/tmp-spark"
