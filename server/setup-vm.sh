#!/usr/bin/env bash
# One-time preparation of the VM (Debian/Ubuntu or RHEL/Rocky/Alma/Fedora). Run it as your normal user; it asks for
# sudo only where it needs it. Safe to run again.
#
#   ./setup-vm.sh
#
# What it does
#   1. installs what the profilers need: python3, perl, unzip, taskset (util-linux), PostgreSQL and Java 17 (JDK)
#   2. installs Apache Spark 3.5.7 (the version Wayang is built against; hadoop3 build) from apache.org, checksum
#      verified, into /opt/spark-3.5.7, linked as /opt/spark; SPARK_HOME, JAVA_HOME and PATH for all users in
#      /etc/profile.d/wayang-profiling.sh
#   3. if this script installed PostgreSQL, stops and disables the distribution's default PostgreSQL service: the
#      profiling server below is a separate one, and an idle second server would still wake up in the background
#   4. makes the CPU energy counter (Intel/AMD RAPL) readable, and sets the CPU frequency policy to "performance",
#      if the VM exposes them (most VMs do not; both reset at reboot: run this script again after a reboot)
#   5. creates and starts the dedicated profiling server (../postgres-profiler/start-local-postgres.sh):
#      127.0.0.1:5433, database wayangprof, autovacuum off, owned by you (so its CPU can be measured)
# The bundle also ships Java 17 and the Spark jars, and the scripts use those first (the very same jars as on the Mac);
# the installed Java and Spark are used when a bundle has none (make-bundle.sh --jdk none) and for your own use
# (spark-shell, spark-submit). Options: --spark-version 3.5.7, --no-spark, --no-java.
set -euo pipefail
SPARK_VERSION=3.5.7
INSTALL_SPARK=1
INSTALL_JAVA=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --spark-version) SPARK_VERSION="$2"; shift 2 ;;
    --no-spark) INSTALL_SPARK=0; shift ;;
    --no-java) INSTALL_JAVA=0; shift ;;
    -h|--help) sed -n '2,/^[^#]/p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
done
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
say() { echo "[setup] $*"; }

if [[ "$(id -u)" -eq 0 ]]; then
  echo "Run this as your normal user (not root, not with sudo): it calls sudo itself, and PostgreSQL refuses to run as root."
  exit 1
fi
[[ "$(uname -s)" == Linux ]] || { echo "This script is for the Linux VM."; exit 1; }
sudo -v || { echo "sudo is needed"; exit 1; }

# ---- 1. packages ---------------------------------------------------------------------------------
have_pg() { ls /usr/lib/postgresql/*/bin/pg_ctl /usr/pgsql-*/bin/pg_ctl 2>/dev/null | grep -q . || command -v pg_ctl >/dev/null; }
PG_BEFORE=0; have_pg && PG_BEFORE=1
if command -v apt-get >/dev/null; then
  say "installing packages with apt-get"
  sudo apt-get update -qq
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3 perl unzip curl util-linux postgresql postgresql-contrib >/dev/null
  # Separately: some releases (e.g. Debian 13) have no Java 17 package; the bundle's Java 17 is used then.
  if (( INSTALL_JAVA )); then
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq openjdk-17-jdk-headless >/dev/null \
      || say "WARNING: no openjdk-17 package here; the bundle's Java 17 will be used"
  fi
  SERVICE=postgresql
elif command -v dnf >/dev/null || command -v yum >/dev/null; then
  PM=$(command -v dnf || command -v yum)
  say "installing packages with $(basename "$PM")"
  sudo "$PM" install -y -q python3 perl unzip curl util-linux postgresql-server postgresql postgresql-contrib >/dev/null
  if (( INSTALL_JAVA )); then
    sudo "$PM" install -y -q java-17-openjdk-devel >/dev/null \
      || say "WARNING: no java-17-openjdk package here; the bundle's Java 17 will be used"
  fi
  SERVICE=postgresql
else
  say "no apt-get or dnf: install python3, perl, unzip, util-linux and PostgreSQL by hand, then run this again"
  exit 1
fi
have_pg || { say "PostgreSQL binaries still not found after installing"; exit 1; }

# ---- Java 17 --------------------------------------------------------------------------------------
JDK17=""
for d in /usr/lib/jvm/java-17-openjdk* /usr/lib/jvm/java-17 /usr/lib/jvm/temurin-17*; do
  [[ -x "$d/bin/javac" ]] && { JDK17="$d"; break; }
done
if [[ -n "$JDK17" ]]; then
  say "Java 17: $JDK17 ($("$JDK17/bin/java" -version 2>&1 | head -1))"
elif (( INSTALL_JAVA )); then
  say "WARNING: Java 17 JDK not found after installing; the bundle's Java 17 is used instead"
fi

# ---- Spark ----------------------------------------------------------------------------------------
SPARK_DIR="/opt/spark-$SPARK_VERSION"
if (( INSTALL_SPARK )); then
  if [[ -x "$SPARK_DIR/bin/spark-submit" ]]; then
    say "Spark $SPARK_VERSION already installed in $SPARK_DIR"
  else
    name="spark-$SPARK_VERSION-bin-hadoop3"
    tmp=$(mktemp -d)
    got=""
    # The CDN only keeps current releases; the archive keeps all of them.
    for base in "https://dlcdn.apache.org/spark" "https://archive.apache.org/dist/spark"; do
      say "downloading $name.tgz from $base (~400 MB)"
      if curl -fsSL --retry 3 --max-time 3600 -o "$tmp/$name.tgz" "$base/spark-$SPARK_VERSION/$name.tgz" \
         && curl -fsSL --retry 3 --max-time 60 -o "$tmp/$name.tgz.sha512" "$base/spark-$SPARK_VERSION/$name.tgz.sha512"; then
        got="$base"; break
      fi
    done
    [[ -n "$got" ]] || { say "could not download Spark $SPARK_VERSION (no internet? use --no-spark: the bundle has the Spark jars)"; exit 1; }
    # The .sha512 file is "<hash>  <file>" (newer releases) or "<file>: <HASH IN GROUPS>" (older ones).
    want=$(sed 's/^[^:]*: //' "$tmp/$name.tgz.sha512" | tr -d ' \n' | grep -oiE '[0-9a-f]{128}' | head -1 | tr 'A-F' 'a-f')
    have=$(sha512sum "$tmp/$name.tgz" | cut -d' ' -f1)
    [[ -n "$want" && "$want" == "$have" ]] || { rm -rf "$tmp"; say "Spark download checksum MISMATCH: deleted, run again"; exit 1; }
    say "checksum ok; installing into $SPARK_DIR"
    sudo mkdir -p "$SPARK_DIR"
    sudo tar -xzf "$tmp/$name.tgz" -C "$SPARK_DIR" --strip-components=1
    rm -rf "$tmp"
  fi
  sudo ln -sfn "$SPARK_DIR" /opt/spark
  # Wayang is built against this Spark and Scala 2.12: the jars must match.
  ls "$SPARK_DIR"/jars/spark-core_2.12-"$SPARK_VERSION".jar >/dev/null 2>&1 \
    || say "WARNING: $SPARK_DIR has no spark-core_2.12-$SPARK_VERSION.jar: not the Scala 2.12 build Wayang needs"
  say "Spark: $("$SPARK_DIR/bin/spark-submit" --version 2>&1 | grep -m1 -o 'version [0-9.]*' || echo "$SPARK_VERSION") in /opt/spark"
fi
{
  echo "# written by Cost-Model/server/setup-vm.sh"
  [[ -n "$JDK17" ]] && echo "export JAVA_HOME=$JDK17"
  (( INSTALL_SPARK )) && echo "export SPARK_HOME=/opt/spark" && echo 'export PATH="$SPARK_HOME/bin:$PATH"'
  [[ -n "$JDK17" ]] && echo 'export PATH="$JAVA_HOME/bin:$PATH"'
  true
} | sudo tee /etc/profile.d/wayang-profiling.sh >/dev/null
say "environment for new logins: /etc/profile.d/wayang-profiling.sh (JAVA_HOME, SPARK_HOME, PATH)"

# ---- the distribution's own PostgreSQL service -------------------------------------------------
if (( ! PG_BEFORE )) && command -v systemctl >/dev/null; then
  if systemctl list-unit-files 2>/dev/null | grep -q "^$SERVICE"; then
    say "disabling the default '$SERVICE' service (installed just now, not used: profiling uses its own server)"
    sudo systemctl disable --now "$SERVICE" >/dev/null 2>&1 || true
  fi
elif (( PG_BEFORE )); then
  say "PostgreSQL was already installed: its service is left as it is (stop it if nothing else needs it,"
  say "  so it does not run next to the measurements: sudo systemctl stop postgresql)"
fi

# ---- energy counter and CPU frequency ----------------------------------------------------------
rapl=$(ls /sys/class/powercap/intel-rapl:*/energy_uj 2>/dev/null || true)
if [[ -n "$rapl" ]]; then
  sudo chmod a+r $rapl && say "energy counter (RAPL) readable: energy columns will be filled"
else
  say "no energy counter (RAPL) on this VM: energy columns stay empty (normal for VMs)"
fi
gov=$(ls /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null || true)
if [[ -n "$gov" ]]; then
  echo performance | sudo tee $gov >/dev/null 2>&1 && say "CPU frequency policy: performance" \
    || say "could not set the CPU frequency policy (left as it is)"
else
  say "CPU frequency policy not exposed (normal for VMs; the host decides)"
fi

# ---- the profiling server ----------------------------------------------------------------------
"$REPO/postgres-profiler/start-local-postgres.sh"
chmod +x "$HERE"/*.sh "$REPO"/*/run-profiling.sh "$REPO/postgres-profiler/start-local-postgres.sh" 2>/dev/null || true

cat <<EOF

[setup] done. Next:
  cd $HERE
  ./run-server.sh --check --postgres local          # ~3 min: must end with CHECK PASSED, POSTGRES CHECK PASSED, DATA OK
  nohup ./run-server.sh --hours 84 --postgres local >/dev/null 2>&1 &
  tail -f ../results-server/server.log
Afterwards (never at the same time), the Spark clusters:
  ./run-cluster.sh --check && nohup ./run-cluster.sh --hours 72 >/dev/null 2>&1 &
EOF
