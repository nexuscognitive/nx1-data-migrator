#!/bin/bash
# Smoke tests for the CDP edge node. Run as root inside the container:
#   docker exec cdp-edge-node bash /test-kerberos.sh
set -uo pipefail

FQDN="$(hostname -f)"
FAILS=0
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; FAILS=$((FAILS + 1)); }

# spark-submit, unlike a piped pyspark shell, exits non-zero when the query fails.
cat > /tmp/show_databases.py <<'PYEOF'
from pyspark.sql import SparkSession
SparkSession.builder.enableHiveSupport().getOrCreate().sql("SHOW DATABASES").show()
PYEOF
chmod 644 /tmp/show_databases.py
SHOW_DBS="spark-submit --master 'local[1]' /tmp/show_databases.py"

# A bare SSH command gets only sshd's minimal environment and never reads a profile.
as_bare()  { runuser -u svc_migration -- env -i HOME=/home/svc_migration USER=svc_migration PATH=/usr/bin:/bin bash -c "$1"; }
as_login() { runuser -u svc_migration -- bash -lc "$1"; }

echo "== CLI layout (bare, non-login session) =="
for c in "hadoop version" "hdfs dfs -ls /" "beeline --version" "spark-submit --version" "python3 --version"; do
    if as_bare "$c" > /dev/null 2>&1; then pass "bare: $c"; else fail "bare: $c"; fi
done

echo "== HDFS, beeline, PySpark =="
if as_bare "echo hello > /tmp/rt.txt && hdfs dfs -put -f /tmp/rt.txt /user/svc_migration/rt.txt && hdfs dfs -cat /user/svc_migration/rt.txt" | grep -q hello; then
    pass "hdfs put/get round trip"
else
    fail "hdfs put/get round trip"
fi
if as_bare "beeline -u 'jdbc:hive2://${FQDN}:10000/default' --silent=true -e 'SHOW DATABASES;'" 2>&1 | grep -q default; then
    pass "beeline SHOW DATABASES"
else
    fail "beeline SHOW DATABASES"
fi
if as_bare "$SHOW_DBS" 2>&1 | grep -q default; then
    pass "pyspark SHOW DATABASES (metastore over thrift)"
else
    fail "pyspark SHOW DATABASES (metastore over thrift)"
fi

echo "== ${FAILS} failure(s) =="
[ "$FAILS" -eq 0 ]
