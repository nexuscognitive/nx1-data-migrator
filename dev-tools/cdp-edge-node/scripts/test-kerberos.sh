#!/bin/bash
# Smoke tests for the CDP edge node. Run as root inside the container:
#   docker exec cdp-edge-node bash /test-kerberos.sh
# Temporarily moves svc_migration's keytab aside (restored on exit).
set -uo pipefail

FQDN="$(hostname -f)"
KEYTAB=/etc/security/keytabs/svc_migration.keytab
BEELINE_URL="jdbc:hive2://${FQDN}:10000/default;principal=hive/${FQDN}@CDP.LOCAL"
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

restore_keytab() { [ -f "${KEYTAB}.aside" ] && mv "${KEYTAB}.aside" "$KEYTAB"; return 0; }
trap restore_keytab EXIT

echo "== Ticket from .profile =="
as_bare "kdestroy 2>/dev/null"
if as_login "klist -s"; then pass "login shell gets a ticket from the keytab"; else fail "login shell gets a ticket from the keytab"; fi

echo "== Concurrent ticket acquisition (parallel DAG tasks) =="
as_bare "kdestroy 2>/dev/null"
for i in 1 2 3 4 5; do as_login "klist -s && echo OK" > "/tmp/par.$i" 2>&1 & done
wait
OKS=$(cat /tmp/par.* | grep -c '^OK$'); rm -f /tmp/par.*
if [ "$OKS" -eq 5 ]; then pass "5 parallel login shells all hold a ticket"; else fail "only $OKS/5 parallel login shells hold a ticket"; fi

echo "== CLI layout (bare, non-login session; ticket cache is shared) =="
for c in "hadoop version" "hdfs dfs -ls /" "beeline --version" "spark-submit --version" "python3 --version"; do
    if as_bare "$c" > /dev/null 2>&1; then pass "bare: $c"; else fail "bare: $c"; fi
done

echo "== With a ticket =="
# Output is captured before grepping: under pipefail, `cmd | grep -q` fails at random
# when grep exits on the first match and cmd dies of SIGPIPE.
expect_output() {  # description, command, pattern
    local out
    out="$(as_bare "$2" 2>&1)"
    if grep -q -- "$3" <<< "$out"; then pass "$1"; else fail "$1"; fi
}
expect_output "hdfs put/get round trip through the DataNode" \
    "echo hello > /tmp/rt.txt && hdfs dfs -put -f /tmp/rt.txt /user/svc_migration/rt.txt && hdfs dfs -cat /user/svc_migration/rt.txt" hello
expect_output "beeline with principal: SHOW DATABASES" \
    "beeline -u '${BEELINE_URL}' --silent=true -e 'SHOW DATABASES;'" default
expect_output "pyspark lists databases through the SASL metastore" "$SHOW_DBS" default

echo "== Without a ticket (keytab moved aside) =="
mv "$KEYTAB" "${KEYTAB}.aside"
as_bare "kdestroy 2>/dev/null"
if as_login "klist -s"; then fail "login shell still has a ticket"; else pass "no keytab, no ticket"; fi
expect_auth_failure() {  # description, command
    local out rc
    out="$(as_bare "$2" 2>&1)"
    rc=$?
    if [ "$rc" -ne 0 ] && grep -qiE 'GSS|Kerberos|SASL|TGT|authentication' <<< "$out"; then
        pass "$1 fails with an authentication error"
    else
        echo "$out" > "/tmp/test-kerberos.failed.log"
        fail "$1 did not fail with an authentication error (exit $rc; full output in /tmp/test-kerberos.failed.log)"
    fi
}
expect_auth_failure "hdfs dfs -ls /" "hdfs dfs -ls /"
expect_auth_failure "beeline" "beeline -u '${BEELINE_URL}' --silent=true -e 'SHOW DATABASES;'"
expect_auth_failure "pyspark" "$SHOW_DBS"
restore_keytab

echo "== ${FAILS} failure(s) =="
[ "$FAILS" -eq 0 ]
