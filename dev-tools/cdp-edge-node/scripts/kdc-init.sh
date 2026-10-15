#!/bin/bash
# Creates the KDC database on first boot and, on every boot, any principal or keytab
# entry missing for this host. Safe to re-run: existing keys are never rotated, so
# keytabs issued earlier stay valid.
set -euo pipefail

REALM=CDP.LOCAL
FQDN="${EDGE_FQDN:-$(hostname -f)}"
KT_DIR=/etc/security/keytabs

mkdir -p /hadoop/data/krb5kdc /hadoop/data/keytabs
chmod 700 /hadoop/data/krb5kdc
chmod 755 /hadoop/data/keytabs

if [ ! -f /var/lib/krb5kdc/principal ]; then
    # The stash file (-s) unlocks the database at startup, so the master password is never needed again.
    kdb5_util create -s -r "$REALM" -P "$(head -c 32 /dev/urandom | base64)"
fi

# keytab file name, principal (without realm), owner
ensure() {
    local file="$KT_DIR/$1" princ="$2@$REALM" owner="$3"
    if ! kadmin.local -q "getprinc $princ" 2>/dev/null | grep -q "^Principal: $princ$"; then
        kadmin.local -q "addprinc -randkey $princ" > /dev/null
    fi
    # -norandkey exports the current key instead of generating a new one.
    if ! klist -kt "$file" 2>/dev/null | grep -q " $princ$"; then
        kadmin.local -q "ktadd -norandkey -k $file $princ" > /dev/null
    fi
    # kadmin.local exits 0 even when a query fails, so verify the result.
    if ! klist -kt "$file" | grep -q " $princ$"; then
        echo "ERROR: keytab $file has no entry for $princ" >&2
        exit 1
    fi
    chown "$owner" "$file"
    chmod 400 "$file"
}

ensure hdfs.keytab          "hdfs/$FQDN" root
ensure spnego.keytab        "HTTP/$FQDN" root
ensure hive.keytab          "hive/$FQDN" root
ensure svc_migration.keytab svc_migration svc_migration
echo "kdc-init: principals and keytabs ready for $FQDN"
