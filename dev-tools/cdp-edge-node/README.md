# CDP edge node with Kerberos

A single-container edge node that looks like a **CDP Private Cloud Base 7.1.x** edge node
with **Kerberos** turned on. It exists to test the migration DAGs (DAG 1
`source_to_s3_migration`, DAG 3 `folder_only_data_copy`) on a Kerberized cluster before
they are handed over to the client.

Ticket: [WF-520](https://linear.app/nexuscognitive/issue/WF-520)

Two rules shape this node:

- **It mirrors a real CDP edge node, not a convenient one.** HDFS, the Hive metastore and
  HiveServer2 all reject clients without a valid ticket. The CLIs sit in `/usr/bin` and
  config in `/etc/*/conf`, as on CDP.
- **It never works around DAG behaviour.** If a DAG fails here, the DAG gets fixed.

## Stack

| Component | Version / value |
|---|---|
| OS | Ubuntu 18.04, OpenJDK 8 |
| Hadoop | 3.1.1 (HDFS NameNode + DataNode; no YARN, DistCp runs `local`) |
| Hive | 3.1.3 (metastore on embedded Derby + HiveServer2) |
| Spark | 2.4.8 (`bin-hadoop2.7`, with the Hadoop 3 jars added) |
| Python | 3.6 (Spark 2.4 does not support 3.8; CDP 7.1.x edge nodes ship 3.6) |
| Kerberos | MIT KDC in the same container, realm `CDP.LOCAL` |
| SSH user | `svc_migration` (root SSH login is disabled) |

The node's hostname is read at startup (`hostname -f`) and written into every config file
and principal, so the same image runs in docker-compose (`cdp-edge`) and in es-tenant-2
(`cdp-edge.cdp-edge.es-tenant-2.svc.cluster.local`).

| Principal | Keytab (`/etc/security/keytabs/`) | Used by |
|---|---|---|
| `hdfs/<fqdn>@CDP.LOCAL` | `hdfs.keytab` | NameNode, DataNode; HDFS superuser |
| `HTTP/<fqdn>@CDP.LOCAL` | `spnego.keytab` | HDFS web UIs |
| `hive/<fqdn>@CDP.LOCAL` | `hive.keytab` | Metastore, HiveServer2 |
| `svc_migration@CDP.LOCAL` | `svc_migration.keytab` | The DAG's service account |

The KDC database and keytabs are created on first boot and kept on the data volume
(`/hadoop/data`), so they survive restarts. `svc_migration`'s `~/.profile` gets a ticket
from its keytab whenever there isn't one; the DAG only checks that it has one, for
`service_account_user_id` (`klist`).

## Run locally

Needs Docker with compose. The first build downloads Hadoop, Hive and Spark. By default
they come from archive.apache.org, which throttles hard (tens of KB/s); point
`APACHE_MIRROR` at any full Apache mirror instead. The Dockerfile checks each download
against Apache's own checksums, so a mirror can't change what gets installed.

```bash
cd dev-tools/cdp-edge-node
APACHE_MIRROR=https://mirrors.huaweicloud.com/apache docker compose up -d --build
docker logs -f cdp-edge-node          # wait for "CDP edge node ready on cdp-edge"
docker exec cdp-edge-node bash /test-kerberos.sh
```

SSH: `ssh -p 2225 svc_migration@localhost` (password `svc_migration`, local only).
NameNode UI: http://localhost:9873. HiveServer2: `localhost:10003`.

## Seed test data

The seeding scripts are shared with the MapR node (`dev-tools/fixtures/`). Run them as
`svc_migration` through a login shell, so the ticket and the node's addresses
(`/etc/profile.d/cdp-edge-seed.sh`) are picked up:

```bash
docker exec cdp-edge-node runuser -u svc_migration -- bash -lc 'bash /setup-test-data.sh && bash /setup-distcp-test-data.sh'
# k8s
kubectl exec -n es-tenant-2 deploy/cdp-edge -- runuser -u svc_migration -- bash -lc 'bash /setup-test-data.sh && bash /setup-distcp-test-data.sh'
```

## Admin access

Root can't log in over SSH. Use `docker exec` / `kubectl exec`, then get the HDFS
superuser's ticket:

```bash
kinit -kt /etc/security/keytabs/hdfs.keytab "hdfs/$(hostname -f)@CDP.LOCAL"
hdfs dfs -ls /
kdestroy
```

## Using Kerberos on the node

```bash
klist                                   # current ticket
kdestroy                                # drop it (.profile gets a new one on the next login)
beeline -u "jdbc:hive2://$(hostname -f):10000/default;principal=hive/$(hostname -f)@CDP.LOCAL"
pyspark --master 'local[1]'             # nothing extra needed; uses the ticket cache
```

## Deploy to es-tenant-2

The image is built from the private `nx1-full-package` repo (branch
`chore/cdp-edge-image-build`), because this public repo must not hold registry
credentials. The tag is pinned in `deploy/es-tenant-2/cdp-edge-deployment.yaml`.

```bash
kubectl create secret generic cdp-edge-ssh -n es-tenant-2 --from-literal=password='<choose one>'
kubectl apply -f dev-tools/cdp-edge-node/deploy/es-tenant-2/
kubectl get pod -n es-tenant-2 -l app=cdp-edge -w          # until READY 1/1
kubectl exec -n es-tenant-2 deploy/cdp-edge -- bash /test-kerberos.sh
```

The pod only turns Ready once HDFS and Hive are up, so the SSH Service doesn't route
the DAG to a half-started node.

## Airflow and DAG settings

- **SSH connection `cdp_edge_ssh`:** host `cdp-edge-ssh.es-tenant-2.svc.cluster.local`,
  port 22, login `svc_migration`, password from the `cdp-edge-ssh` Secret.
- **DAG settings** (per-DAG env file or Airflow Variables):

  ```
  CLUSTER_SSH_CONN_ID=cdp_edge_ssh
  AUTH_METHOD=kinit
  CLUSTER_TYPE=CDP
  SERVICE_ACCOUNT_USER_ID=svc_migration
  ```

## Troubleshooting

- **Daemon logs:** `/opt/hadoop/logs/hadoop-root-namenode-*.log`,
  `/opt/hadoop/logs/hadoop-root-datanode-*.log`, `/var/log/hive-metastore.log`,
  `/var/log/hiveserver2.log`, `/var/log/krb5kdc.log`.
- **"Server not found in Kerberos database":** the principal a client asked for doesn't
  exist. Compare `hostname -f` with `klist -kt /etc/security/keytabs/hdfs.keytab`; a
  volume reused under another hostname gets new principals on the next boot.
- **"Clock skew too great":** the container clock drifted from the KDC's; restart it.
- **DAG fails with `CLUSTER_AUTH_FAIL`:** `svc_migration` has no ticket. Check the keytab
  exists and `runuser -u svc_migration -- bash -lc klist` shows a ticket.
