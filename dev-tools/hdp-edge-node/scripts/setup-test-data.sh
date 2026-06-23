#!/bin/bash
# Populates HDFS + Hive metastore with tables matching the migration DAG test cases.
# Run INSIDE the container: docker exec -u root hdp-edge-node bash /setup-test-data.sh
# Safe to re-run — existing HDFS files and Hive tables are overwritten/skipped cleanly.

BEELINE="/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 --silent=true"

echo "=== [1/4] Creating HDFS directories ==="
hdfs dfs -mkdir -p /user/hive/warehouse/sales_db.db/orders/year=2024/month=01
hdfs dfs -mkdir -p /user/hive/warehouse/sales_db.db/orders/year=2024/month=02
hdfs dfs -mkdir -p /user/hive/warehouse/sales_db.db/orders/year=2024/month=03
hdfs dfs -mkdir -p /user/hive/warehouse/hr_db.db/employees
hdfs dfs -mkdir -p /user/hive/warehouse/hr_db.db/departments
hdfs dfs -mkdir -p /user/hive/warehouse/analytics_db.db/events/region=US/year=2024/month=01
hdfs dfs -mkdir -p /user/hive/warehouse/analytics_db.db/events/region=EU/year=2024/month=01
hdfs dfs -mkdir -p /user/hive/warehouse/analytics_db.db/sessions/year=2024/month=01
hdfs dfs -mkdir -p /user/hive/warehouse/analytics_db.db/sessions/year=2024/month=02
hdfs dfs -mkdir -p /user/hive/warehouse/logs_db.db/app_logs/dt=2024-01-01
hdfs dfs -mkdir -p /user/hive/warehouse/logs_db.db/app_logs/dt=2024-01-15
hdfs dfs -mkdir -p /user/hive/warehouse/logs_db.db/app_logs/dt=2024-02-01

echo "=== [2/4] Writing sample data files to HDFS ==="
# Drop any stale TEXTFILE table definitions before (re)writing their CSV: each table
# below is created with CREATE TABLE IF NOT EXISTS in [3/4], which is a no-op if the
# table already exists in a different storage format (e.g. left STORED AS PARQUET by
# an earlier run). That leaves the metastore typed wrong over these CSV files, so
# table discovery reads a CSV as a Parquet footer and fails. These are managed tables,
# so the drop must run BEFORE the data is written — a drop afterwards would delete the
# CSV. Idempotent: a no-op on a fresh node.
$BEELINE -e "
CREATE DATABASE IF NOT EXISTS sales_db;
CREATE DATABASE IF NOT EXISTS hr_db;
CREATE DATABASE IF NOT EXISTS analytics_db;
CREATE DATABASE IF NOT EXISTS logs_db;
DROP TABLE IF EXISTS sales_db.orders;
DROP TABLE IF EXISTS hr_db.employees;
DROP TABLE IF EXISTS analytics_db.events;
DROP TABLE IF EXISTS analytics_db.sessions;
DROP TABLE IF EXISTS logs_db.app_logs;
"
echo -e "1,Alice,100\n2,Bob,200\n3,Carol,150" \
  | hdfs dfs -put -f - /user/hive/warehouse/sales_db.db/orders/year=2024/month=01/data.csv
echo -e "4,Dave,90\n5,Eve,310" \
  | hdfs dfs -put -f - /user/hive/warehouse/sales_db.db/orders/year=2024/month=02/data.csv
echo -e "6,Frank,400\n7,Grace,250\n8,Hank,180\n9,Ivy,320\n10,Jack,110" \
  | hdfs dfs -put -f - /user/hive/warehouse/sales_db.db/orders/year=2024/month=03/data.csv
echo -e "1,Alice,Engineering,\N\n2,Bob,HR,50000\n3,Carol,Engineering,55000\n4,Dave,\N,48000\n5,Eve,Finance,62000" \
  | hdfs dfs -put -f - /user/hive/warehouse/hr_db.db/employees/data.csv
echo -e "e1,click,US\ne2,view,US\ne3,buy,US" \
  | hdfs dfs -put -f - /user/hive/warehouse/analytics_db.db/events/region=US/year=2024/month=01/data.csv
echo -e "e4,click,EU" \
  | hdfs dfs -put -f - /user/hive/warehouse/analytics_db.db/events/region=EU/year=2024/month=01/data.csv
echo -e "s1,userA,300\ns2,userB,450" \
  | hdfs dfs -put -f - /user/hive/warehouse/analytics_db.db/sessions/year=2024/month=01/data.csv
echo -e "s3,userC,120\ns4,userD,600" \
  | hdfs dfs -put -f - /user/hive/warehouse/analytics_db.db/sessions/year=2024/month=02/data.csv
echo -e "2024-01-01,INFO,app started\n2024-01-01,WARN,slow query" \
  | hdfs dfs -put -f - /user/hive/warehouse/logs_db.db/app_logs/dt=2024-01-01/data.csv
echo -e "2024-01-15,ERROR,timeout\n2024-01-15,INFO,retry ok" \
  | hdfs dfs -put -f - /user/hive/warehouse/logs_db.db/app_logs/dt=2024-01-15/data.csv
echo -e "2024-02-01,INFO,deploy\n2024-02-01,INFO,health ok\n2024-02-01,DEBUG,trace" \
  | hdfs dfs -put -f - /user/hive/warehouse/logs_db.db/app_logs/dt=2024-02-01/data.csv

echo "=== [3/4] Creating Hive databases and tables ==="

cat > /tmp/setup-test-data.sql << 'SQL_EOF'
CREATE DATABASE IF NOT EXISTS sales_db;

CREATE TABLE IF NOT EXISTS sales_db.orders (
  order_id INT, customer STRING, amount DOUBLE
)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/sales_db.db/orders';

ALTER TABLE sales_db.orders ADD IF NOT EXISTS PARTITION (year=2024, month=1);
ALTER TABLE sales_db.orders ADD IF NOT EXISTS PARTITION (year=2024, month=2);
ALTER TABLE sales_db.orders ADD IF NOT EXISTS PARTITION (year=2024, month=3);

CREATE DATABASE IF NOT EXISTS hr_db;

CREATE TABLE IF NOT EXISTS hr_db.employees (
  emp_id INT, name STRING, dept STRING, salary DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
NULL DEFINED AS '\N'
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/hr_db.db/employees';

CREATE TABLE IF NOT EXISTS hr_db.departments (
  dept_id INT, dept_name STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/hr_db.db/departments';

CREATE DATABASE IF NOT EXISTS analytics_db;

CREATE TABLE IF NOT EXISTS analytics_db.events (
  event_id STRING, event_type STRING, user_region STRING
)
PARTITIONED BY (region STRING, year INT, month INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/analytics_db.db/events';

ALTER TABLE analytics_db.events ADD IF NOT EXISTS PARTITION (region='US', year=2024, month=1);
ALTER TABLE analytics_db.events ADD IF NOT EXISTS PARTITION (region='EU', year=2024, month=1);

CREATE TABLE IF NOT EXISTS analytics_db.sessions (
  session_id STRING, user_id STRING, duration_sec INT
)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/analytics_db.db/sessions';

CREATE DATABASE IF NOT EXISTS logs_db;

CREATE TABLE IF NOT EXISTS logs_db.app_logs (
  log_date STRING, level STRING, message STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/logs_db.db/app_logs';

ALTER TABLE logs_db.app_logs ADD IF NOT EXISTS PARTITION (dt='2024-01-01');
ALTER TABLE logs_db.app_logs ADD IF NOT EXISTS PARTITION (dt='2024-01-15');
ALTER TABLE logs_db.app_logs ADD IF NOT EXISTS PARTITION (dt='2024-02-01');
SQL_EOF

$BEELINE -f /tmp/setup-test-data.sql

echo "=== [4/4] Verifying ==="
$BEELINE -e "SHOW DATABASES;"
echo "Done. Test data is ready."