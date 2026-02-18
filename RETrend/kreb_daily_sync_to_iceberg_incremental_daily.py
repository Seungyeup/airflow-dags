from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

local_tz = pendulum.timezone("Asia/Seoul")

DAG_ID = "kreb_daily_sync_to_iceberg_incremental_daily"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=local_tz.datetime(2025, 1, 1, 0, 0),
    schedule_interval="30 2 * * *",  # daily_sync 이후(02:00) 조금 뒤
    catchup=False,
    max_active_runs=1,
    tags=["retrend", "kreb", "daily-sync", "iceberg"],
) as dag:
    daily_sync = KubernetesPodOperator(
        task_id="kreb_daily_sync_once",
        name="kreb-daily-sync-once",
        namespace="airflow",
        image="dave126/kreb-backfill:0.1.2",
        cmds=["python"],
        arguments=["/app/src/kreb/src/kreb_etl_v2/daily_sync.py"],
        env_vars={
            "MINIO_ENDPOINT": "http://172.30.1.28:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            # Airflow Variable `KREB_SERVICE_KEY`로 주입
            "KREB_SERVICE_KEY": "{{ var.value.KREB_SERVICE_KEY }}",
            "KREB_DAILY_LIMIT": "10000",
            "KREB_LAWD_CSV": "s3://retrend-raw-data/shigungu_list.csv",
            "KREB_DAILY_SYNC_STATE_URI": "s3://retrend-raw-data/kreb_state_daily_sync.json",
            "KREB_OUTPUT_URI": "s3://retrend-raw-data/bronze/kreb_etl_v2/apt_trade",
            "LOG_LEVEL": "INFO",
        },
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
    )

    # NOTE: This task requires RBAC that allows creating SparkApplication in kubeflow namespace.
    submit_spark_incremental = KubernetesPodOperator(
        task_id="spark_kreb_csv_to_iceberg_incremental",
        name="spark-kreb-csv-to-iceberg-incremental",
        namespace="airflow",
        image="bitnami/kubectl:1.29",
        startup_timeout_seconds=600,
        log_events_on_failure=True,
        cmds=["/bin/sh", "-c"],
        arguments=[
            """
set -euo pipefail

# Recreate SparkApplication each day.
kubectl -n kubeflow delete sparkapplication kreb-csv-to-iceberg-incremental --ignore-not-found

cat <<'YAML' | kubectl apply -f -
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: kreb-csv-to-iceberg-incremental
  namespace: kubeflow
spec:
  type: Python
  mode: cluster
  image: dave126/spark-py-s3a-iceberg:3.3.2
  imagePullPolicy: IfNotPresent
  mainApplicationFile: local:///opt/spark/app/kreb_csv_to_iceberg_incremental.py
  sparkVersion: 3.3.1
  restartPolicy:
    type: Never
  deps:
    packages:
      - org.apache.spark:spark-hadoop-cloud_2.12:3.3.1
      - org.apache.hadoop:hadoop-aws:3.3.4
      - com.amazonaws:aws-java-sdk-bundle:1.12.262
  sparkConf:
    spark.sql.extensions: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    spark.sql.catalog.iceberg: org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.iceberg.type: hive
    spark.sql.catalog.iceberg.uri: thrift://172.30.1.30:9083
    spark.sql.catalog.iceberg.warehouse: s3a://retrend-raw-data/warehouse/iceberg
    spark.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
    spark.hadoop.fs.s3a.endpoint: http://172.30.1.28:9000
    spark.hadoop.fs.s3a.access.key: minioadmin
    spark.hadoop.fs.s3a.secret.key: minioadmin
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.hadoop.fs.s3a.connection.ssl.enabled: "false"
    spark.jars.ivy: /tmp/.ivy
  driver:
    cores: 1
    memory: 2g
    serviceAccount: spark
    env:
      - name: HIVE_METASTORE_URI
        value: thrift://172.30.1.30:9083
      - name: BRONZE_PREFIX
        value: s3a://retrend-raw-data/bronze/kreb_etl_v2/apt_trade
      - name: ICEBERG_TABLE
        value: iceberg.default.apt_trade
      - name: WAREHOUSE_BASE
        value: s3a://retrend-raw-data/warehouse/iceberg
      - name: KREB_DAILY_SYNC_MANIFEST_PATH
        value: s3a://retrend-raw-data/bronze/kreb_etl_v2/apt_trade/_manifests/daily_sync/latest.json
      - name: AWS_ACCESS_KEY_ID
        value: minioadmin
      - name: AWS_SECRET_ACCESS_KEY
        value: minioadmin
      - name: RECREATE_TABLE
        value: "false"
  executor:
    instances: 2
    cores: 1
    memory: 2g
    env:
      - name: AWS_ACCESS_KEY_ID
        value: minioadmin
      - name: AWS_SECRET_ACCESS_KEY
        value: minioadmin
YAML

# Wait for completion (COMPLETED/FAILED)
for i in $(seq 1 360); do
  state=$(kubectl -n kubeflow get sparkapplication kreb-csv-to-iceberg-incremental -o jsonpath='{.status.applicationState.state}' 2>/dev/null || true)
  if [ "$state" = "COMPLETED" ]; then
    echo "SparkApplication completed"
    exit 0
  fi
  if [ "$state" = "FAILED" ]; then
    echo "SparkApplication failed"
    kubectl -n kubeflow describe sparkapplication kreb-csv-to-iceberg-incremental || true
    exit 1
  fi
  sleep 10
done

echo "Timed out waiting for SparkApplication"
kubectl -n kubeflow describe sparkapplication kreb-csv-to-iceberg-incremental || true
exit 1
"""
        ],
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
    )

    daily_sync >> submit_spark_incremental
