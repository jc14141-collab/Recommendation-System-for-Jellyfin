from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


with DAG(
    dag_id="bootstraper_job_v1",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["k8s", "pipeline", "bootstraper"],
) as dag:

    volumes = [
        k8s.V1Volume(
            name="host-minio-data",
            host_path=k8s.V1HostPathVolumeSource(path="/mnt/object/minio_data"),
        ),
        k8s.V1Volume(
            name="host-block-embedding",
            host_path=k8s.V1HostPathVolumeSource(path="/mnt/block/embedding"),
        ),
        # k8s.V1Volume(
        #     name="local-code",
        #     host_path=k8s.V1HostPathVolumeSource(
        #         path="/home/cc/project/data-pipeline/bootstraper"
        #     ),
        # ),
    ]

    volume_mounts = [
        k8s.V1VolumeMount(name="host-minio-data", mount_path="/data"),
        k8s.V1VolumeMount(name="host-block-embedding", mount_path="/mnt/block/embedding"),
        # k8s.V1VolumeMount(name="local-code", mount_path="/app"),
    ]

    init_containers = [
        k8s.V1Container(
            name="wait-for-postgres",
            image="busybox:1.28",
            command=[
                "sh",
                "-c",
                "until nc -zv postgres 5432; do echo 'Waiting for Postgres...'; sleep 3; done;",
            ],
        ),
    ]

    env_from = [
        k8s.V1EnvFromSource(
            secret_ref=k8s.V1SecretEnvSource(name="data-pipeline-secrets")
        ),
        k8s.V1EnvFromSource(
            config_map_ref=k8s.V1ConfigMapEnvSource(name="data-pipeline-config")
        ),
    ]

    run_bootstraper = KubernetesPodOperator(
        task_id="run_bootstraper",
        name="run-bootstraper",
        namespace="mlops",
        in_cluster=True,
        service_account_name="airflow-job-trigger",
        image="songchenxue/bootstraper:v1.0",
        image_pull_policy="Always",
        cmds=["/bin/bash", "-c"],
        arguments=[
            """
set -euo pipefail

cd /app

echo "[DEBUG] building DB URI..."
export PYICEBERG_CATALOG__DEFAULT__URI="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:5432/${POSTGRES_DB}"

echo "[STEP 1] Ingesting datasets..."
python3 -u scripts/ingest_datasets.py

echo "[STEP 2] Building text embeddings..."
python3 -u scripts/build_embedding_text.py

echo "[STEP 3] Running embedding..."
python3 -u scripts/embedding.py

echo "[STEP 4] Building index..."
python3 -u scripts/build_embedding_index.py

echo "[STEP 5] Building initial user..."
python3 -u scripts/build_initial_user.py --config scripts/config.yaml

echo "[STEP 6] Building simulator profile..."
python3 -u scripts/build_simulator_base_profile.py --config scripts/config_simulator_profile.yaml

echo "[DONE] All steps completed successfully."
"""
        ],
        env_from=env_from,
        env_vars={
            "POSTGRES_HOST": "postgres",
            "MINIO_ENDPOINT": "http://minio:9000",
            "PYICEBERG_CATALOG__DEFAULT__TYPE": "sql",
            "PYICEBERG_CATALOG__DEFAULT__S3__ENDPOINT": "http://minio:9000",
            "PYICEBERG_CATALOG__DEFAULT__WAREHOUSE": "s3://warehouse/",
            "TMPDIR": "/data/tmp",
            "TMP": "/data/tmp",
            "TEMP": "/data/tmp",
        },
        volumes=volumes,
        volume_mounts=volume_mounts,
        init_containers=init_containers,
        node_selector={
            "kubernetes.io/hostname": "node-mlflow-proj25"
        },
        get_logs=True,
        log_events_on_failure=True,
        is_delete_operator_pod=False,
        startup_timeout_seconds=600,
    )