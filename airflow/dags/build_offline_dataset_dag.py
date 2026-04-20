from datetime import datetime

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


with DAG(
    dag_id="offline_pipeline_job_v1",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["k8s", "pipeline", "offline"],
) as dag:

    volumes = [
        k8s.V1Volume(
            name="artifacts",
            host_path=k8s.V1HostPathVolumeSource(
                path="/mnt/object/minio_data/artifacts",
                type="DirectoryOrCreate",
            ),
        ),
        k8s.V1Volume(
            name="cache",
            host_path=k8s.V1HostPathVolumeSource(
                path="/mnt/block/cache",
                type="DirectoryOrCreate",
            ),
        ),
        k8s.V1Volume(
            name="warehouse",
            host_path=k8s.V1HostPathVolumeSource(
                path="/mnt/object/minio_data/warehouse",
                type="DirectoryOrCreate",
            ),
        ),
        k8s.V1Volume(
            name="data-root",
            host_path=k8s.V1HostPathVolumeSource(
                path="/mnt/object/minio_data",
                type="DirectoryOrCreate",
            ),
        ),
        k8s.V1Volume(
            name="data-tmp",
            host_path=k8s.V1HostPathVolumeSource(
                path="/mnt/object/tmp",
                type="DirectoryOrCreate",
            ),
        ),
    ]

    volume_mounts = [
        k8s.V1VolumeMount(name="artifacts", mount_path="/data/artifacts"),
        k8s.V1VolumeMount(name="cache", mount_path="/data/cache"),
        k8s.V1VolumeMount(name="warehouse", mount_path="/data/warehouse"),
        k8s.V1VolumeMount(name="data-root", mount_path="/data"),
        k8s.V1VolumeMount(name="data-tmp", mount_path="/data/tmp"),
    ]

    init_containers = [
        k8s.V1Container(
            name="wait-for-postgres",
            image="busybox:1.36",
            command=[
                "sh",
                "-c",
                'until nc -z postgres 5432; do echo "waiting for postgres:5432"; sleep 2; done',
            ],
        ),
        k8s.V1Container(
            name="wait-for-minio",
            image="busybox:1.36",
            command=[
                "sh",
                "-c",
                'until nc -z minio 9000; do echo "waiting for minio:9000"; sleep 2; done',
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

    run_offline_pipeline = KubernetesPodOperator(
        task_id="run_offline_pipeline",
        name="run-offline-pipeline",
        namespace="mlops",
        in_cluster=True,
        service_account_name="airflow-job-trigger",
        image="songchenxue/pipeline:v1.0",
        image_pull_policy="IfNotPresent",
        cmds=["sh", "-c"],
        arguments=[
            """
set -euo pipefail

cd /app

echo "[STEP] Build Offline Samples"
python scripts/build_offline_samples.py --config scripts/config_offline_build.yaml

echo "[STEP] Split Dataset"
python scripts/split_dataset_new.py --config scripts/config_offline_split.yaml

echo "[DONE] finished"
"""
        ],
        env_vars={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "MINIO_ENDPOINT": "http://minio:9000",
            "ARTIFACTS_PATH": "/data/artifacts",
            "TMPDIR": "/data/tmp",
            "TMP": "/data/tmp",
            "TEMP": "/data/tmp",
        },
        env_from=env_from,
        volumes=volumes,
        volume_mounts=volume_mounts,
        init_containers=init_containers,
        get_logs=True,
        log_events_on_failure=True,
        is_delete_operator_pod=False,
        startup_timeout_seconds=300,
    )