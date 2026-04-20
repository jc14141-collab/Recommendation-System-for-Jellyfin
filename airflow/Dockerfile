FROM apache/airflow:3.1.0

USER root

COPY airflow /opt/project/airflow
COPY airflow/requirements.txt /requirements.txt
COPY airflow/constraints/constraints-3.12.txt /constraints-3.12.txt

RUN chown -R airflow:0 /opt/project
RUN mkdir -p /opt/airflow/dags \
    && cp -r /opt/project/airflow/dags/* /opt/airflow/dags/ || true \
    && chown -R airflow:0 /opt/airflow/dags

USER airflow

RUN pip install --no-cache-dir \
    --constraint /constraints-3.12.txt \
    -r /requirements.txt