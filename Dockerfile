FROM apache/airflow:3.2.1

# Library Python yang dipakai DAG pipeline (diinstal ke environment Airflow).
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# dbt diisolasi di virtualenv sendiri: dbt-core mengunci versi beberapa package
# (pyyaml, protobuf, click, ...) yang jika tidak, akan bertabrakan dengan
# pin versi milik Airflow.
USER root
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-postgres>=1.8.0" \
    && chown -R airflow:0 /opt/dbt-venv
USER airflow
