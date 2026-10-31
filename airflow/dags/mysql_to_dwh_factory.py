"""
DAG factory per tabel: satu DAG untuk setiap tabel yang dideklarasikan di
``config/mysql_to_dwh_tables.yaml``.

Setiap tabel aktif (enabled) menjadi DAG mandiri (id default
``mysql_to_dwh__<table>``) dengan schedule, riwayat run, dan retry sendiri:

    start -> extract -> transform -> load_raw -> dq_staging -> load_dwh -> dq_dwh -> end

* source: tabel MySQL (connection/database dari YAML)
* file staging: ``s3://<bucket>/<prefix>/<table>/dt=<ds>/data.csv``
* warehouse: ``<schema>.<table_prefix><table>`` (full refresh, aman di-rerun)

Menambah tabel cukup mengedit YAML saja -- DAG dibuat otomatis pada parse
berikutnya, tanpa perubahan Python. Implementasi task berada di
``utils/mysql_to_dwh_tasks.py`` agar tetap mudah diuji dan bebas dari DAG.

Path konfigurasi dapat diganti lewat
``MYSQL_TO_DWH_TABLES_CONFIG=/path/ke/file.yaml``.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from utils.mysql_to_dwh_tasks import (
    dq_dwh,
    dq_staging,
    extract,
    load_config,
    load_dwh,
    load_raw,
    transform,
)

CONFIG_PATH = Path(
    os.environ.get(
        "MYSQL_TO_DWH_TABLES_CONFIG",
        Path(__file__).resolve().parent / "config" / "mysql_to_dwh_tables.yaml",
    )
)

DAG_DEFAULTS, TABLES = load_config(CONFIG_PATH)

DAG_DOC = """
### Satu tabel, satu DAG: MySQL → SeaweedFS → PostgreSQL

Dihasilkan dari `config/mysql_to_dwh_tables.yaml` untuk tabel **`{table}`**.

`extract` → `transform` → `load_raw` → `dq_staging` → `load_dwh` → `dq_dwh`

* source: `{database}.{table}` (MySQL)
* primary key: `{primary_key}`
* file staging: `s3://{bucket}/{prefix}/{table}/dt=<ds>/data.csv`
* warehouse: `{schema}.{target_table}` (full refresh, aman di-rerun)
* toleransi row loss: {max_row_loss_pct}% | freshness: {freshness_minutes} menit

Menambah atau mengubah tabel cukup melalui YAML; DAG ini dibuat otomatis
pada parse berikutnya.
"""


def _as_datetime(value: object) -> datetime:
    """Menerima objek datetime/date atau string ISO-8601 dari YAML."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    return datetime.fromisoformat(str(value))


def _as_tags(value: object) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    return [str(tag) for tag in value]


def _build_dag(cfg: dict) -> None:
    settings = cfg["dag"]
    table = cfg["name"]
    target = cfg["target"]
    storage = cfg["storage"]

    tags = _as_tags(settings.get("tags"))
    if table not in tags:
        tags.append(table)

    with DAG(
        dag_id=settings["dag_id"],
        description=f"MySQL {table} -> SeaweedFS -> PostgreSQL {target['schema']}",
        start_date=_as_datetime(settings["start_date"]),
        schedule=settings.get("schedule"),
        catchup=False,
        max_active_runs=int(settings.get("max_active_runs", 1)),
        max_active_tasks=int(settings.get("max_active_tasks", 4)),
        tags=tags,
        default_args={
            "owner": settings.get("owner", "data-eng"),
            "retries": int(settings.get("retries", 1)),
            "retry_delay": timedelta(
                minutes=float(settings.get("retry_delay_minutes", 2))
            ),
        },
        doc_md=DAG_DOC.format(
            table=table,
            database=cfg["source"]["database"],
            primary_key=cfg["primary_key"],
            bucket=storage["bucket"],
            prefix=storage["prefix"],
            schema=target["schema"],
            target_table=cfg["target_table"],
            max_row_loss_pct=cfg["dq"]["max_row_loss_pct"],
            freshness_minutes=cfg["dq"]["freshness_minutes"],
        ),
    ):
        start = EmptyOperator(task_id="start")

        extracted = extract(cfg)
        cleaned = transform(extracted, cfg)
        uploaded = load_raw(cleaned, cfg)
        staging_ok = dq_staging(uploaded, cfg)
        loaded = load_dwh(staging_ok, cfg)
        dwh_ok = dq_dwh(loaded, cfg)

        end = EmptyOperator(task_id="end")

        start >> extracted
        dwh_ok >> end


for _table_cfg in TABLES:
    _build_dag(_table_cfg)
