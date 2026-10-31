"""
Pipeline MySQL -> SeaweedFS -> PostgreSQL (staging) berbasis konfigurasi.

Ini adalah versi scalable dari ``testing_dag_customer.py``: logikanya sama
(extract -> clean -> upload CSV -> DQ -> load ke warehouse -> DQ), tetapi
daftar tabel dideklarasikan di ``config/elt_pipeline.yaml`` alih-alih
hard-coded di kode Python.

Untuk setiap tabel yang aktif (enabled), DAG membuat TaskGroup tersendiri:

    <table>.extract          SELECT * FROM <table>            -> CSV lokal
    <table>.transform        de-duplicate / trim / fill nulls -> CSV bersih
    <table>.load_raw         upload CSV bersih ke SeaweedFS
    <table>.dq_staging       cek file, row count, schema, PK
    <table>.load_dwh         full refresh ke <schema>.<prefix><table>
    <table>.dq_dwh           cek row count, PK, dan freshness

Semua tabel berjalan paralel (dibatasi ``max_active_tasks``), sehingga
menambah tabel cukup mengubah YAML -- tanpa edit Python, tanpa DAG baru.

Connection juga diambil dari YAML. Secara default DAG memakai tiga connection
yang dibuat otomatis docker-compose dari variabel ``AIRFLOW_CONN_*``
(``mysql_source`` / ``s3_raw`` / ``warehouse_pg``). Jika Airflow Anda masih
memakai connection buatan manual dari ``testing_dag_customer.py``, arahkan
YAML ke ``my_mysql_conn`` / ``seaweed_s3`` / ``my_postgres_conn``.

Yang dibutuhkan hanya pandas, PyMySQL, psycopg2, dan boto3 (sudah ada di
requirements.txt) -- tidak ada import package provider hook.
"""

from __future__ import annotations

import copy
import io
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup, dag, get_current_context, task
from airflow.sdk.bases.hook import BaseHook

# --------------------------------------------------------------------------- #
# konfigurasi
# --------------------------------------------------------------------------- #
DAG_ID = "mysql_to_dwh_scalable"

CONFIG_PATH = Path(
    os.environ.get(
        "MYSQL_TO_DWH_CONFIG",
        Path(__file__).resolve().parent / "config" / "elt_pipeline.yaml",
    )
)

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

DEFAULT_DQ = {"max_row_loss_pct": 1.0, "freshness_minutes": 60.0}
DEFAULT_TRANSFORM = {
    "drop_duplicates": True,
    "trim_strings": True,
    "null_text": "unknown",
    "null_number": 0,
}


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(f"[{CONFIG_PATH}] {message}")


def _deep_merge(base: dict, override: dict) -> dict:
    """Gabungkan ``override`` secara rekursif ke salinan ``base``."""
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _validate_identifier(value: object, field: str) -> str:
    _require(
        isinstance(value, str) and bool(IDENTIFIER_RE.match(value)),
        f"{field} must be a valid SQL identifier, got {value!r}",
    )
    return value  # type: ignore[return-value]


def _load_tables(path: Path) -> list[dict]:
    """Baca YAML, merge dengan defaults, validasi, lalu kembalikan konfigurasi per tabel."""
    _require(path.is_file(), f"config file not found: {path}")

    with path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    defaults = raw.get("defaults") or {}
    _require(isinstance(defaults, dict), "'defaults' must be a mapping")

    tables: list[dict] = []
    seen: set[str] = set()

    for index, item in enumerate(raw.get("tables") or []):
        _require(isinstance(item, dict), f"tables[{index}] must be a mapping")

        cfg = _deep_merge(defaults, item)
        name = cfg.get("name")
        _validate_identifier(name, f"tables[{index}].name")
        _require(name not in seen, f"duplicate table: {name!r}")
        seen.add(name)

        if not cfg.get("enabled", True):
            print(f"[config] skipping disabled table: {name}")
            continue

        _validate_identifier(cfg.get("primary_key"), f"tables[{name}].primary_key")

        for section, keys in (
            ("source", ("conn_id", "database")),
            ("storage", ("conn_id", "bucket")),
            ("target", ("conn_id", "schema")),
        ):
            _require(isinstance(cfg.get(section), dict), f"tables[{name}].{section} must be a mapping")
            for key in keys:
                _require(cfg[section].get(key), f"tables[{name}].{section}.{key} is required")

        _validate_identifier(cfg["source"]["database"], f"tables[{name}].source.database")
        _validate_identifier(cfg["target"]["schema"], f"tables[{name}].target.schema")

        # nama tabel tujuan default: <table_prefix><source table>
        cfg["target_table"] = _validate_identifier(
            cfg.get("target_table")
            or f"{cfg['target'].get('table_prefix', 'stg_')}{name}",
            f"tables[{name}].target_table",
        )

        cfg["storage"]["prefix"] = str(cfg["storage"].get("prefix") or "raw").strip("/")
        cfg["target"].setdefault("table_prefix", "stg_")
        cfg["local_dir"] = str(cfg.get("local_dir") or "/tmp/airflow_data")

        # daftar kolom opsional (whitelist)
        columns = cfg.get("columns")
        if columns:
            _require(isinstance(columns, list), f"tables[{name}].columns must be a list")
            cfg["columns"] = [
                _validate_identifier(col, f"tables[{name}].columns[]") for col in columns
            ]

        cfg["transform"] = _deep_merge(DEFAULT_TRANSFORM, cfg.get("transform") or {})
        cfg["dq"] = _deep_merge(DEFAULT_DQ, cfg.get("dq") or {})
        cfg["dq"]["max_row_loss_pct"] = float(cfg["dq"]["max_row_loss_pct"])
        cfg["dq"]["freshness_minutes"] = float(cfg["dq"]["freshness_minutes"])

        tables.append(cfg)

    _require(tables, "no enabled tables found under 'tables'")
    return tables


TABLES = _load_tables(CONFIG_PATH)

# --------------------------------------------------------------------------- #
# helper
# --------------------------------------------------------------------------- #
def _mysql_engine(source: dict):
    """SQLAlchemy engine untuk sumber data MySQL (driver PyMySQL)."""
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    conn = BaseHook.get_connection(source["conn_id"])
    url = URL.create(
        "mysql+pymysql",
        username=conn.login,
        password=conn.password,
        host=conn.host,
        port=int(conn.port or 3306),
        database=source.get("database") or conn.schema,
    )
    return create_engine(url, pool_pre_ping=True)


def _postgres_engine(target: dict):
    """SQLAlchemy engine untuk warehouse PostgreSQL."""
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    conn = BaseHook.get_connection(target["conn_id"])
    url = URL.create(
        "postgresql+psycopg2",
        username=conn.login,
        password=conn.password,
        host=conn.host,
        port=int(conn.port or 5432),
        database=target.get("database") or conn.schema,
    )
    return create_engine(url, pool_pre_ping=True)


def _s3_client(storage: dict):
    """boto3 client untuk S3 gateway SeaweedFS (kompatibel juga dengan MinIO/AWS)."""
    import boto3
    from botocore.config import Config

    conn = BaseHook.get_connection(storage["conn_id"])
    endpoint_url = conn.extra_dejson.get("endpoint_url") or (
        f"http://{conn.host}:{int(conn.port or 8333)}"
    )
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=10,
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def _ensure_bucket(s3, bucket: str) -> None:
    from botocore.exceptions import ClientError

    try:
        s3.head_bucket(Bucket=bucket)
        return
    except ClientError:
        pass

    try:
        s3.create_bucket(Bucket=bucket)
        print(f"[s3] created missing bucket '{bucket}'")
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise


def _read_csv(source, columns: list[str] | None = None) -> pd.DataFrame:
    """Baca CSV, toleran jika file hanya berisi baris header."""
    try:
        return pd.read_csv(source)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=list(columns or []))


def _evaluate_checks(stage: str, checks: list[tuple[str, bool, str]]) -> None:
    """Cetak semua hasil check, lalu gagalkan task jika ada yang gagal."""
    from airflow.sdk.exceptions import AirflowException

    failed = []
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        print(f"[{stage}] [{status}] {name} -> {detail}")
        if not passed:
            failed.append(name)
    if failed:
        raise AirflowException(f"DQ {stage} gagal: {', '.join(failed)}")
    print(f"[{stage}] Semua check lolos")


# --------------------------------------------------------------------------- #
# task
# --------------------------------------------------------------------------- #
@task
def extract(cfg: dict) -> dict:
    """Baca tabel sumber ke CSV lokal dan kembalikan metadata-nya."""
    context = get_current_context()
    ds = str(context["ds"])

    columns = cfg.get("columns")
    projection = ", ".join(f"`{col}`" for col in columns) if columns else "*"
    table = cfg["name"]

    engine = _mysql_engine(cfg["source"])
    try:
        df = pd.read_sql(f"SELECT {projection} FROM `{table}`", engine)
    finally:
        engine.dispose()

    path = Path(cfg["local_dir"]) / table / f"dt={ds}" / "extracted.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

    print(f"[{table}] extract: {len(df)} baris, {len(df.columns)} kolom -> {path}")
    return {
        "table": table,
        "ds": ds,
        "path": str(path),
        "rows": len(df),
        "columns": list(df.columns),
    }


@task
def transform(extract_meta: dict, cfg: dict) -> dict:
    """Bersihkan CSV hasil extract dan simpan untuk tahap upload."""
    table = cfg["name"]
    pk = cfg["primary_key"]
    rules = cfg["transform"]

    df = _read_csv(extract_meta["path"], extract_meta["columns"])
    rows_before = len(df)
    print(f"[{table}] transform: sebelum cleaning {rows_before} baris")

    # Primary key yang null tetap dibiarkan null agar DQ dapat menolak batch
    # sebelum ditulis ke warehouse. Kolom lain diisi null-nya seperti DAG lama.
    pk_nulls_before = int(df[pk].isna().sum()) if pk in df.columns else 0

    if rules.get("drop_duplicates", True):
        df = df.drop_duplicates()

    text_columns = list(df.select_dtypes(include="object").columns)
    number_columns = list(df.select_dtypes(include="number").columns)

    if rules.get("trim_strings", True):
        for col in text_columns:
            if col == pk:
                continue
            df[col] = df[col].map(lambda value: value.strip() if isinstance(value, str) else value)

    null_text = rules.get("null_text")
    if null_text is not None:
        for col in text_columns:
            if col == pk:
                continue
            df[col] = df[col].fillna(null_text)

    null_number = rules.get("null_number")
    if null_number is not None:
        for col in number_columns:
            if col == pk:
                continue
            df[col] = df[col].fillna(null_number)

    rows_after = len(df)
    print(f"[{table}] transform: sesudah cleaning {rows_after} baris")

    path = Path(extract_meta["path"]).with_name("cleaned.csv")
    df.to_csv(path, index=False)

    return {
        **extract_meta,
        "path": str(path),
        "rows_before": rows_before,
        "rows_after": rows_after,
        "columns": list(df.columns),
        "primary_key": pk,
        "pk_nulls_before": pk_nulls_before,
        "dupes_removed": rows_before - rows_after,
    }


@task
def load_raw(transform_meta: dict, cfg: dict) -> dict:
    """Upload CSV bersih ke SeaweedFS."""
    storage = cfg["storage"]
    table = cfg["name"]
    bucket = storage["bucket"]
    key = f"{storage['prefix']}/{table}/dt={transform_meta['ds']}/data.csv"

    s3 = _s3_client(storage)
    _ensure_bucket(s3, bucket)
    s3.upload_file(
        Filename=transform_meta["path"],
        Bucket=bucket,
        Key=key,
        ExtraArgs={"ContentType": "text/csv"},
    )

    print(f"[{table}] upload: {transform_meta['path']} -> s3://{bucket}/{key}")
    return {**transform_meta, "bucket": bucket, "object_key": key}


@task
def dq_staging(raw_meta: dict, cfg: dict) -> dict:
    """Validasi object di SeaweedFS sebelum dipromosikan ke warehouse."""
    from botocore.exceptions import ClientError

    table = cfg["name"]
    pk = cfg["primary_key"]
    storage = cfg["storage"]
    bucket = raw_meta["bucket"]
    key = raw_meta["object_key"]

    s3 = _s3_client(storage)
    try:
        s3.head_object(Bucket=bucket, Key=key)
        exists = True
    except ClientError:
        exists = False

    checks: list[tuple[str, bool, str]] = [
        ("file_exists", exists, f"s3://{bucket}/{key}"),
    ]
    if not exists:
        _evaluate_checks(f"{table}:staging", checks)

    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    staging_df = _read_csv(io.BytesIO(body), raw_meta["columns"])
    staging_rows = len(staging_df)

    engine = _mysql_engine(cfg["source"])
    try:
        source_rows = int(pd.read_sql(f"SELECT COUNT(*) AS n FROM `{table}`", engine)["n"][0])
    finally:
        engine.dispose()

    gap = source_rows - staging_rows
    allowed_gap = raw_meta["dupes_removed"] + (
        source_rows * cfg["dq"]["max_row_loss_pct"] / 100
    )

    checks += [
        ("not_empty", staging_rows > 0, f"staging_rows={staging_rows}"),
        (
            "rowcount_staging_vs_transform",
            staging_rows == raw_meta["rows_after"],
            f"staging={staging_rows}, transform={raw_meta['rows_after']}",
        ),
        (
            "rowcount_source_vs_staging",
            0 <= gap <= allowed_gap,
            f"source={source_rows}, staging={staging_rows}, gap={gap}, "
            f"duplikat_dibuang={raw_meta['dupes_removed']}, max_gap={allowed_gap:.0f}",
        ),
        (
            "schema_match",
            list(staging_df.columns) == raw_meta["columns"],
            f"staging_cols={list(staging_df.columns)}",
        ),
    ]

    if pk in staging_df.columns:
        checks += [
            ("pk_not_null", int(staging_df[pk].isna().sum()) == 0,
             f"null_pk={int(staging_df[pk].isna().sum())}"),
            ("pk_unique", int(staging_df[pk].duplicated().sum()) == 0,
             f"duplicate_pk={int(staging_df[pk].duplicated().sum())}"),
        ]
    else:
        checks.append(("pk_column_exists", False, f"kolom {pk} tidak ada"))

    _evaluate_checks(f"{table}:staging", checks)
    return {**raw_meta, "staging_rows": staging_rows}


@task
def load_dwh(staging_meta: dict, cfg: dict) -> dict:
    """Baca CSV dari SeaweedFS lalu full-refresh ke PostgreSQL."""
    from sqlalchemy import text

    table = cfg["name"]
    target = cfg["target"]
    schema = target["schema"]
    target_table = cfg["target_table"]

    s3 = _s3_client(cfg["storage"])
    body = s3.get_object(
        Bucket=staging_meta["bucket"], Key=staging_meta["object_key"]
    )["Body"].read()
    df = _read_csv(io.BytesIO(body), staging_meta["columns"])
    df["loaded_at"] = pd.Timestamp.now()

    engine = _postgres_engine(target)
    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        df.to_sql(
            name=target_table,
            con=engine,
            schema=schema,
            if_exists="replace",  # full refresh -> aman di-rerun
            index=False,
            chunksize=5000,
            method="multi",
        )
    finally:
        engine.dispose()

    print(f"[{table}] load: {len(df)} baris -> {schema}.{target_table}")
    return {
        **staging_meta,
        "schema": schema,
        "target_table": target_table,
        "loaded_rows": len(df),
    }


@task
def dq_dwh(load_meta: dict, cfg: dict) -> None:
    """Validasi row count, primary key, dan freshness di warehouse."""
    from sqlalchemy import text

    table = cfg["name"]
    pk = cfg["primary_key"]
    schema = load_meta["schema"]
    target_table = load_meta["target_table"]
    fq_table = f'"{schema}"."{target_table}"'

    engine = _postgres_engine(cfg["target"])
    try:
        with engine.connect() as conn:
            dwh_rows = int(conn.execute(text(f"SELECT COUNT(*) FROM {fq_table}")).scalar_one())
            pk_nulls = int(
                conn.execute(
                    text(f"SELECT COUNT(*) FROM {fq_table} WHERE {pk} IS NULL")
                ).scalar_one()
            )
            pk_dupes = int(
                conn.execute(
                    text(
                        f"SELECT COUNT(*) FROM ("
                        f"SELECT {pk} FROM {fq_table} "
                        f"GROUP BY {pk} HAVING COUNT(*) > 1"
                        f") AS duplicates"
                    )
                ).scalar_one()
            )
            age_row = conn.execute(
                text(
                    f"SELECT EXTRACT(EPOCH FROM (NOW() - MAX(loaded_at))) / 60 "
                    f"FROM {fq_table}"
                )
            ).scalar_one()
    finally:
        engine.dispose()

    age_minutes = float(age_row) if age_row is not None else float("inf")

    checks = [
        (
            "rowcount_dwh_vs_staging",
            dwh_rows == load_meta["staging_rows"],
            f"dwh={dwh_rows}, staging={load_meta['staging_rows']}",
        ),
        ("pk_not_null", pk_nulls == 0, f"null_pk={pk_nulls}"),
        ("pk_unique", pk_dupes == 0, f"duplicate_pk={pk_dupes}"),
        (
            "freshness",
            age_minutes <= cfg["dq"]["freshness_minutes"],
            f"loaded {age_minutes:.1f} menit lalu "
            f"(maks {cfg['dq']['freshness_minutes']:.0f})",
        ),
    ]
    _evaluate_checks(f"{table}:dwh", checks)


# --------------------------------------------------------------------------- #
# DAG
# --------------------------------------------------------------------------- #
DAG_DOC = """
### Pipeline MySQL → SeaweedFS → PostgreSQL berbasis konfigurasi

Setiap tabel di `config/elt_pipeline.yaml` mendapat TaskGroup sendiri dengan:

`extract` → `transform` → `load_raw` → `dq_staging` → `load_dwh` → `dq_dwh`

* source: koneksi/database MySQL dari YAML
* file staging: `s3://<bucket>/<prefix>/<table>/dt=<ds>/data.csv`
* warehouse: `<schema>.<table_prefix><table>` (full refresh, aman di-rerun)

Menambah tabel cukup dengan mengedit YAML — DAG mengambilnya pada parse berikutnya.
"""


@dag(
    dag_id=DAG_ID,
    description="Pipeline MySQL -> SeaweedFS -> PostgreSQL berbasis konfigurasi",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=16,
    tags=["etl", "config-driven", "mysql", "seaweedfs", "postgres"],
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    doc_md=DAG_DOC,
)
def mysql_to_dwh_scalable():
    start = EmptyOperator(task_id="start")
    first_tasks = []
    last_tasks = []

    for cfg in TABLES:
        with TaskGroup(group_id=cfg["name"]):
            extracted = extract(cfg)
            cleaned = transform(extracted, cfg)
            uploaded = load_raw(cleaned, cfg)
            staging_ok = dq_staging(uploaded, cfg)
            loaded = load_dwh(staging_ok, cfg)
            dwh_ok = dq_dwh(loaded, cfg)

        first_tasks.append(extracted)
        last_tasks.append(dwh_ok)

    end = EmptyOperator(task_id="end")

    start >> first_tasks
    last_tasks >> end


mysql_to_dwh_scalable()
