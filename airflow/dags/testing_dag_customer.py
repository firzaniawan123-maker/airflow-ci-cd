import io
import os
from datetime import datetime

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task


# ============================================================
# KONFIGURASI
# Kredensial sudah disimpan di Airflow Connections, di sini
# cukup memakai connection ID-nya.
# ============================================================
MYSQL_CONN_ID = "my_mysql_conn"
SEAWEED_CONN_ID = "seaweed_s3"
POSTGRES_CONN_ID = "my_postgres_conn"   # sesuaikan dengan Conn Id Postgres di Airflow UI

TABLE_NAME = "customers"
DATABASE_NAME = "fintech_payment_oltp"
BUCKET_RAW = "raw"
LOCAL_DIR = "/tmp/airflow_data"

DWH_SCHEMA = "staging"                  # schema tujuan di Postgres
DWH_TABLE = f"stg_{TABLE_NAME}"         # nama tabel tujuan di Postgres
PK_COLUMN = "customer_id"               # GANTI sesuai primary key tabel kamu

# Threshold DQ
MAX_ROW_LOSS_PCT = 1.0                  # toleransi selisih row source vs staging (selain duplikat), dalam %


def _evaluate_checks(stage: str, checks: list[tuple[str, bool, str]]) -> None:
    """Print hasil semua check, lalu fail task kalau ada yang gagal."""
    failed = []
    for name, passed, detail in checks:
        status = "PASS" if passed else "FAIL"
        print(f"[{stage}] [{status}] {name} -> {detail}")
        if not passed:
            failed.append(name)
    if failed:
        raise AirflowException(f"DQ {stage} gagal: {', '.join(failed)}")
    print(f"[{stage}] Semua check lolos")


@dag(
    dag_id="mysql_to_dwh",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl"],
)
def mysql_to_dwh():

    # ---------- 1. START ----------
    start = EmptyOperator(task_id="start")

    # ---------- 2. EXTRACT ----------
    @task
    def extract() -> dict:
        """Ambil data dari MySQL dan simpan ke CSV."""
        os.makedirs(LOCAL_DIR, exist_ok=True)

        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID, schema=DATABASE_NAME)
        df = hook.get_df(f"SELECT * FROM {TABLE_NAME}", df_type="pandas")
        print(f"Extract selesai: {len(df)} baris, {len(df.columns)} kolom")

        path = f"{LOCAL_DIR}/{TABLE_NAME}_extracted.csv"
        df.to_csv(path, index=False)
        return {"path": path, "rows": len(df), "columns": list(df.columns)}

    # ---------- 3. TRANSFORM ----------
    @task
    def transform(extract_meta: dict) -> dict:
        """Bersihkan data kotor, simpan hasilnya, lalu kembalikan metadata."""
        df = pd.read_csv(extract_meta["path"])
        rows_before = len(df)
        print(f"Sebelum cleaning: {rows_before} baris")

        # a. buang duplikat
        df = df.drop_duplicates()

        # b. trim spasi + c. teks null -> "unknown"
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.strip().fillna("unknown")

        # d. null angka -> 0
        for col in df.select_dtypes(include="number").columns:
            df[col] = df[col].fillna(0)

        print(f"Sesudah cleaning: {len(df)} baris")

        output_path = f"{LOCAL_DIR}/{TABLE_NAME}_cleaned.csv"
        df.to_csv(output_path, index=False)
        return {
            "path": output_path,
            "rows_before": rows_before,
            "rows_after": len(df),
            "columns": list(df.columns),
        }

    # ---------- 4. LOAD KE SEAWEEDFS ----------
    @task
    def load_to_raw(transform_meta: dict) -> dict:
        """Upload file ke bucket SeaweedFS (via S3 API)."""
        s3 = S3Hook(aws_conn_id=SEAWEED_CONN_ID)

        if not s3.check_for_bucket(BUCKET_RAW):
            s3.create_bucket(bucket_name=BUCKET_RAW)

        object_key = f"{TABLE_NAME}/{os.path.basename(transform_meta['path'])}"
        s3.load_file(
            filename=transform_meta["path"],
            key=object_key,
            bucket_name=BUCKET_RAW,
            replace=True,
        )
        print(f"Upload sukses: s3://{BUCKET_RAW}/{object_key}")
        return {"object_key": object_key, **transform_meta}

    # ---------- 5. DQ CHECK STAGING (SeaweedFS vs source) ----------
    @task
    def dq_check_staging(raw_meta: dict) -> dict:
        """
        Validasi file di SeaweedFS sebelum masuk DWH:
        1. File ada & tidak kosong
        2. Row count staging == row count hasil transform (tidak ada loss saat upload)
        3. Row count source (live) vs staging, selisih harus <= duplikat yang dibuang + toleransi
        4. Schema/kolom staging sama dengan source
        5. Primary key tidak null & unik
        """
        s3 = S3Hook(aws_conn_id=SEAWEED_CONN_ID)
        key = raw_meta["object_key"]

        exists = s3.check_for_key(key, bucket_name=BUCKET_RAW)
        checks = [("file_exists", exists, f"s3://{BUCKET_RAW}/{key}")]
        if not exists:
            _evaluate_checks("staging", checks)  # langsung raise

        # baca kembali file dari SeaweedFS
        csv_text = s3.read_key(key, bucket_name=BUCKET_RAW)
        staging_df = pd.read_csv(io.StringIO(csv_text))
        staging_rows = len(staging_df)

        # row count langsung dari source (live)
        mysql = MySqlHook(mysql_conn_id=MYSQL_CONN_ID, schema=DATABASE_NAME)
        source_rows = int(mysql.get_first(f"SELECT COUNT(*) FROM {TABLE_NAME}")[0])

        dupes_removed = raw_meta["rows_before"] - raw_meta["rows_after"]
        gap = source_rows - staging_rows
        allowed_gap = dupes_removed + (source_rows * MAX_ROW_LOSS_PCT / 100)

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
                f"duplikat_dibuang={dupes_removed}, max_gap={allowed_gap:.0f}",
            ),
            (
                "schema_match",
                list(staging_df.columns) == raw_meta["columns"],
                f"staging_cols={list(staging_df.columns)}",
            ),
        ]

        if PK_COLUMN in staging_df.columns:
            pk_nulls = int(staging_df[PK_COLUMN].isna().sum())
            pk_dupes = int(staging_df[PK_COLUMN].duplicated().sum())
            checks += [
                ("pk_not_null", pk_nulls == 0, f"null_pk={pk_nulls}"),
                ("pk_unique", pk_dupes == 0, f"duplicate_pk={pk_dupes}"),
            ]
        else:
            checks.append(("pk_column_exists", False, f"kolom {PK_COLUMN} tidak ada"))

        _evaluate_checks("staging", checks)
        return {"object_key": key, "staging_rows": staging_rows}

    # ---------- 6. LOAD KE DWH (POSTGRES) ----------
    @task
    def load_to_dwh(staging_meta: dict) -> dict:
        """Baca dari SeaweedFS, load ke Postgres (full refresh, idempotent)."""
        s3 = S3Hook(aws_conn_id=SEAWEED_CONN_ID)
        csv_text = s3.read_key(staging_meta["object_key"], bucket_name=BUCKET_RAW)
        df = pd.read_csv(io.StringIO(csv_text))
        df["loaded_at"] = pd.Timestamp.now()

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg.run(f"CREATE SCHEMA IF NOT EXISTS {DWH_SCHEMA}")

        engine = pg.get_sqlalchemy_engine()
        df.to_sql(
            name=DWH_TABLE,
            con=engine,
            schema=DWH_SCHEMA,
            if_exists="replace",   # full refresh -> aman di-rerun
            index=False,
            chunksize=5000,
            method="multi",
        )
        print(f"Load ke DWH sukses: {DWH_SCHEMA}.{DWH_TABLE} ({len(df)} baris)")
        return {"loaded_rows": len(df), "staging_rows": staging_meta["staging_rows"]}

    # ---------- 7. DQ CHECK DWH ----------
    @task
    def dq_check(load_meta: dict) -> None:
        """
        Validasi data yang sudah ada di DWH:
        1. Row count DWH == row count staging
        2. PK tidak null & unik
        3. Data fresh (loaded_at baru saja)
        """
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        fq_table = f"{DWH_SCHEMA}.{DWH_TABLE}"

        dwh_rows = int(pg.get_first(f"SELECT COUNT(*) FROM {fq_table}")[0])
        pk_nulls = int(pg.get_first(f"SELECT COUNT(*) FROM {fq_table} WHERE {PK_COLUMN} IS NULL")[0])
        pk_dupes = int(
            pg.get_first(
                f"SELECT COUNT(*) FROM (SELECT {PK_COLUMN} FROM {fq_table} "
                f"GROUP BY {PK_COLUMN} HAVING COUNT(*) > 1) d"
            )[0]
        )
        age_minutes = float(
            pg.get_first(
                f"SELECT EXTRACT(EPOCH FROM (NOW() - MAX(loaded_at))) / 60 FROM {fq_table}"
            )[0]
            or 1e9
        )

        checks = [
            (
                "rowcount_dwh_vs_staging",
                dwh_rows == load_meta["staging_rows"],
                f"dwh={dwh_rows}, staging={load_meta['staging_rows']}",
            ),
            ("pk_not_null", pk_nulls == 0, f"null_pk={pk_nulls}"),
            ("pk_unique", pk_dupes == 0, f"duplicate_pk={pk_dupes}"),
            ("freshness", age_minutes <= 60, f"loaded {age_minutes:.1f} menit lalu"),
        ]
        _evaluate_checks("dwh", checks)

    # ---------- 8. END ----------
    end = EmptyOperator(task_id="end")

    # ---------- URUTAN TASK ----------
    extracted = extract()
    cleaned = transform(extracted)
    loaded_raw = load_to_raw(cleaned)
    staging_ok = dq_check_staging(loaded_raw)
    loaded_dwh = load_to_dwh(staging_ok)
    dwh_ok = dq_check(loaded_dwh)

    start >> extracted
    dwh_ok >> end


mysql_to_dwh()