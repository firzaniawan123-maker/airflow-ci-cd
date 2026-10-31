from datetime import datetime
import os
import boto3

from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook

LOCAL_DIR = "/tmp/seaweed_test"
os.makedirs(LOCAL_DIR, exist_ok=True)

@dag(
    dag_id="test_seaweed_only",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # hanya berjalan manual via trigger
    catchup=False,
    tags=["test", "seaweedfs", "s3"],
)
def test_seaweed_only():

    start = EmptyOperator(task_id="start")

    @task
    def test_seaweed_operations():
        """Uji lengkap: koneksi, buat bucket, upload, dan list object di SeaweedFS."""

        # 1. Ambil koneksi 'seaweed_s3' yang dibuat manual di Airflow UI
        s3_conn = BaseHook.get_connection("seaweed_s3")
        extra_config = s3_conn.extra_dejson
        endpoint_url = extra_config.get("endpoint_url", "http://seaweedfs:8333")

        print(f"Menghubungkan ke SeaweedFS S3 di endpoint: {endpoint_url}")

        # 2. Inisialisasi S3 client dengan boto3
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=s3_conn.login,
            aws_secret_access_key=s3_conn.password,
            endpoint_url=endpoint_url,
        )

        bucket_name = "raw"

        # 3. Cek / buat bucket
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' sudah ada.")
        except Exception:
            print(f"Bucket '{bucket_name}' belum ada, membuat bucket baru...")
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' berhasil dibuat!")

        # 4. Buat file dummy lokal
        file_name = "hello_seaweed.txt"
        local_path = os.path.join(LOCAL_DIR, file_name)
        with open(local_path, "w") as f:
            f.write("Halo SeaweedFS, ini test koneksi dari Airflow DAG!")

        # 5. Upload file ke SeaweedFS
        print(f"Mengupload {file_name} ke bucket '{bucket_name}'...")
        s3_client.upload_file(local_path, bucket_name, file_name)
        print("Upload berhasil!")

        # 6. List isi bucket untuk verifikasi file sudah masuk
        print(f"Mengecek isi bucket '{bucket_name}':")
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if "Contents" in response:
            for obj in response["Contents"]:
                print(f" - File ditemukan di S3: {obj['Key']} (Ukuran: {obj['Size']} bytes)")
        else:
            print("Bucket kosong.")

        print("TEST SEAWEEDFS SELESAI DENGAN SUKSES!")

    start >> test_seaweed_operations()

test_seaweed_only()