# Pipeline Data Engineering (MySQL → SeaweedFS → PostgreSQL)

Repository ini berisi pipeline ingestion data yang berjalan penuh di Docker:
MySQL (source OLTP) → SeaweedFS (object storage, CSV) → PostgreSQL (data warehouse),
yang diorkestrasi oleh Apache Airflow, termasuk pemeriksaan Data Quality (DQ)
di dalam setiap DAG.

> **Status saat ini:** tahap ingestion + Data Quality sudah terimplementasi.
> Data modelling dengan **dbt masih dalam rencana** (belum ada model, konfigurasi,
> maupun DAG dbt). Detail lihat bagian [Rencana pengembangan data modelling (dbt)](#rencana-pengembangan-data-modelling-dbt).

```
┌────────────┐   extract    ┌───────────────┐    load    ┌─────────────────────┐
│   MySQL    │ ───────────► │   SeaweedFS   │ ─────────► │ PostgreSQL          │
│  (source)  │  CSV         │ (object store)│    CSV     │ schema "staging"    │
└────────────┘              └───────────────┘            │ (data warehouse)    │
      ▲                                                  └─────────────────────┘
      │          setiap tahap disertai pemeriksaan Data Quality (DQ)
      │
      └──────────────────────  Airflow (orkestrasi)  ──────────────────────────
```

## Services

| Service | Peran | Port host | Kredensial (dari `.env`) |
|---|---|---|---|
| `mysql` | Sumber data (OLTP) | `3306` | user `elt` / pass `eltpass`, db `fintech_payment_oltp` (root pass `rootpass`) |
| `seaweedfs` | Object storage kompatibel S3 | `8333` (S3 API), `8888` (Filer UI), `9333` (Master UI), `23646` (Admin UI) | `airflowadmin` / `seaweedfs2026`, bucket `raw-zone` |
| `warehouse-postgres` | Data warehouse | `5433` | user `warehouse` / pass `warehouse`, db `warehouse_db` |
| `airflow-*` | Orkestrasi (api-server, scheduler, dag-processor, worker, triggerer) | `8080` | `airflow` / `airflow` |
| `postgres` | Metadata database milik Airflow sendiri | *(internal)* | `airflow` / `airflow` |
| `redis` | Broker Celery Executor | *(internal)* | — |
| `cloudflared` | Tunnel Cloudflare ke Airflow (opsional) | *(internal)* | token di `.env` |

> ⚠️ `postgres` = metadata Airflow, sedangkan `warehouse-postgres` = data warehouse
> tempat hasil ingestion ditulis. Keduanya adalah container PostgreSQL yang berbeda.

## Struktur repository

```
.
├── docker-compose.yaml                  # seluruh service dalam satu file
├── Dockerfile                           # image Airflow + dependensi pipeline; instalasi dbt di venv terpisah
├── requirements.txt                     # dependensi Python yang dibaked ke image Airflow
├── .env                                 # seluruh kredensial (gitignored)
├── initdb/01_schema.sql                 # schema source MySQL, otomatis dijalankan saat start pertama
├── airflow/
│   ├── dags/
│   │   ├── mysql_to_dwh_scalable.py     # DAG satu DAG-multi-tabel (config-driven)
│   │   ├── mysql_to_dwh_factory.py      # factory: satu DAG per tabel
│   │   ├── testing_dag_customer.py      # DAG referensi satu tabel (koneksi manual)
│   │   ├── test_connection_seaweed.py   # DAG uji konektivitas SeaweedFS
│   │   ├── dag_pertama.py               # contoh DAG pembelajaran
│   │   ├── config/
│   │   │   ├── elt_pipeline.yaml        # daftar tabel untuk mysql_to_dwh_scalable
│   │   │   └── mysql_to_dwh_tables.yaml # daftar tabel untuk mysql_to_dwh_factory
│   │   └── utils/mysql_to_dwh_tasks.py  # implementasi task bersama (tanpa DAG)
│   ├── config/airflow.cfg               # konfigurasi Airflow
│   ├── logs/                            # log runtime (gitignored)
│   └── plugins/
├── dbt/                                 # folder kosong, disiapkan untuk pengembangan dbt mendatang
└── .github/workflows/deploy.yml         # deploy otomatis ke server (self-hosted runner)
```

## Schema sumber data

Database sumber didefinisikan di `initdb/01_schema.sql` berupa database
`fintech_payment_oltp` (aplikasi pembayaran/fintech) dengan **11 tabel**:

`customers`, `merchants`, `payment_methods`, `payment_transactions`,
`payment_transaction_events`, `refunds`, `chargebacks`,
`settlement_providers`, `settlement_batches`, `settlement_transactions`,
`settlement_adjustments`.

## Inventaris DAG

### 1. `mysql_to_dwh_scalable` — pipeline multi-tabel (config-driven)

File: `airflow/dags/mysql_to_dwh_scalable.py`, konfigurasi:
`airflow/dags/config/elt_pipeline.yaml`. Trigger manual (`schedule: null`),
maksimal 16 task berjalan paralel.

Untuk setiap tabel aktif di YAML, DAG membuat TaskGroup tersendiri:

```
<table>.extract    ->    <table>.transform    ->    <table>.load_raw
    ->    <table>.dq_staging    ->    <table>.load_dwh    ->    <table>.dq_dwh
```

| Task | Yang dilakukan |
|---|---|
| `extract` | `SELECT *` (atau daftar `columns` dari konfigurasi) dari MySQL ke CSV lokal |
| `transform` | buang duplikat, trim string, isi nilai null (primary key tidak pernah diisi) |
| `load_raw` | upload CSV ke `s3://<bucket>/<prefix>/<table>/dt=<ds>/data.csv` |
| `dq_staging` | file ada & tidak kosong, row count, selisih source vs staging, schema, PK |
| `load_dwh` | full-refresh ke `staging.<table_prefix><table>` (aman di-rerun) |
| `dq_dwh` | row count, PK null/unik, freshness kolom `loaded_at` |

Semua tabel berjalan paralel. **Menambah tabel baru cukup mengubah YAML** —
tambahkan entri beserta `primary_key`-nya, simpan, lalu trigger DAG. Bagian
`defaults` menyimpan koneksi, bucket, aturan pembersihan, dan threshold DQ;
key apa pun dapat dioverride per tabel. Set `enabled: false` untuk melewati
satu tabel secara sementara.

### 2. DAG per tabel (`mysql_to_dwh_factory.py`)

File: `airflow/dags/mysql_to_dwh_factory.py`, konfigurasi:
`airflow/dags/config/mysql_to_dwh_tables.yaml`.

Setiap entri tabel menjadi DAG mandiri dengan id default
`mysql_to_dwh__<table>` (dapat dioverride lewat `dag.dag_id`), sehingga
kegagalan atau rerun satu tabel tidak memengaruhi tabel lainnya. Task-nya
memakai implementasi bersama dari `airflow/dags/utils/mysql_to_dwh_tasks.py`
dengan alur yang sama seperti nomor 1. Path konfigurasi dapat diganti lewat
variabel `MYSQL_TO_DWH_TABLES_CONFIG`.

### 3. `mysql_to_dwh` (`testing_dag_customer.py`) — DAG referensi satu tabel

Implementasi awal untuk satu tabel (`customers`). **Catatan:** DAG ini
memakai connection buatan manual (`my_mysql_conn`, `seaweed_s3`,
`my_postgres_conn`) yang **tidak** dibuat otomatis oleh docker-compose, dan
memerlukan provider package (`apache-airflow-providers-amazon`, dll.).
Dibuat sebagai referensi/logika dasar; pipeline yang dipakai sehari-hari
adalah nomor 1 dan 2.

### 4. `test_seaweed_only` (`test_connection_seaweed.py`)

DAG uji manual: mengambil bucket, upload file dummy, dan list object di
SeaweedFS memakai connection `seaweed_s3` (dibuat manual di Airflow UI).
Berguna untuk memverifikasi konektivitas S3 tanpa menjalankan pipeline penuh.

### 5. `dag_pertama4` (`dag_pertama.py`) — contoh pembelajaran

DAG sederhana dengan satu `BashOperator` untuk latihan data scheduling.
Tidak terhubung ke pipeline data mana pun.

## Pemeriksaan Data Quality (DQ)

DQ dijalankan di dua gerbang sebelum dan sesudah data ditulis ke warehouse:

**`dq_staging` (validasi object di SeaweedFS):**
- `file_exists` — object CSV ada di S3
- `not_empty` — file tidak hanya berisi header / tidak kosong
- `rowcount_staging_vs_transform` — row count saat upload == hasil transform
- `rowcount_source_vs_staging` — selisih baris source vs staging ≤ duplikat
  yang dibuang + toleransi `max_row_loss_pct`
- `schema_match` — kolom staging identik dengan source
- `pk_not_null`, `pk_unique` — primary key tidak null dan unik

**`dq_dwh` (validasi data di PostgreSQL):**
- `rowcount_dwh_vs_staging` — row count warehouse == staging
- `pk_not_null`, `pk_unique`
- `freshness` — usia `loaded_at` maksimal `freshness_minutes` menit

Jika ada check yang gagal, task langsung gagal dan batch **tidak** lanjut
ditulis ke warehouse. Seluruh check dicetak ke task log dengan status `PASS`/`FAIL`.

## Cara menjalankan

Semua perintah dari root project.

### 1. Build image Airflow custom (wajib, pertama kali dan setelah mengubah `requirements.txt`)

```bash
docker compose build
```

### 2. Jalankan seluruh stack

```bash
docker compose up -d
```

### 3. Pastikan service sehat

```bash
docker compose ps
docker compose logs -f seaweedfs
```

`seaweedfs` seharusnya melaporkan S3 gateway aktif di port `8333` dan membuat
bucket `raw-zone` saat start pertama (dari env `S3_BUCKET`). Seluruh service
`airflow-*`, `mysql`, `seaweedfs`, dan `warehouse-postgres` seharusnya berstatus
`healthy`. Jika `seaweedfs` menampilkan `unhealthy`, lihat bagian
[Troubleshooting](#troubleshooting) — hal ini tidak memblokir pipeline.

> Bucket hanya dibuat saat volume `seaweedfs-data` masih baru. DAG juga membuat
> bucket on-demand, sehingga volume yang terhapus tidak merusak run berikutnya.

### 4. Isi data sumber ke MySQL

Schema di `initdb/01_schema.sql` hanya membuat tabel kosong. Contoh mengisi data:

```bash
docker compose exec mysql mysql -uelt -peltpass fintech_payment_oltp \
  -e "INSERT INTO customers (customer_id, first_name, last_name, email, country) VALUES ('CUST001','Budi','Santoso','budi@example.com','ID'),('CUST002','Siti','Ayu','siti@example.com','ID');"
```

Cek isi tabel:

```bash
docker compose exec mysql mysql -uelt -peltpass fintech_payment_oltp -e "SELECT COUNT(*) FROM customers;"
```

### 5. Buka Airflow dan trigger DAG

- URL: <http://localhost:8080>
- Login: `airflow` / `airflow`

Unpause **`mysql_to_dwh_scalable`** lalu trigger DAG tersebut (▶ / Trigger DAG).
DAG dibuat dalam keadaan paused secara default (`DAGS_ARE_PAUSED_AT_CREATION=true`).

Atau trigger dari CLI:

```bash
docker compose exec airflow-scheduler airflow dags trigger mysql_to_dwh_scalable
```

Untuk mode satu DAG per tabel, unpause dan trigger `mysql_to_dwh__<table>`
yang dihasilkan `mysql_to_dwh_factory.py`.

### 6. Verifikasi tiap tahap

**SeaweedFS (CSV staging)** — buka Filer UI di <http://localhost:8888> dan
telusuri `buckets/raw-zone/raw/<table>/dt=<tanggal>/data.csv`, atau Admin UI
di <http://localhost:23646>.

**PostgreSQL (schema `staging`):**

```bash
docker compose exec warehouse-postgres psql -U warehouse -d warehouse_db -c "\dt staging.*"
docker compose exec warehouse-postgres psql -U warehouse -d warehouse_db -c "SELECT * FROM staging.stg_customers LIMIT 10;"
```

**Hasil DQ** — buka task `dq_staging` / `dq_dwh` di Airflow UI untuk melihat
status tiap check, atau dari CLI:

```bash
docker compose exec airflow-scheduler \
  airflow dags list-runs -d mysql_to_dwh_scalable
```

### 7. Contoh menambah tabel baru

Tambahkan tabel baru di MySQL (`initdb/01_schema.sql` atau langsung), lalu
tambahkan entri di YAML:

```yaml
tables:
  - name: promo_codes
    primary_key: promo_code_id
```

Simpan file, Airflow mem-parse ulang DAG secara otomatis, lalu trigger DAG.
Untuk pipeline factory, DAG `mysql_to_dwh__promo_codes` muncul otomatis.

## Rencana pengembangan data modelling (dbt)

> **Catatan penting:** bagian ini adalah *rencana*, bukan fitur yang sudah ada.
> Belum ada model dbt, konfigurasi dbt, DAG dbt, maupun eksekusi dbt di dalam
> pipeline saat ini.

**Kondisi sekarang:**
- Folder `dbt/` sengaja dikosongkan sebagai persiapan.
- Binary `dbt` (adapter `dbt-postgres`) tetap terinstal di image Airflow,
  di-isolasi pada virtualenv `/opt/dbt-venv` (lihat `Dockerfile`) agar
  dependensinya tidak bentrok dengan dependensi Airflow.
- Tidak ada file lain terkait dbt di repository ini.

**Rencana:**
1. Inisialisasi project dbt di folder `dbt/` (`dbt_project.yml`, `profiles.yml`
   menunjuk ke `warehouse-postgres`).
2. Model **staging** — view di atas tabel `staging.stg_<table>` hasil ingestion
   untuk standardisasi kolom dan tipe data.
3. Model **marts** — tabel dimensional (`dim_*` untuk customer, merchant,
   payment method) dan tabel fakta (`fct_*` untuk transaksi, settlement),
   mengikuti star schema di database `analytics`.
4. **Data tests** dbt — `unique`, `not_null`, dan `relationships` antar tabel
   marts sebagai lapisan DQ tambahan di luar Airflow.
5. **Orkestrasi** — menjadwalkan `dbt build` setelah tahap ingestion selesai
   (mis. via `BashOperator` / `Cosmos`), dengan venv dbt yang sama di container.

Progress pengembangan dbt akan didokumentasikan di bagian ini.

## Ports at a glance

| Port | Service |
|---|---|
| 8080 | Airflow UI |
| 8333 | S3 API SeaweedFS |
| 8888 | Filer UI SeaweedFS (browse object) |
| 9333 | Master UI SeaweedFS |
| 23646 | Admin UI SeaweedFS |
| 3306 | MySQL |
| 5433 | Warehouse PostgreSQL |
| 5555 | Flower (profile `flower`) |

## Troubleshooting

**`NoSuchBucket` / `404` saat task upload**
SeaweedFS hanya melakukan pre-create bucket saat volume `seaweedfs-data` baru.
DAG juga membuat bucket on-demand, jadi retry biasanya menyelesaikan masalah.
Untuk melakukan manual:

```bash
docker compose down -v        # -v juga menghapus seaweedfs-data, mysql-data, ...
docker compose up -d
```

**`SignatureDoesNotMatch` / `403 AccessDenied`**
Key yang dipakai SeaweedFS saat start tidak cocok dengan yang dipakai Airflow
untuk sign request. Kedua sisi membaca dua baris yang sama di `.env`
(`S3_ACCESS_KEY`, `S3_SECRET_KEY`); ubah keduanya lalu recreate seluruh stack:

```bash
docker compose down          # tambahkan -v jika key diubah
docker compose up -d
```

> ⚠️ SeaweedFS menyiapkan kredensial S3-nya saat volume `seaweedfs-data` dibuat
> pertama kali, sehingga perubahan key pada volume lama mungkin tidak berlaku.
> Jika ragu, gunakan `docker compose down -v` (ini juga menghapus data source
> dan warehouse).

**SeaweedFS menampilkan `unhealthy`**
Healthcheck memantau `http://localhost:9333/healthz`. Periksa log:

```bash
docker compose logs seaweedfs
```

Airflow hanya menunggu container SeaweedFS *berjalan* (`service_started`),
bukan hingga `healthy`, sehingga status unhealthy tidak pernah memblokir
Airflow sendiri — error sebenarnya akan muncul di log task `load_raw`.

**Task DQ gagal saat trigger ulang cepat**
`freshness` membandingkan usia kolom `loaded_at` dengan threshold
`freshness_minutes`. Jika Anda trigger DAG berulang dalam waktu dekat dan data
justru memenuhi batas threshold, naikkan nilai `freshness_minutes` di YAML.

**"Test" button pada connection `s3_raw` menampilkan `Unknown hook type "aws"`**
Ini normal. Package `apache-airflow-providers-amazon` sengaja tidak diinstal —
DAG berbicara ke object storage via `boto3` langsung dan hanya membaca
`.host` / `.port` / `.login` / `.password` dari connection, yang tidak
memerlukan hook terdaftar. Tombol tersebut hanya bagian dari UI; pipeline
tidak terpengaruh.

## Catatan CI/CD

`deploy.yml` (self-hosted runner) menjalankan: pull kode → `docker compose build
airflow-scheduler` → `docker compose up -d --no-build`. Setelah mengubah
`Dockerfile` atau `requirements.txt`, image akan ikut ter-build ulang otomatis
saat push ke branch `main`.

> `docker compose up` hanya membangun image bila image belum ada. Setelah
> mengubah `Dockerfile` atau `requirements.txt` secara lokal (tanpa push),
> lakukan build eksplisit:
>
> ```bash
> docker compose build
> ```
