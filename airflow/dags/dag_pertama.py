"""
Contoh DAG pembelajaran; tidak terhubung ke pipeline data apapun.
Hanya menjalankan BashOperator untuk mencetak pesan ke log.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner' : 'Firza',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 5)

}

with DAG(
    dag_id = 'dag_pertama4',
    default_args = default_args,
    description = 'Contoh DAG pertama untuk belajar data scheduling',
    start_date = datetime(2025, 8, 2, 2),
    schedule_interval = '@daily'
) as dag:

    task1 = BashOperator(
        task_id = 'tugas_pertama',
        bash_command = "printf Halo Dunia, saya sedang belajar data scheduling",
    )

    task1
