from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'dustin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id ='cms_pipeline',
    default_args=default_args,
    description='CMS Medicare Part D pipeline',
    schedule_interval='@weekly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['cms','medicare'],
) as dag:

    # task 1: download nppes weekly file
    download_nppes_weekly = SSHOperator(
        task_id='download_nppes_weekly',
        ssh_conn_id='ssh_arch_host',
        command='cd /mnt/storage2/Projects/cms_ingest && .venv/bin/python nppes_weekly_update.py',
    )

    # task 2: refresh stg.nppes
    refresh_nppes_weekly = PostgresOperator(
        task_id='refresh_nppes_weekly',
        postgres_conn_id='postgres_portfolio',
        sql='CALL adm.refresh_nppes_weekly();',
    )

    # task 3: update dim_provider
    update_dim_provider = PostgresOperator(
        task_id='update_dim_provider',
        postgres_conn_id='postgres_portfolio',
        sql='CALL adm.update_dim_provider();',
    )

    # task 4: run dbt models
    dbt_run = SSHOperator(
        task_id='dbt_run',
        ssh_conn_id='ssh_arch_host',
        command='cd /mnt/storage2/Projects/dbt/cms_medicare && .venv/bin/dbt run --select tag:weekly',
    )

    # set dependencies
    download_nppes_weekly >> refresh_nppes_weekly >> update_dim_provider >> dbt_run
