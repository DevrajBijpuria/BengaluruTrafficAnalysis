from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator

DBT= "/opt/airflow/dbt_venv/bin/dbt"
DBT_PROJECT="/opt/airflow/dbt/traffic"
DBT_PROFILES="/opt/airflow/dbt/profiles"

COPY_RAW=[
    "COPY INTO RAW.RAW_TRAFFIC_DATA FROM @TRAFFIC.RAW.TRAFFIC_RAW_STAGE ON_ERROR = 'CONTINUE' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;",
    "COPY INTO  AGGREGATED.TRAFFIC_METRICS_5MIN FROM @TRAFFIC.AGGREGATED.TRAFFIC_AGG_STAGE ON_ERROR = 'CONTINUE' MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ;"
]
with DAG (
    dag_id="traffic_build",
    start_date=datetime(2026, 10, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["traffic","batch","dbt"],
    doc_md=__doc__
) as dag:
    
    reload_raw=SQLExecuteQueryOperator(
        task_id="reload_raw",
        conn_id="snowflake_default",
        sql=COPY_RAW,
        split_statements=True,
        autocommit=True
    )

    dbt_build_code=BashOperator(
        task_id="dbt_build_code",
        bash_command=f"{DBT} build --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROFILES}"
    )

    reload_raw >> dbt_build_code

