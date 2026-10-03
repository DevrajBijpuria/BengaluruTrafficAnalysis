
# Airflow 3.1.0 with Python 3.12
FROM apache/airflow:3.1.0-python3.12

USER airflow

# Install Airflow providers while keeping Airflow core pinned
# (constraints file stops pip from pulling provider/dependency versions that clash with Airflow 3.1.0)
RUN pip install --no-cache-dir \
    "apache-airflow==3.1.0" \
    apache-airflow-providers-snowflake \
    apache-airflow-providers-fab \
    apache-airflow-providers-standard \
    apache-airflow-providers-postgres \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.12.txt"

# Separate virtual environment for dbt
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
    "dbt-core==1.12.4" "dbt-snowflake==1.12.0"
