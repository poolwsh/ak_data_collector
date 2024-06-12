FROM apache/airflow:2.9.1-python3.11

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH

USER root
RUN apt-get update && apt-get install -y \
    vim \
    procps \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY ./airflow/requirements.txt $AIRFLOW_HOME/tools/requirements.txt
RUN pip install --no-cache-dir --timeout=1000 -r $AIRFLOW_HOME/tools/requirements.txt

COPY ./airflow/dags/ $AIRFLOW_HOME/dags/
COPY ./airflow/init_dag_tables.py $AIRFLOW_HOME/tools/init_dag_tables.py
COPY ./airflow/entrypoint.sh $AIRFLOW_HOME/tools/entrypoint.sh

ENTRYPOINT ["/opt/airflow/tools/entrypoint.sh"]
