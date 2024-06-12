FROM apache/airflow:2.9.1-python3.12

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH

# Install necessary tools
USER root
RUN apt-get update && apt-get install -y \
    vim \
    procps \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set ownership back to airflow user
USER airflow

# Copy tools and install Python packages with increased timeout
COPY ./requirements.txt /opt/airflow/tools/requirements.txt
RUN pip install --no-cache-dir --timeout=1000 -r /opt/airflow/tools/requirements.txt

# Copy dags
COPY ./dags/ $AIRFLOW_HOME/dags/

# Copy entrypoint.sh
COPY ./entrypoint.sh /opt/airflow/tools/entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/opt/airflow/tools/entrypoint.sh"]
