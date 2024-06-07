FROM apache/airflow:2.9.1-python3.12

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH

# Copy tools and install Python packages with increased timeout
COPY tools/ /opt/airflow/tools/
RUN pip install --no-cache-dir --timeout=1000 -r /opt/airflow/tools/requirements.txt

# Copy dags
COPY dags/ $AIRFLOW_HOME/dags/

# Copy entrypoint.sh
COPY docker/entrypoint.sh /entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
