FROM apache/airflow:2.9.1-python3.12

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow:$PYTHONPATH

# Copy requirements.txt and install Python packages with increased timeout
COPY tools/requirements.txt .
RUN pip install --no-cache-dir --timeout=1000 -r requirements.txt

# Copy dags
COPY dags/ $AIRFLOW_HOME/dags/

# Copy tools
COPY tools/ /opt/airflow/tools/

# Copy entrypoint.sh
COPY docker/entrypoint.sh /entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
