FROM apache/airflow:2.9.1-python3.10

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Copy requirements.txt and install Python packages with increased timeout
COPY docker/requirements.txt .
RUN pip install --no-cache-dir --timeout=1000 -r requirements.txt

# Copy dags
COPY dags/ $AIRFLOW_HOME/dags/

# Copy entrypoint.sh
COPY docker/entrypoint.sh /entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
