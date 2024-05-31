FROM apache/airflow:2.9.1-python3.10

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Copy requirements.txt and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy dags
COPY ../dags/ $AIRFLOW_HOME/dags/

# Copy entrypoint.sh
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
