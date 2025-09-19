# Start from the official Airflow image
FROM apache/airflow:2.8.1

# Switch to root to install system packages and perform other root-level tasks
USER root

# Install system packages required for the Postgres client and others
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    curl \
    && apt-get autoremove -yqq --purge && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt into the container
COPY requirements.txt .

# Switch to the 'airflow' user before installing Python packages
# This is the crucial fix!
USER airflow

# Install Python packages as the 'airflow' user
RUN pip install --no-cache-dir -r requirements.txt

# Back to root to copy files that require root access
USER root

# Copy Spark JARs for connectors
COPY ./spark/jars /opt/bitnami/spark/jars/

# Switch back to the airflow user for final execution
USER airflow