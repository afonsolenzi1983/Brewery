# Start from the official Airflow image
FROM apache/airflow:2.9.3

# Switch to the root user to install packages
USER root

# Install default-jre-headless which is a minimal Java runtime
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean

# Switch back to the airflow user
USER airflow