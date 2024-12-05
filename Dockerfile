FROM apache/airflow:latest-python3.8

# Arguments for Airflow user configuration
ARG AIRFLOW_UID=50000
ARG AIRFLOW_GID=0
ARG AIRFLOW_HOME=/opt/airflow

USER root

# Create Airflow directories with proper permissions
RUN mkdir -p ${AIRFLOW_HOME}/dags \
    && mkdir -p ${AIRFLOW_HOME}/logs \
    && mkdir -p ${AIRFLOW_HOME}/scripts \
    && mkdir -p ${AIRFLOW_HOME}/files \
    && chown -R ${AIRFLOW_UID}:${AIRFLOW_GID} ${AIRFLOW_HOME}

# Install system dependencies if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt /requirements.txt
RUN chown ${AIRFLOW_UID}:${AIRFLOW_GID} /requirements.txt

# Switch to Airflow user for pip installations
USER ${AIRFLOW_UID}

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
        boto3

# Copy DAGs after setting permissions
COPY --chown=${AIRFLOW_UID}:${AIRFLOW_GID} dags ${AIRFLOW_HOME}/dags/

# Ensure we stay as Airflow user
USER ${AIRFLOW_UID}