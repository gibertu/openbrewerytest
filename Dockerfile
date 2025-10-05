# Dockerfile for custom Airflow image with additional Python packages
# Extends the official Apache Airflow image to include project-specific dependencies

# Base image: official Airflow 2.8.1 with Python 3.9
FROM apache/airflow:2.8.1-python3.9

ARG AIRFLOW_UID=50000
ARG AIRFLOW_GID=0

# Switch to root temporarily to install system dependencies for PySpark
USER root

# Install OpenJDK (required for PySpark) and procps (provides `ps` for Spark scripts)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Align airflow user/group with host-provided IDs to keep file ownership consistent
RUN set -eux; \
    if [ "${AIRFLOW_GID}" != "0" ]; then \
        if getent group airflow >/dev/null; then \
            groupmod -g "${AIRFLOW_GID}" airflow; \
        else \
            groupadd -g "${AIRFLOW_GID}" airflow; \
        fi; \
        usermod -g "${AIRFLOW_GID}" airflow; \
    fi; \
    usermod -u "${AIRFLOW_UID}" airflow; \
    chown -R "${AIRFLOW_UID}":"${AIRFLOW_GID}" /opt/airflow

# Switch back to airflow user for security
USER airflow

# Copy Python dependencies file into the container
COPY requirements.txt /requirements.txt

# Install Python packages
# --no-cache-dir: reduces image size by not storing pip cache
RUN pip install --no-cache-dir -r /requirements.txt
