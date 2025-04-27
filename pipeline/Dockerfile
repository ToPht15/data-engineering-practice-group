FROM apache/airflow:2.8.1

USER root

# Cài đặt các dependencies cần thiết
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Trở lại user airflow
USER airflow

COPY app/ /opt/airflow/app/
COPY dags/ /opt/airflow/dags/