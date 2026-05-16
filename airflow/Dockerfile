FROM apache/airflow:3.1.5

USER root

ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt ${AIRFLOW_HOME}/requirements.txt
USER airflow

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r ${AIRFLOW_HOME}/requirements.txt