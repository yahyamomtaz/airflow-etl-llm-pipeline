FROM apache/airflow:3.1.3-python3.10

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && pip install --no-cache-dir \
        apache-airflow-providers-fab \
        apache-airflow-providers-ssh \
        apache-airflow-providers-sftp \
        apache-airflow-providers-smtp \
        apache-airflow-providers-celery
