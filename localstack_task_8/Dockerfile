FROM apache/airflow:2.10.3-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    default-jre-headless \
    build-essential \
    git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt