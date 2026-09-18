FROM apache/airflow:2.7.1-python3.11

USER root

# Java 11 + build tools (fixed apt sources + compiler for pip packages that need to build from source)
RUN sed -i '/deb.debian.org\/debian-security/d' /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk-headless build-essential python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt