FROM apache/airflow:2.9.2

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY quarto.sh /
RUN cd / && bash /quarto.sh
