# # 1) 先停掉旧容器
# sudo docker compose down

# # 2) 重建镜像（把需要的py类型的写到requirement里面）
# sudo docker compose build --no-cache

# # 3) 以新镜像重建容器
# sudo docker compose up -d --force-recreate

# # 4) 验证 scheduler 容器内是否新装成功
# sudo docker compose exec scheduler bash -lc 'java -version && which java'


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
