# # 1) 先停掉旧容器
# sudo docker compose down

# # 2) 重建镜像（把需要的py类型的写到requirement里面）
# sudo docker compose build --no-cache

# # 3) 以新镜像重建容器
# sudo docker compose up -d --force-recreate

# # 4) 验证 scheduler 容器内是否新装成功
# sudo docker compose exec scheduler bash -lc 'java -version && which java'


# 统一用一个基础镜像（和 compose 里保持一致）
FROM apache/airflow:2.9.3-python3.11

# ---- 系统依赖：Java（Spark/Delta 需要）----
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ---- Spark 依赖 JAR（Hadoop AWS + AWS SDK）----
# 下载到 /opt/spark/jars，避免 /content/jars 这种不存在的目录
RUN mkdir -p /opt/spark/jars && \
    curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# 让 Spark 知道这些 jars 的位置
ENV SPARK_CLASSPATH="/opt/spark/jars/*:${SPARK_CLASSPATH}"

# ✅ 新增：让 pyspark 自动加载 hadoop-aws & aws-java-sdk-bundle
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar pyspark-shell"


# 让 dags 里的自定义包可被 import（可选）
ENV PYTHONPATH="/opt/airflow/dags:/opt/airflow/plugins:${PYTHONPATH}"

# ---- Python 依赖 ----
USER airflow
COPY requirements.txt /requirements.txt
RUN python -m pip install --no-cache-dir -r /requirements.txt





