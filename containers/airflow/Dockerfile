# 基础镜像：和你现有的一样
FROM apache/airflow:2.9.2

# ---- 安装 Java（给 PySpark 用）----
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
 && rm -rf /var/lib/apt/lists/*

# 配置 Java 环境变量（pyspark 需要）
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ---- 回到 airflow 用户，安装 Python 依赖 ----
USER airflow

# 安装 Python 依赖（requirements.txt 里请包含：pyspark==3.4.0、delta-spark==2.4.0）
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# 可选：你原有的脚本，按需保留
COPY quarto.sh /
RUN cd / && bash /quarto.sh
