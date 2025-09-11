# # 1) 先停掉旧容器
# sudo docker compose down

# # 2) 重建镜像（把需要的py类型的写到requirement里面）
# sudo docker compose build --no-cache

# # 3) 以新镜像重建容器
# sudo docker compose up -d --force-recreate

# # 4) 验证 scheduler 容器内是否新装成功
# sudo docker compose exec scheduler bash -lc 'java -version && which java'


# 基础镜像（建议用带 Python 版本的标签，稳定可复现）
FROM apache/airflow:2.9.3-python3.11

# ---- 安装 Java（Spark / delta-spark 运行所需）----
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Java 路径（bookworm 下为 openjdk-17）
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# （可选）让容器里 Python 默认能 import 你的 dags 子模块
# /opt/airflow/dags 是你 compose 里挂载的路径
ENV PYTHONPATH="/opt/airflow/dags:${PYTHONPATH}"

# ---- 切回 airflow 用户并安装 Python 依赖 ----
USER airflow

# 把 requirements.txt 放到构建上下文同目录（containers/airflow/）
# 文件里务必包含：pandas、requests、pyspark==3.4.1、delta-spark==2.4.0 等
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# 如果你有不需要的脚本（比如之前的 quarto.sh），请删掉以下两行
# COPY quarto.sh /quarto.sh
# RUN bash /quarto.sh

FROM apache/airflow:2.9.0-python3.11

# 切到 root 用户才能装系统包
USER root

# 安装 OpenJDK 17（运行 Spark 必需）
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# 设置 Java 环境变量（PySpark 会用到）
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"

# 切回 airflow 用户（避免权限问题）
USER airflow

