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

# 让 dags 里的自定义包可被 import（可选）
ENV PYTHONPATH="/opt/airflow/dags:/opt/airflow/plugins:${PYTHONPATH}"

# ---- Python 依赖 ----
USER airflow

# # 临时调试标记，只为在 build 日志里确认
# RUN echo "USING THIS DOCKERFILE"

# 复制 requirements 并安装
COPY requirements.txt /requirements.txt
RUN python -m pip install --no-cache-dir -r /requirements.txt





