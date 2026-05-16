# Sử dụng Python 3.9 làm gốc để ổn định với Dagster/Airflow theo sơ đồ
FROM python:3.9-slim-bullseye

USER root

# Thiết lập các biến phiên bản để dễ quản lý
ENV SPARK_VERSION=3.5.7
ENV HADOOP_VERSION=3
ENV DELTA_VERSION=3.2.0
ENV POSTGRES_DRIVER_VERSION=42.7.2
ENV AWS_SDK_VERSION=1.12.262
ENV HADOOP_AWS_VERSION=3.3.4

# 1. Cài đặt các gói hệ thống và Java 17
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 2. Cấu hình biến môi trường Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# 3. Tải và cài đặt Apache Spark 3.5.7
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME && \
    rm spark.tgz

# 4. Tải các thư viện JARs cần thiết vào thư mục /opt/spark/jars
WORKDIR $SPARK_HOME/jars

# Delta Lake JARs
RUN curl -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar

# Kết nối MinIO (S3) JARs
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Kết nối PostgreSQL (Hive Metastore) JARs
RUN curl -O https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_DRIVER_VERSION}/postgresql-${POSTGRES_DRIVER_VERSION}.jar

# 5. Cài đặt thư viện Python hỗ trợ (Pyspark, Delta, Boto3)
RUN pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    delta-spark==${DELTA_VERSION} \
    boto3

# 6. Thiết lập thư mục làm việc và phân quyền cho Hive
WORKDIR /app
RUN mkdir -p /opt/spark/metastore_db && chmod -R 777 /opt/spark/metastore_db

# Lệnh để giữ container sống khi chạy (để bạn dùng docker exec hoặc Dagster gọi)
CMD ["tail", "-f", "/dev/null"]