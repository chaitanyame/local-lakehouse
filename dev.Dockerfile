# Use the official Python image with Debian bookworm (newer and more secure)
FROM python:3.11-slim-bookworm

# Set environment variables for Spark and Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_VERSION=3.5.5 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/home/spark \
    PATH=/home/spark/bin:$PATH \
    JAVA_VERSION=17 \
    HUDI_VERSION=0.15.0 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install necessary packages and dependencies in a single layer to reduce image size
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-${JAVA_VERSION}-jre-headless \
    curl \
    wget \
    vim \
    sudo \
    whois \
    dos2unix \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download and install Spark in a single layer
RUN SPARK_DOWNLOAD_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mkdir -p /home/spark \
    && wget --no-verbose -O apache-spark.tgz "${SPARK_DOWNLOAD_URL}" \
    && tar -xf apache-spark.tgz -C /home/spark --strip-components=1 \
    && rm apache-spark.tgz

# Set up a non-root user
ARG USERNAME=sparkuser
ARG USER_UID=1000
ARG USER_GID=1000

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m -s /bin/bash $USERNAME \
    && echo "$USERNAME ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# Create directories for logs and event logs and set up Spark configuration in a single layer
RUN mkdir -p ${SPARK_HOME}/logs ${SPARK_HOME}/event_logs ${SPARK_HOME}/jars \
    && echo "spark.eventLog.enabled true" >> $SPARK_HOME/conf/spark-defaults.conf \
    && echo "spark.eventLog.dir file://${SPARK_HOME}/event_logs" >> $SPARK_HOME/conf/spark-defaults.conf \
    && echo "spark.history.fs.logDirectory file://${SPARK_HOME}/event_logs" >> $SPARK_HOME/conf/spark-defaults.conf \
    && chown -R $USER_UID:$USER_GID ${SPARK_HOME}

# Copy core-site.xml configuration
COPY core-site.xml $SPARK_HOME/conf/core-site.xml

# Install Python packages for Jupyter and PySpark using pip with security flags
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir jupyter findspark

# Download JAR files in a single layer to reduce image size and build time
RUN curl -o $SPARK_HOME/jars/hudi-spark3-bundle_2.12-${HUDI_VERSION}.jar https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3-bundle_2.12/${HUDI_VERSION}/hudi-spark3-bundle_2.12-${HUDI_VERSION}.jar && \
    curl -o $SPARK_HOME/jars/spark-avro_2.12-${SPARK_VERSION}.jar https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/${SPARK_VERSION}/spark-avro_2.12-${SPARK_VERSION}.jar && \
    curl -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.592.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.592/aws-java-sdk-bundle-1.12.592.jar

# Add the entrypoint script with explicit permissions
COPY entrypoint.sh /home/spark/entrypoint.sh
RUN dos2unix /home/spark/entrypoint.sh && \
    chmod +x /home/spark/entrypoint.sh && \
    chown $USER_UID:$USER_GID /home/spark/entrypoint.sh

# Switch to non-root user for security
USER $USERNAME

# Set workdir and create application directory
RUN mkdir -p /home/$USERNAME/app
WORKDIR /home/$USERNAME/app

# Expose necessary ports for Jupyter and Spark UI
EXPOSE 4040 4041 18080 8888

# Health check for the container
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8888 || exit 1

ENTRYPOINT ["/home/spark/entrypoint.sh"]