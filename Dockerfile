FROM apache/airflow:2.9.1-python3.11
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install  apache-airflow-providers-apache-spark apache-airflow-providers-amazon
USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get clean
    #export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Set JAVA_HOME environment variable
#ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
COPY ./jobs /opt/airflow/jobs
COPY ./data /opt/bitnami/spark/data
RUN chmod 777 ./jobs
# Ensure JAVA_HOME is set correctly
#RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /opt/bitnami/spark/conf/spark-env.sh
