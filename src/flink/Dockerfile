FROM flink:1.20-scala_2.12
USER root
RUN wget -q -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar \
    && wget -q -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar \
    && wget -q -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar
RUN apt-get update && apt-get install -y python3 python3-pip python3-venv && ln -s /usr/bin/python3 /usr/bin/python
RUN pip3 install apache-flink==1.20.5
COPY src/flink/job.py /opt/flink/job.py
