FROM python:3.13-slim
WORKDIR /app

COPY training/requirements.txt ./training/requirements.txt

RUN pip install --no-cache-dir -r ./training/requirements.txt

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

ADD https://snapshots.elastic.co/maven/org/elasticsearch/elasticsearch-spark-41_2.13/9.6.0-SNAPSHOT/elasticsearch-spark-41_2.13-9.6.0-SNAPSHOT.jar /opt/spark-connectors/elasticsearch-spark-41_2.13-9.6.0-SNAPSHOT.jar

COPY streaming/*.py ./streaming/
COPY training/config/tournaments.json ./training/config/tournaments.json

CMD ["spark-submit", "--master", "local[2]", "--jars", "/opt/spark-connectors/elasticsearch-spark-41_2.13-9.6.0-SNAPSHOT.jar", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0", "streaming/stream_matches.py"]
