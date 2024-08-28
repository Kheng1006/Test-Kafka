FROM apache/hive:4.0.0

USER root

RUN apt-get update && apt-get install -y python3 python3-pip curl net-tools

RUN apt-get install -y libpq-dev

RUN mkdir -p /opt/hive/lib

RUN curl -L -o /opt/hive/lib/postgres.jar \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar"
