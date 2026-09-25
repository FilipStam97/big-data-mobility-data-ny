FROM flink:2.1.0-scala_2.12-java17

USER root

RUN apt-get update && \
    apt-get install -y \
        python3 \
        python3-pip \
        python-is-python3 \
        maven && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install apache-flink==2.1.0

COPY pom.xml /tmp/pom.xml

RUN mvn -f /tmp/pom.xml dependency:copy-dependencies \
    -DoutputDirectory=/opt/flink/lib \
    -DincludeScope=runtime

USER flink