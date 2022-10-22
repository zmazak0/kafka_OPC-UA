FROM ubuntu:xenial

RUN apt-get update && apt-get install -y \
    python3-pip \
    iputils-ping \
    libsnappy-dev

ENV LC_ALL C.UTF-8

RUN yes | apt-get install libffi-dev
RUN pip3 install --upgrade pip==20.3.4
COPY pip3libs.txt pip3libs.txt
RUN pip3 install -r pip3libs.txt

ENV OPCUA_SERVER localhost
ENV OPCUA_PORT 4840
ENV KAFKA_SERVER 192.168.0.7
ENV KAFKA_PORT 9092
ENV KAFKA_TOPIC aszolotarev
ENV SLEEP_DURATION 1
ENV OUT_FILE response
#ENV COMPRESSION snappy

COPY opc_ua_kafka_connector.py opc_ua_kafka_connector.py
COPY run.sh run.sh
RUN chmod +x run.sh
RUN sh run.sh