FROM ubuntu:20.04 AS builder
RUN apt-get update && apt-get install -y curl unzip

FROM cnfldemos/cp-server-connect-datagen:0.6.2-7.5.0

USER root

COPY --from=builder /usr/bin/curl /usr/bin/
COPY --from=builder /usr/bin/unzip /usr/bin/

# Download and install Confluent Hub client without needing additional packages
RUN curl -L -o confluent-hub-client.zip https://packages.confluent.io/archive/6.2/confluent-hub-client-6.2.0.zip && \
    mkdir -p /usr/local/bin/confluent-hub-client && \
    unzip confluent-hub-client.zip -d /usr/local/bin/confluent-hub-client && \
    chmod +x /usr/local/bin/confluent-hub-client/bin/* && \
    rm confluent-hub-client.zip

# Install the desired SMT (e.g., connect-transforms)
RUN /usr/local/bin/confluent-hub-client/bin/confluent-hub install confluentinc/connect-transforms:latest --no-prompt


