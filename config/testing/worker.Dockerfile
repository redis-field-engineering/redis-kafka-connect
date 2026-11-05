# Confluent's image may advance its bundled JDK independently of the platform version.
ARG CONFLUENT_VERSION=8.3.2
FROM eclipse-temurin:21-jdk AS java21
FROM confluentinc/cp-kafka-connect:${CONFLUENT_VERSION}
COPY --from=java21 /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"
