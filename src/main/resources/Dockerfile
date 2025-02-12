FROM maven:3.8.4-openjdk-17 AS builder

WORKDIR /app
COPY pom.xml .
COPY src ./src

# Build the application
RUN mvn clean package -DskipTests

FROM apache/flink:1.17.1

# Copy the jar from the builder stage
COPY --from=builder /app/target/elastic-kafka-flink-1.0-SNAPSHOT.jar /opt/flink/usrlib/
COPY src/main/resources/config.properties /opt/flink/usrlib/
