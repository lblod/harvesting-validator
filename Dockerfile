FROM maven:3.9-eclipse-temurin-21 as builder
LABEL maintainer="info@redpencil.io"

WORKDIR /app

COPY pom.xml .

RUN mvn -B dependency:resolve-plugins dependency:resolve

COPY ./src ./src

RUN mvn package -DskipTests

FROM eclipse-temurin:21-jre

WORKDIR /app

COPY --from=builder /app/target/harvesting-validator.jar ./app.jar
ENV JAVA_OPTS=""
ENTRYPOINT ["java", "-XX:+CompactStrings", "${JAVA_OPTS}","-jar", "/app/app.jar"]
