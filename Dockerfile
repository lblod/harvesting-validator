FROM maven:3.8-openjdk-18 as builder
LABEL maintainer="info@redpencil.io"

WORKDIR /app

COPY pom.xml .

RUN mvn -B dependency:resolve-plugins dependency:resolve

COPY ./src ./src

RUN mvn package -DskipTests

FROM eclipse-temurin:18-jre

WORKDIR /app

COPY --from=builder /app/target/harvesting-validator.jar ./app.jar

ENTRYPOINT ["sh", "-c", "java -Dlog4j2.formatMsgNoLookups=true ${JAVA_OPTS} -jar /app/app.jar"]
