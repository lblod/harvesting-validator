FROM maven:3.6.3-amazoncorretto-15 as builder
LABEL maintainer="info@redpencil.io"

WORKDIR /app

COPY pom.xml .

RUN mvn -B dependency:resolve-plugins dependency:resolve

COPY ./src ./src

RUN mvn package -DskipTests

FROM amazoncorretto:15-alpine

WORKDIR /app

COPY --from=builder /app/target/harvesting-validator.jar ./app.jar

ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /app/app.jar"]
