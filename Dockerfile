FROM maven:3-openjdk-15
COPY . .
RUN mvn clean install -DskipTests
ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS} -jar /target/harvesting-filtering-service.jar"]
