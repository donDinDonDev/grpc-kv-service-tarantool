FROM maven:3.9.11-eclipse-temurin-21 AS build

WORKDIR /workspace

COPY .mvn/ .mvn/
COPY mvnw pom.xml ./

RUN chmod +x mvnw
RUN ./mvnw -B -Dmaven.test.skip=true dependency:go-offline

COPY src/ src/

RUN ./mvnw -B -Dmaven.test.skip=true package

FROM eclipse-temurin:21-jre

WORKDIR /opt/kvservice

COPY --from=build /workspace/target/*.jar app.jar

EXPOSE 8080 9090

ENV SPRING_PROFILES_ACTIVE=container

ENTRYPOINT ["java", "-jar", "/opt/kvservice/app.jar"]
