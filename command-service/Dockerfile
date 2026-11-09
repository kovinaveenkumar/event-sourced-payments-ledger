# Build step. Dependencies are downloaded first in their own layer, so code changes rebuild faster.
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q package -DskipTests

# Run step. Only the JRE, and the app runs as a normal user, not root.
FROM eclipse-temurin:17-jre
RUN useradd --system --uid 1001 app
WORKDIR /app
COPY --from=build /src/target/command-service-*.jar app.jar
USER app
EXPOSE 8081
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
