FROM sbtscala/scala-sbt:eclipse-temurin-17.0.13_11_1.10.7_2.13.15 AS build
WORKDIR /src
COPY build.sbt .
COPY project/build.properties project/plugins.sbt ./project/
RUN sbt -batch update
COPY src ./src
RUN sbt -batch assembly

FROM eclipse-temurin:17-jre
RUN useradd --system --uid 1001 app
WORKDIR /app
COPY --from=build /src/target/scala-2.13/ledger-processor.jar app.jar
USER app
EXPOSE 8083
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75" LOG_APPENDER=JSON
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
