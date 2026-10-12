# build context is the repository root
FROM gradle:9.8.1-jdk21 AS build
WORKDIR /src
COPY proto proto
COPY java java
WORKDIR /src/java
RUN gradle --no-daemon -q installDist -x test

FROM eclipse-temurin:21-jre
COPY --from=build /src/java/build/install/dataset-java /opt/dataset-java
ENV PORT=50051 \
    JAVA_OPTS="-XX:MaxRAMPercentage=60 -XX:+UseSerialGC"
ENTRYPOINT ["/opt/dataset-java/bin/dataset-java"]
