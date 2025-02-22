FROM gradle:8.4.0-jdk21 as gradle
USER root
WORKDIR /http4k
COPY build.gradle.kts build.gradle.kts
COPY settings.gradle.kts settings.gradle.kts
COPY core core
COPY core-jdbc core-jdbc
COPY core-pgclient core-pgclient
COPY helidon-jdbc helidon-jdbc
COPY helidon-graalvm helidon-graalvm

RUN gradle --quiet --no-daemon helidon-graalvm:shadowJar
FROM ghcr.io/graalvm/graalvm-community:21.0.0-ol9-20230919 as graalvm
COPY --from=gradle /http4k/core/src/main/resources/* /home/app/http4k-helidon-graalvm/
COPY --from=gradle /http4k/helidon-graalvm/build/libs/http4k-benchmark.jar /home/app/http4k-helidon-graalvm/
COPY --from=gradle /http4k/helidon-graalvm/config/*.json /home/app/http4k-helidon-graalvm/
WORKDIR /home/app/http4k-helidon-graalvm
RUN native-image \
    -H:ReflectionConfigurationFiles=reflect-config.json \
    -H:ResourceConfigurationFiles=resource-config.json \
    --initialize-at-build-time="org.slf4j.LoggerFactory,org.slf4j.simple.SimpleLogger,org.slf4j.impl.StaticLoggerBinder" \
    --no-fallback -cp http4k-benchmark.jar Http4kGraalVMBenchmarkServerKt

EXPOSE 9000
ENTRYPOINT ["/home/app/http4k-helidon-graalvm/http4kgraalvmbenchmarkserverkt"]