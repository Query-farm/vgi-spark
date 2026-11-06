# Copyright 2026 Query Farm LLC - https://query.farm
#
# Bakes this connector into a real Spark 4.2.0 image — the exact version
# this connector is built and tested against (see README.md's own "Spark/
# Scala compatibility" note), not the closest available published Docker
# Hub tag (no official `apache/spark:4.2.0` image exists as of this
# writing; the tarball is downloaded directly from Apache's own archive
# instead, mirroring how `dev/run-local-spark.sh` does the same for a
# non-Docker local run).
#
# Build the deploy dir first, from the repo root:
#   ./gradlew :connector:assembleDeployDir
#   docker build -f docker/Dockerfile -t vgi-spark .
#
# The two Spark JVM flags below (--add-opens, --enable-native-access) are
# baked in as spark-defaults.conf's spark.{driver,executor}.extraJavaOptions
# -- not guessed, ported directly from what vgi-trino's own docker/Dockerfile
# needed running the equivalent connector inside a real trinodb/trino image
# (see that file's own comment for the full story: Arrow's off-heap memory
# access crashes the FIRST catalog load without --add-opens on java.nio, and
# the launch: transport's Foreign Function & Memory API use needs
# --enable-native-access). Spark's own launch scripts already document this
# exact mechanism (root build.gradle.kts's own comment references it) --
# unlike Trino's isolated-per-plugin classloader, which can't grant a plugin
# native access for itself, Spark's own extraJavaOptions conf keys are the
# supported way to add whole-JVM flags, so that's what's used here rather
# than a Dockerfile-level JAVA_TOOL_OPTIONS hack.

FROM eclipse-temurin:21-jdk

ARG SPARK_VERSION=4.2.0
ARG SPARK_SHA512=3a6559e8546ff387db8fe7a04b8fe4008853b467b972c2e343df8e14a81450534777719fc5d4998dd96519a86fdf9de140cee753c61e2e94db044d5f9555ddc4
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL --retry 3 --retry-all-errors \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
        -o /tmp/spark.tgz \
    && echo "${SPARK_SHA512}  /tmp/spark.tgz" | sha512sum -c - \
    && mkdir -p "${SPARK_HOME}" \
    && tar -xzf /tmp/spark.tgz -C "${SPARK_HOME}" --strip-components=1 \
    && rm /tmp/spark.tgz

# This connector's own jar plus its non-Spark, non-Arrow, non-Netty runtime
# dependencies (assembleDeployDir's own output — see connector/build.gradle.kts
# for exactly what's excluded and why: Spark's bundled Arrow/Netty already
# satisfy what farm.query:vgi/vgirpc need, confirmed via this connector's own
# test-classpath resolution, not guessed). Dropped straight into Spark's own
# jars/ directory so the connector is just always on the classpath, the same
# way Spark's own bundled dependencies are -- no --jars/--packages needed at
# spark-submit/spark-shell time.
COPY connector/build/deploy/*.jar "${SPARK_HOME}/jars/"

RUN printf '%s\n' \
    'spark.driver.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED' \
    'spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED' \
    >> "${SPARK_HOME}/conf/spark-defaults.conf"

WORKDIR "${SPARK_HOME}"
