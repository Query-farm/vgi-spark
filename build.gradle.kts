// Copyright 2026 Query Farm LLC - https://query.farm
plugins {
    java
}

allprojects {
    group = "farm.query"
    version = "0.1.0"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")

    extensions.configure<JavaPluginExtension> {
        // Build and target Java 21 — Spark 4.x supports 17 or 21, but this
        // module depends on farm.query:vgi, whose own :vgi module targets 21
        // bytecode (its shared-memory transport needs java.lang.foreign,
        // GA in 22, with a pipe-transport fallback on 21 — 21 is its floor,
        // not 17). Unlike vgi-trino (built/targeted at 25 because Trino 483
        // requires it), 21 is both floors' actual intersection here.
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.release.set(21)
        options.compilerArgs.addAll(listOf("-Xlint:all,-serial,-processing", "-parameters"))
        options.encoding = "UTF-8"
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        // Arrow's memory module needs access to java.nio internals (pulled in
        // transitively via farm.query:vgi's client package), and Spark's own
        // Arrow/Unsafe usage needs the same plus a handful of java.base/java.lang
        // opens on JDK 17+ (mirrors what Spark's own launch scripts set via
        // spark.driver.extraJavaOptions / spark.executor.extraJavaOptions).
        jvmArgs(
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "--add-opens=java.base/java.util=ALL-UNNAMED",
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            "--enable-native-access=ALL-UNNAMED",
        )
        systemProperty("arrow.memory.debug.allocator", "true")
        maxHeapSize = "2g"
    }
}
