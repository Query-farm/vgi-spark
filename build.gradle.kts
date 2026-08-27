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
        //
        // A JDK 25 toolchain (to exercise the real FFM-backed launch:
        // transport under THIS repo's own test suite — farm.query.vgirpc
        // .launcher.PosixLauncherSupport ships a JDK-21 baseline stub, real
        // implementation only on a JDK-22+ multi-release overlay) was tried
        // and reverted: it works for the launcher itself, but Spark 4.2.0's
        // own Netty-based networking and Arrow's arrow-memory-netty
        // allocator want two DIFFERENT, mutually incompatible Netty majors
        // under JDK 25 on this dependency graph (unlike vgi-trino, which
        // has no such conflicting Netty consumer) — see connector/build
        // .gradle.kts's own comment for the exact failure. The launch:
        // default (VgiCatalogConfig#launcherEnabled) is instead verified via
        // an isolated reproduction outside this build's own test run, plus
        // matching vgi-trino's own proven identical wiring; VgiCatalogConfig
        // #launcherEnabled's own javadoc documents this and the graceful
        // degrade to the old per-connection subprocess spawn on a JDK <22
        // deployment runtime — which is what THIS toolchain now is again.
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
        maxHeapSize = "2g"
    }
}
