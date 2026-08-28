// Copyright 2026 Query Farm LLC - https://query.farm

// Spark 4.2.0 (Scala 2.13; Spark 4 dropped 2.12). Bundles Arrow 19.0.0 and
// Netty 4.2.13.Final — vgi-rpc-java currently pins arrow-vector 18.1.0
// transitively, so this is the first place a version-alignment conflict
// (the same category vgi-trino hit against Trino's own Arrow/Netty pins)
// would surface. See the resolutionStrategy block below.
val sparkVersion = "4.2.0"
val scalaBinaryVersion = "2.13"

// A JDK 22+ toolchain (to exercise the real launch: transport under test —
// see root build.gradle.kts) was tried and reverted: forcing Netty down to
// vgi-trino's own 4.1.114.Final (to dodge arrow-memory-netty 18.1.0's
// UnsupportedOperationException under JDK 25's stricter Unsafe checks —
// NettyAllocationManager's static init crashing via EmptyByteBuf
// .memoryAddress -> UnsafeDirectLittleEndian.<init>) breaks Spark's OWN
// networking instead: Spark 4.2.0's NettyBlockTransferService needs newer
// netty-common APIs (io.netty.util.concurrent.ThreadAwareExecutor) that
// don't exist in 4.1.114.Final, so every Spark-using test then failed with
// NoClassDefFoundError instead. Unlike vgi-trino (no such conflicting
// Netty consumer), this repo has two dependency trees wanting different
// Netty majors — not resolvable with a single force. See
// VgiCatalogConfig#launcherEnabled's own javadoc for how the launch:
// default's JDK 22+ gate is verified instead (isolated reproduction outside
// this test suite, plus vgi-trino's own proven identical wiring).

dependencies {
    // Spark is provided by the cluster (spark-submit's own classpath) at
    // runtime, exactly like trino-spi is compileOnly in vgi-trino.
    compileOnly("org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkVersion")
    compileOnly("org.apache.spark:spark-core_$scalaBinaryVersion:$sparkVersion")
    // spark-sql pulls this in transitively (it's where Identifier/CatalogPlugin
    // and the rest of connector.catalog actually live post the sql-api module
    // split), declared explicitly so the compile classpath doesn't depend on
    // transitive-resolution details.
    compileOnly("org.apache.spark:spark-sql-api_$scalaBinaryVersion:$sparkVersion")

    // farm.query:vgi is the VGI client SDK: RpcConnection.proxy(VgiService.class),
    // the protocol records, and the client.* pushdown/projection encoders. Built
    // from source via the composite build in ../settings.gradle.kts so this
    // module sees the table_function_plan client additions.
    implementation("farm.query:vgi:0.27.0")

    // Iceberg's Spark integration — needed only for VgiNativeScanResolver's
    // iceberg_scan native-delegation handler (see that class's own javadoc).
    // compileOnly, same tier as Spark itself: an operator wanting
    // iceberg_scan delegation adds this jar to their own cluster; it is NOT
    // bundled into assembleDeployDir's output. No iceberg-spark-runtime build
    // targets Spark 4.2 yet (confirmed against Maven Central's search API,
    // 2026-08-28) -- 4.0_2.13 is the closest available and is what's used
    // here; re-pin to a 4.2-specific build once/if one is published.
    compileOnly("org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0")

    // Real Spark jars for tests — a local SparkSession is how this connector's
    // own correctness is verified end to end (see connector/src/test).
    testImplementation("org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkVersion")
    testImplementation("org.apache.spark:spark-core_$scalaBinaryVersion:$sparkVersion")
    testImplementation("org.apache.spark:spark-sql-api_$scalaBinaryVersion:$sparkVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:5.12.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// This module's own jar carries only its own compiled classes — no
// shadow/shade plugin, matching vgi-trino's own choice not to fat-jar
// either. An operator (or spark-submit --jars/--packages) needs this jar
// PLUS every runtime dependency it doesn't already get from Spark's own
// classpath. Mirrors vgi-trino's own assemblePluginDir Copy task (that one
// flattens into the directory-of-jars shape Trino's plugin loader
// specifically needs; this one just flattens into a plain directory
// spark-submit --jars can point at directly).
//
// org.apache.arrow/io.netty are EXCLUDED from the copy even though they're
// on runtimeClasspath (farm.query:vgi pulls arrow-vector 18.1.0 and
// io.netty transitively via farm.query:vgirpc) — Spark 4.2.0 itself
// bundles newer versions of both (Arrow 19.0.0, Netty 4.2.13.Final; see
// this file's own top comment), and shipping vgi's older copies via
// --jars would put two conflicting majors on the same executor classpath —
// the same category of conflict root build.gradle.kts's own comment
// documents hitting at BUILD-toolchain time (JDK 25 + Spark's networking
// vs. arrow-memory-netty), just at DEPLOYMENT-classpath time instead.
// Confirmed safe to exclude, not just assumed: Gradle's own dependency
// resolution on this module's test classpath already upgrades both
// (`arrow-vector:18.1.0 -> 19.0.0`, `netty-{buffer,common}:4.2.9.Final ->
// 4.2.13.Final` — run `./gradlew :connector:dependencies --configuration
// testRuntimeClasspath` to see it), and vgi-java/vgi-rpc-java's own code
// runs this whole connector's entire test suite successfully against
// those newer, Spark-provided versions every time — proof the newer
// versions already satisfy whatever API surface vgi's own code needs.
val deployExcludedGroups = setOf("org.apache.arrow", "io.netty")
val assembleDeployDir by tasks.registering(Copy::class) {
    group = "distribution"
    description = "Assemble this connector's own jar plus its non-Spark, non-Arrow, " +
            "non-Netty runtime dependencies into build/deploy/, ready for spark-submit --jars " +
            "(Arrow/Netty are excluded -- see this task's own comment)."
    from(tasks.named("jar"))
    // A real, previously-undiscovered bug lived here: a raw Kotlin lambda
    // (`from({ ... })`) is NOT one of the source types Gradle's CopySpec
    // resolution recognizes (Closure, Callable, Provider, FileCollection,
    // Task, ...) -- a bare Kotlin `Function0` isn't `java.util.concurrent
    // .Callable`, so Gradle silently treated it as contributing ZERO files,
    // no error. Every dependency jar (vgi, vgirpc, jackson, jetty, ...) was
    // missing from every deploy dir this task ever produced; only the
    // connector's own jar (wired above via `tasks.named("jar")`, a real
    // Provider) actually landed. This went unnoticed because a *stale*
    // build/deploy/ directory from an earlier invocation (with different
    // composite-build inputs) still had old copies of those jars sitting
    // in it from a previous run -- `Copy` never cleans stale outputs it
    // didn't itself produce this time, so it looked populated. Caught by
    // the CI `docker` job's smoke test (a real query against a genuinely
    // fresh image), NOT by any earlier manual verification, which reused a
    // dev machine's already-populated build/deploy/ dir throughout. Fixed
    // two ways: `project.provider { ... }` IS a Gradle-recognized lazy
    // source (unlike a bare lambda), and `dependsOn(configurations
    // .runtimeClasspath)` explicitly forces every task that produces this
    // configuration's artifacts -- including the vgi-java/vgi-rpc-java
    // composite builds' own :vgi/:vgirpc `jar` tasks -- to run first
    // (Configuration is Buildable; resolving `.resolvedArtifacts` alone,
    // inside a lambda Gradle never actually calls, does NOT establish that
    // ordering on its own).
    dependsOn(configurations.runtimeClasspath)
    from(project.provider {
        configurations.runtimeClasspath.get().resolvedConfiguration.resolvedArtifacts
            .filter { it.moduleVersion.id.group !in deployExcludedGroups }
            .map { it.file }
    })
    into(layout.buildDirectory.dir("deploy"))
    // Stale outputs (e.g. an old connector-0.2.0.jar sitting next to a
    // fresh connector-0.3.0.jar after a version bump) are exactly what let
    // the bug above hide -- start every run from a clean directory instead
    // of merging into whatever a previous run left behind.
    doFirst { delete(layout.buildDirectory.dir("deploy")) }
}
tasks.named("assemble") { dependsOn(assembleDeployDir) }

// Fresh JVM per test class: several test classes each open a real pooled
// connection to a real subprocess/socket worker via VgiWorkerClient's own
// cached thread pool, and a local SparkSession also spins up its own thread
// pools per instance — matches vgi-trino's own forkEvery rationale (OS-level
// socket/thread accumulation across many such classes in one JVM).
tasks.test {
    forkEvery = 1
    // Run those per-class forks CONCURRENTLY instead of one at a time — each
    // fork is a fully separate JVM/process (its own SparkSession, its own
    // worker subprocess, its own temp unix socket), so there's no shared
    // mutable state between them to race on; the only real constraints are
    // host RAM/CPU. Left at Gradle's own default (1) this connector's test
    // suite ran every class strictly sequentially regardless of how many
    // cores the host actually has — the sweep's own internal FILE_PARALLELISM
    // thread pool was the only thing that ever used more than one core at a
    // time. Divide by 4, not 2: several classes each burst wide on their own
    // (the sweep especially, up to 32 threads) once running, so the fork
    // COUNT needs headroom below the raw core count rather than assuming
    // every fork stays single-threaded.
    maxParallelForks = (Runtime.getRuntime().availableProcessors() / 4).coerceAtLeast(1)
}
