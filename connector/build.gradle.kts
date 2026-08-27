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

    // Real Spark jars for tests — a local SparkSession is how this connector's
    // own correctness is verified end to end (see connector/src/test).
    testImplementation("org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkVersion")
    testImplementation("org.apache.spark:spark-core_$scalaBinaryVersion:$sparkVersion")
    testImplementation("org.apache.spark:spark-sql-api_$scalaBinaryVersion:$sparkVersion")
    testImplementation("org.junit.jupiter:junit-jupiter:5.12.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

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
