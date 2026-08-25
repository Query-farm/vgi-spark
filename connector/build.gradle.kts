// Copyright 2026 Query Farm LLC - https://query.farm

// Spark 4.2.0 (Scala 2.13; Spark 4 dropped 2.12). Bundles Arrow 19.0.0 and
// Netty 4.2.13.Final — vgi-rpc-java currently pins arrow-vector 18.1.0
// transitively, so this is the first place a version-alignment conflict
// (the same category vgi-trino hit against Trino's own Arrow/Netty pins)
// would surface. See the resolutionStrategy block below.
val sparkVersion = "4.2.0"
val scalaBinaryVersion = "2.13"

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
}
