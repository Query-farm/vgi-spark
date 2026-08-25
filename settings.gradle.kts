// Copyright 2026 Query Farm LLC - https://query.farm
plugins {
    // Auto-provision the JDK toolchain when not installed locally.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "vgi-spark"

// Composite-include the sibling vgi-java repo so :vgi is built from source —
// this connector tracks client-side additions (TableFunctionPlanRequest, the
// max_splits_per_response plumbing) that may not be released to Maven Central
// yet. Falls back to the published farm.query:vgi artifact if the directory
// isn't present. VGI_JAVA_DIR overrides the path, mirroring vgi-trino's own
// convention for CI layouts where the sibling isn't checked out next door.
val vgiJavaDir = System.getenv("VGI_JAVA_DIR")?.let { file(it) }
    ?: file("../../vgi-java")
if (vgiJavaDir.isDirectory) {
    includeBuild(vgiJavaDir) {
        dependencySubstitution {
            // Coordinates match what vgi-java actually publishes (group=farm.query,
            // see ~/vgi-java/build.gradle.kts).
            substitute(module("farm.query:vgi")).using(project(":vgi"))
        }
    }
}

include("connector")
