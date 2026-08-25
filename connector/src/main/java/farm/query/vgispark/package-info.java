// Copyright 2026 Query Farm LLC - https://query.farm

/**
 * A Spark DataSource V2 catalog connector for VGI (Vector Gateway Interface)
 * workers — the same out-of-process, Arrow-IPC-based worker protocol the
 * {@code vgi} DuckDB extension and {@code vgi-trino} attach to.
 *
 * <p>One Spark catalog ({@code spark.sql.catalog.&lt;name&gt;}) maps to one VGI
 * {@code ATTACH}, the same granularity DuckDB and Trino use.
 */
package farm.query.vgispark;
