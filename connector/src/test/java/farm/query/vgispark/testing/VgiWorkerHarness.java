// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Starts the real reference fixture worker against each transport {@code
 * VgiWorkerClient} understands, and hands back the {@code location} value
 * plus a teardown — so test content can run against subprocess, {@code
 * unix://}, {@code tcp://}, and {@code http(s)://} without each transport's
 * test class re-deriving its own spawn/discovery logic.
 *
 * <p><strong>Backed by {@code vgi-rust}'s {@code vgi-example-worker} binary,
 * not the Python one.</strong> Both are byte-for-byte wire-compatible
 * reference fixtures (same catalogs, same functions, same test-only fixture
 * surface — {@code vgi-rust}'s own worker is a deliberate line-for-line port
 * of the Python one for exactly this reason), but every {@code uv run
 * --project ~/Development/vgi-python vgi-fixture-worker} invocation pays a
 * real, per-process Python/venv-resolution startup cost — multiplied by
 * however many fresh worker processes a test run spawns (one per {@code
 * unix()}/{@code tcp()}/{@code http()} call across every test class, plus
 * one per POOLED CONNECTION for a bare {@code subprocess()} location).
 * {@code vgi-example-worker} is a single compiled binary with no
 * interpreter/dependency-resolution step at all — its own discovery line
 * appears in well under a second even under heavy machine load, vs. several
 * seconds (worse under contention) for {@code uv run}. When both {@code
 * target/debug/vgi-example-worker} and {@code target/release/...} exist,
 * picks whichever has the newer mtime — not a fixed preference either way:
 * during active {@code vgi-rust} source development the debug build is
 * usually the fresher one (a quick {@code cargo build}, no {@code
 * --release}), but for actually RUNNING this connector's own test suite
 * (this class's real purpose) an operator who deliberately built {@code
 * --release} wants that optimized binary picked up, not silently
 * shadowed by a stale-but-still-present debug one. Startup latency (spawn +
 * discovery-line wait) doesn't depend on optimization level either way, but
 * steady-state per-call throughput — real for a 327-file sqllogictest sweep
 * moving real row data — does.
 *
 * <p>{@code unix}/{@code tcp} spawn the worker themselves and block-read its
 * stdout for the one discovery line ({@code UNIX:<path>} / {@code
 * TCP:<host>:<port>} / {@code PORT:<port>} for HTTP — {@code vgi-rust}'s
 * {@code vgi::transport} module's own convention, matching the Python
 * worker's {@code UNIX:}/{@code TCP:} prefixes and simplifying HTTP's own
 * discovery to the same "one stdout line" shape instead of Python's
 * separate {@code --port-file} mechanism), not a fixed {@code
 * Thread.sleep()} poll — a real worker process whose startup time isn't a
 * constant.
 */
public final class VgiWorkerHarness {

    private VgiWorkerHarness() {}

    /** A running (or, for {@code subprocess}, not-yet-started) worker: the
     *  {@code location} value to hand this connector, and a teardown to run afterward. */
    public record Handle(String location, AutoCloseable teardown) {}

    /**
     * @return the {@code vgi-example-worker} binary path — whichever of the
     *         debug/release builds is newer when both exist (see this
     *         class's own javadoc for why)
     * @throws IllegalStateException if neither build exists — run {@code
     *         cargo build [--release] --bin vgi-example-worker} in {@code
     *         vgiRustDir} first
     */
    public static Path workerBinary(File vgiRustDir) {
        Path debug = vgiRustDir.toPath().resolve("target/debug/vgi-example-worker");
        Path release = vgiRustDir.toPath().resolve("target/release/vgi-example-worker");
        boolean debugOk = Files.isExecutable(debug);
        boolean releaseOk = Files.isExecutable(release);
        if (debugOk && releaseOk) {
            try {
                return Files.getLastModifiedTime(debug).compareTo(Files.getLastModifiedTime(release)) >= 0
                        ? debug : release;
            } catch (IOException e) {
                return debug; // mtime read raced a concurrent rebuild — either binary works, just pick one
            }
        }
        if (debugOk) return debug;
        if (releaseOk) return release;
        throw new IllegalStateException("vgi-example-worker binary not found under " + vgiRustDir
                + "/target/{debug,release} — run `cargo build --bin vgi-example-worker` (or --release) there first");
    }

    /**
     * Bare command — this connector's own pool spawns one subprocess per
     * pooled connection. Serves the {@code "example"} catalog (the binary's
     * own default when {@code VGI_WORKER_CATALOG_NAME} is unset).
     */
    public static Handle subprocess(File vgiRustDir) {
        return new Handle(workerBinary(vgiRustDir).toString(), () -> {});
    }

    /** One real worker process listening on a fresh temp-directory Unix domain socket,
     *  serving the {@code "example"} catalog. */
    public static Handle unix(File vgiRustDir) throws IOException {
        return unix(vgiRustDir, "example");
    }

    /**
     * Same as {@link #unix(File)}, but selecting a DIFFERENT catalog the
     * same {@code vgi-example-worker} binary can serve — e.g. {@code
     * "versioned"} (ATTACH-time version negotiation) or {@code
     * "attach_options"}/{@code "attach_options_required"} (custom ATTACH
     * options) — via {@code VGI_WORKER_CATALOG_NAME}, matching {@code
     * vgi-example-worker}'s own {@code main.rs} switch. Each of those is a
     * separate ENTRY-POINT SCRIPT on the Python side but a single binary
     * plus an env var here — one real difference the wire-compatibility
     * claim doesn't cover, just how each SDK's own fixture set is packaged.
     *
     * @param catalogName the VGI-side catalog to serve — passed as {@code
     *        VGI_WORKER_CATALOG_NAME}
     */
    public static Handle unix(File vgiRustDir, String catalogName) throws IOException {
        Path socketDir = Files.createTempDirectory("vgi-spark-unix-");
        Path socketPath = socketDir.resolve("w.sock");
        ProcessBuilder pb = new ProcessBuilder(workerBinary(vgiRustDir).toString(), "--unix", socketPath.toString())
                .directory(vgiRustDir)
                .redirectError(ProcessBuilder.Redirect.DISCARD);
        pb.environment().put("VGI_WORKER_CATALOG_NAME", catalogName);
        Process worker = pb.start();
        awaitDiscoveryLine(worker, "UNIX:");
        return new Handle("unix://" + socketPath, () -> {
            worker.destroy();
            Files.deleteIfExists(socketPath);
            Files.deleteIfExists(socketDir);
        });
    }

    /** One real worker process listening on an auto-selected TCP port, serving the {@code "example"} catalog. */
    public static Handle tcp(File vgiRustDir) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(workerBinary(vgiRustDir).toString(), "--tcp", "0")
                .directory(vgiRustDir)
                .redirectError(ProcessBuilder.Redirect.DISCARD);
        Process worker = pb.start();
        String discovery = awaitDiscoveryLine(worker, "TCP:");
        String hostPort = discovery.substring("TCP:".length());
        return new Handle("tcp://" + hostPort, worker::destroy);
    }

    /** One real worker process serving HTTP, port discovered via the {@code PORT:<port>} stdout
     *  discovery line — the same one-line convention {@code --unix}/{@code --tcp} use. */
    public static Handle http(File vgiRustDir) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(workerBinary(vgiRustDir).toString(), "--http")
                .directory(vgiRustDir)
                .redirectError(ProcessBuilder.Redirect.DISCARD);
        Process worker = pb.start();
        String discovery = awaitDiscoveryLine(worker, "PORT:");
        int port = Integer.parseInt(discovery.substring("PORT:".length()).trim());
        return new Handle("http://127.0.0.1:" + port, worker::destroy);
    }

    /** Block-read {@code worker}'s stdout for a line starting with {@code prefix}, skipping anything
     *  else (banner noise, warnings) up to a byte cap, then drains the rest of stdout on a daemon
     *  thread so the child never blocks on a full pipe after the line we care about has been read. */
    private static String awaitDiscoveryLine(Process worker, String prefix) throws IOException {
        BlockingQueue<Object> lines = new ArrayBlockingQueue<>(64); // String lines, or a Throwable on failure
        Thread reader = new Thread(() -> {
            long bytesRead = 0;
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(worker.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    bytesRead += line.length() + 1;
                    if (bytesRead > 1_048_576) {
                        lines.offer(new IOException("exceeded 1 MiB of stdout without a " + prefix + " line"));
                        return;
                    }
                    lines.offer(line);
                    if (line.startsWith(prefix)) {
                        // Keep draining afterward so the child never blocks on a full stdout pipe.
                        while (in.readLine() != null) { /* discard */ }
                        return;
                    }
                }
                lines.offer(new IOException("worker's stdout closed before a " + prefix + " line appeared"));
            } catch (IOException e) {
                lines.offer(e);
            }
        }, "vgi-worker-harness-discovery");
        reader.setDaemon(true);
        reader.start();

        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (!worker.isAlive()) {
                throw new IOException("worker exited before a " + prefix + " line appeared (exit code "
                        + worker.exitValue() + ")");
            }
            Object next;
            try {
                next = lines.poll(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted waiting for a " + prefix + " line", e);
            }
            if (next instanceof IOException e) throw e;
            if (next instanceof String s && s.startsWith(prefix)) return s;
        }
        worker.destroy();
        throw new IOException("timed out waiting for a " + prefix + " line on the worker's stdout");
    }
}
