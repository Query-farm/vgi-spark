// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.client;

import farm.query.vgi.SettingSpec;
import farm.query.vgi.VgiService;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgirpc.RpcConnection;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.launcher.LaunchConfig;
import farm.query.vgirpc.launcher.LauncherClient;
import farm.query.vgirpc.transport.RpcTransport;
import farm.query.vgirpc.transport.SubprocessTransport;
import farm.query.vgirpc.transport.TcpSocketTransport;
import farm.query.vgirpc.transport.UnixSocketTransport;
import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.settings.SettingSpecDecoder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.net.StandardProtocolFamily;
import java.net.URI;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * A pool of independent connections to one VGI worker, each attached to the
 * same VGI-side catalog.
 *
 * <p>Ported from {@code vgi-trino}'s connector-agnostic {@code VgiWorkerClient}
 * (which has no Trino-SPI dependency beyond its own config type) — see that
 * repo's own class javadoc for the full self-healing-pool design rationale,
 * reproduced below.
 *
 * <p>VGI's RPC is lockstep per connection — one call in flight at a time, the
 * same constraint that makes DuckDB pool subprocess workers rather than share
 * one — so redeeming {@code N} splits concurrently needs {@code N} independent
 * connections, not one shared client. This pool pre-spawns
 * {@link VgiCatalogConfig#connections()} of them at construction (each doing
 * its own {@code catalog_attach}) and hands them out via {@link #withConnection}.
 *
 * <p>A connection that throws is never returned to the pool as-is — VGI's
 * lockstep framing means a call that failed mid-stream may have left the wire
 * in an indeterminate state, and reusing it would corrupt the next call
 * rather than merely fail it. {@link #release} closes it and immediately
 * opens+attaches a fresh replacement in its place, so the POOL SIZE is
 * self-healing rather than monotonically shrinking: with {@code connections=N},
 * N cumulative connection failures over the catalog attachment's LIFETIME
 * (not per-query) would otherwise drain the pool to zero, and every
 * subsequent {@link #borrow} — on a completely unrelated, otherwise-healthy
 * query — would block forever on an empty queue that nothing would ever
 * refill. A worker that is genuinely down still degrades honestly: if the
 * replacement attach itself fails, that one slot is lost (logged failures
 * compound instead of manufacturing fake capacity), rather than retried in a
 * loop that could itself hang {@link #release}.
 *
 * <p>One pool instance is used on the Spark driver for catalog/table
 * discovery and each scan's bind+plan (see {@code VgiCatalog}/{@code
 * VgiScanBuilder}); {@code VgiPartitionReader} on each executor opens its own
 * independent connection per task rather than sharing this pool, since a
 * task's connection must survive for that task's entire lifetime and Spark
 * gives tasks no shared, driver-coordinated resource to borrow from.
 */
public final class VgiWorkerClient implements AutoCloseable {

    /**
     * One pooled, attached connection.
     *
     * @param connection {@link RpcConnection} for subprocess/{@code unix://}/
     *        {@code tcp://}, {@link HttpRpcConnection} for {@code http(s)://}
     *        — {@code AutoCloseable} is the only thing the rest of this class
     *        needs from it (see {@link #closeQuietly}); every actual RPC call
     *        goes through {@link #service} instead, which is transport-agnostic
     *        already (both connection types offer the identical {@code
     *        proxy(Class)} surface)
     */
    public record Attached(AutoCloseable connection, VgiService service, CatalogAttachResult attach) {
        /** @return the {@code attach_opaque_data} handle every subsequent call on this connection echoes */
        public byte[] handle() { return attach.attach_opaque_data(); }
    }

    private final VgiCatalogConfig config;
    // Guards both deques below. A plain monitor, not a BlockingQueue: async
    // acquisition (borrowAsync) needs to hand an available connection
    // straight to a WAITING FUTURE without any thread blocking to receive
    // it, which a blocking queue's own API has no way to express.
    private final Object lock = new Object();
    private final Deque<Attached> available = new ArrayDeque<>();
    private final Deque<CompletableFuture<Attached>> waiters = new ArrayDeque<>();
    // Every live connection this client has ever opened, for close() to shut
    // down — including self-healing replacements minted by release(), which
    // is why this can't be the fixed List.copyOf snapshot construction alone
    // produces. Concurrent because release() (many callers, many threads) and
    // close() (one, at shutdown) touch it independently of the pool queue.
    private final Queue<Attached> all = new ConcurrentLinkedQueue<>();
    // A dedicated pool for async plan-page fetches — deliberately NOT
    // CompletableFuture.supplyAsync's default (the JVM-wide common
    // ForkJoinPool), which is shared with whatever else the driver JVM uses
    // it for. A connector-private pool makes that categorically impossible.
    private final ExecutorService executor = Executors.newCachedThreadPool(
            runnable -> {
                Thread t = new Thread(runnable, "vgi-spark-worker-client");
                t.setDaemon(true);
                return t;
            });

    // Spark's CatalogPlugin has no shutdown/close lifecycle hook at all — a
    // catalog plugin instance is cached for the SparkSession's lifetime and
    // simply dropped, never told to release anything. Without this, this
    // pool's subprocess-transport connections (each a real child process)
    // would be silently orphaned or killed abruptly on JVM exit — the latter
    // caught mid-request in practice, producing a scary but harmless-looking
    // "ArrowInvalid: Tried reading schema message" traceback in the child's
    // own logs. A best-effort JVM shutdown hook closes them properly instead.
    private final Thread shutdownHook = new Thread(this::closeAllQuietly, "vgi-spark-worker-client-shutdown");

    /**
     * Spawn/connect {@link VgiCatalogConfig#connections()} independent
     * connections and attach each one.
     *
     * @param config the catalog's parsed configuration
     */
    public VgiWorkerClient(VgiCatalogConfig config) {
        this.config = config;
        int n = Math.max(1, config.connections());
        List<Attached> opened = new ArrayList<>(n);
        try {
            for (int i = 0; i < n; i++) {
                opened.add(openAndAttach());
            }
        } catch (RuntimeException e) {
            for (Attached a : opened) closeQuietly(a);
            throw e;
        }
        available.addAll(opened);
        all.addAll(opened);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    /** @return this client's configuration */
    public VgiCatalogConfig config() { return config; }

    // Decoded once, lazily, from whichever connection happens to answer
    // first — CatalogAttachResult.settings() is worker-level metadata (every
    // pooled connection's own attach reports the identical list), not
    // per-connection, so there's nothing to invalidate or re-derive per
    // borrow. A plain (unsynchronized) field: a benign race decodes the same
    // bytes into an equal map at most a handful of times at startup, never a
    // correctness issue, and not worth a lock for.
    private volatile Map<String, SettingSpec> declaredSettings;

    /**
     * @return this catalog's worker-declared session settings (name →
     *         {@link SettingSpec}, carrying each setting's Arrow type), from
     *         {@code CatalogAttachResult.settings} — what {@code
     *         VgiUnboundScalarFunction} intersects against Spark's own
     *         session config to build {@code BindRequest.settings} with the
     *         correct wire type instead of guessing from a plain string.
     */
    public Map<String, SettingSpec> declaredSettings() {
        Map<String, SettingSpec> cached = declaredSettings;
        if (cached != null) return cached;
        List<SettingSpec> specs = withConnection(a -> SettingSpecDecoder.decodeAll(a.attach().settings()));
        Map<String, SettingSpec> byName = new LinkedHashMap<>();
        for (SettingSpec spec : specs) byName.put(spec.name(), spec);
        Map<String, SettingSpec> immutable = Map.copyOf(byName);
        declaredSettings = immutable;
        return immutable;
    }

    // Same lazy/cache rationale as declaredSettings above — CatalogAttachResult
    // .default_schema is worker-level metadata, identical on every connection.
    private volatile String defaultSchema;

    /**
     * @return the schema an unqualified (namespace-less) identifier resolves
     *         against — {@code CatalogAttachResult.default_schema}, e.g.
     *         {@code "main"} on the standard fixture worker — or {@code null}
     *         if the worker didn't declare one (an unqualified reference then
     *         stays unresolvable, same as before this existed).
     */
    public String defaultSchema() {
        String cached = defaultSchema;
        if (cached != null) return cached;
        String resolved = withConnection(a -> a.attach().default_schema());
        if (resolved != null) defaultSchema = resolved;
        return resolved;
    }

    /**
     * @return the dedicated executor for async connection acquisition
     *         (never the JVM-wide common {@code ForkJoinPool} — see this
     *         field's own javadoc for why)
     */
    public ExecutorService executor() { return executor; }

    /**
     * Borrow a connection, run {@code fn} against it, and return it to the
     * pool — or evict it if {@code fn} threw.
     *
     * <p>For a call that needs to hold a connection across more than one
     * synchronous operation — a partition reader draining a producer stream
     * tick by tick — use {@link #borrow}/{@link #release} directly instead.
     *
     * @param fn the RPC calls to make against one connection
     * @param <T> the result type
     * @return whatever {@code fn} returned
     */
    public <T> T withConnection(Function<Attached, T> fn) {
        Attached a = borrow();
        boolean healthy = false;
        try {
            T result = fn.apply(a);
            healthy = true;
            return result;
        } finally {
            release(a, healthy);
        }
    }

    /**
     * Borrow a connection for the caller to hold across multiple operations.
     * Must be paired with exactly one {@link #release} call.
     *
     * <p>Blocks up to {@link VgiCatalogConfig#connectionAcquireTimeoutMillis()},
     * not forever.
     *
     * @return a connection from the pool
     * @throws RuntimeException if none becomes available within the
     *         configured timeout, or the wait is interrupted
     */
    public Attached borrow() {
        CompletableFuture<Attached> future = borrowAsync();
        try {
            return future.get(config.connectionAcquireTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // Nobody else will ever collect from `future` — if it slipped
            // through despite withdrawal (raced a concurrent release()),
            // reclaim the connection into the pool rather than lose it.
            cancelPendingBorrow(future);
            future.thenAccept(this::offer);
            throw new RuntimeException(new TimeoutException(
                    "timed out after " + config.connectionAcquireTimeoutMillis()
                            + "ms waiting for a pooled VGI worker connection ("
                            + "connections=" + config.connections() + " may be too small for how many "
                            + "calls this query makes concurrently, or the worker may be stuck — raise "
                            + "connection-acquire-timeout-millis or connections if this is expected)"));
        } catch (InterruptedException e) {
            cancelPendingBorrow(future);
            future.thenAccept(this::offer);
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted waiting for a VGI worker connection", e);
        } catch (ExecutionException e) {
            throw e.getCause() instanceof RuntimeException re ? re : new RuntimeException(e.getCause());
        }
    }

    /**
     * Reserve a connection without blocking the calling thread: completes
     * immediately if one is already available, otherwise returns an
     * incomplete future that a later {@link #release} call will complete.
     * Never times out on its own; a caller that needs a bound should race it
     * against a delay of its own choosing.
     *
     * @return a future for a connection from the pool
     */
    public CompletableFuture<Attached> borrowAsync() {
        synchronized (lock) {
            Attached a = available.poll();
            if (a != null) return CompletableFuture.completedFuture(a);
            CompletableFuture<Attached> future = new CompletableFuture<>();
            waiters.add(future);
            return future;
        }
    }

    /**
     * Withdraw an unwanted wait from {@link #waiters}: for a {@link #borrow}
     * caller giving up (timeout, interruption), or a caller whose work was
     * closed/cancelled before {@link #borrowAsync} ever handed it a
     * connection. Without this, an abandoned waiter sits in {@link #waiters}
     * forever — not leaking a real connection (nothing is holding one), but
     * capable of stealing a LATER {@link #release} from whichever active
     * caller actually needs it, since {@link #offer} has no way to tell a
     * live waiter from a dead one.
     *
     * <p>Only removes the entry — does NOT reclaim a connection that slipped
     * through despite the withdrawal (a race against a concurrent {@link
     * #release} completing this exact future in the gap before this call
     * runs). {@link #borrow}'s own callers own that decision for themselves.
     *
     * @param future a future this same client's {@link #borrowAsync} returned
     */
    public void cancelPendingBorrow(CompletableFuture<Attached> future) {
        synchronized (lock) {
            waiters.remove(future);
        }
    }

    /**
     * Return a borrowed connection: to the pool (or straight to a waiting
     * {@link #borrowAsync} caller) if {@code healthy}, or evict, close it,
     * and mint a fresh replacement otherwise (VGI's lockstep framing means a
     * connection a call failed against may be left in an indeterminate wire
     * state — reusing it would corrupt the next call rather than merely
     * fail it).
     *
     * <p>The replacement is what keeps the pool's SIZE stable across a
     * transient failure. If re-attaching itself fails, the worker is
     * presumably genuinely unreachable — that slot is honestly lost rather
     * than retried in a loop that could hang this call.
     *
     * @param a the connection {@link #borrow}/{@link #borrowAsync} returned
     * @param healthy whether every call made against it completed cleanly
     */
    public void release(Attached a, boolean healthy) {
        if (healthy) {
            offer(a);
            return;
        }
        closeQuietly(a);
        all.remove(a);
        try {
            Attached replacement = openAndAttach();
            all.add(replacement);
            offer(replacement);
        } catch (RuntimeException e) {
            // The worker looks genuinely down: nothing to put back. Losing
            // one pool slot here is an honest degradation, not a hang —
            // later borrow() calls still succeed against the remaining
            // connections and only run out if EVERY one has failed the
            // same way.
        }
    }

    /**
     * Hand a connection straight to the longest-waiting {@link #borrowAsync}
     * caller, or park it in {@link #available} if there isn't one.
     * {@code CompletableFuture.complete} runs any callbacks already chained
     * onto that future synchronously on THIS thread unless they were chained
     * with an executor-qualified variant — which is why every caller of
     * {@code borrowAsync} in this connector should chain onward with {@code
     * thenApplyAsync(..., executor())}, never a bare {@code thenApply}: this
     * method runs from inside {@link #release}, called from arbitrary
     * connector threads, and none of them should end up unexpectedly running
     * a DIFFERENT call's RPC.
     */
    private void offer(Attached a) {
        CompletableFuture<Attached> waiter;
        synchronized (lock) {
            waiter = waiters.poll();
            if (waiter == null) {
                available.add(a);
                return;
            }
        }
        if (!waiter.complete(a)) {
            // Lost a race with cancelPendingBorrow discarding this exact
            // waiter between poll() and complete() — it has no taker now,
            // so put it back rather than let it vanish.
            offer(a);
        }
    }

    private Attached openAndAttach() {
        return connect(config);
    }

    /**
     * Open and attach a single, unpooled connection.
     *
     * <p>The static entry point a Spark executor uses directly: a
     * {@code VgiPartitionReader} needs exactly one connection, held for that
     * one task's lifetime, in a JVM that never runs a driver-side {@link
     * VgiWorkerClient} pool at all — pooling {@link VgiCatalogConfig#connections()}
     * connections up front (this class's constructor) would be the wrong
     * shape there. This method — and the transport-opening logic below it —
     * has no other dependency on pool state, so it works identically whether
     * called from here (pool construction/self-healing) or directly from
     * executor code.
     *
     * @param config the catalog configuration to connect and attach with
     * @return the attached connection; the caller owns closing it (via
     *         {@code connection().close()})
     */
    public static Attached connect(VgiCatalogConfig config) {
        String location = config.location();
        if (location.startsWith("http://") || location.startsWith("https://")) {
            return openAndAttachHttp(config, location);
        }
        RpcTransport transport = openTransport(location);
        RpcConnection connection = new RpcConnection(transport);
        VgiService service = connection.proxy(VgiService.class);
        CatalogAttachResult attach = service.catalog_attach(
                CatalogAttachRequest.of(config.catalogName(), null, null, null), null);
        return new Attached(connection, service, attach);
    }

    /**
     * {@code http(s)://} has no {@link RpcTransport} — it's a chain of
     * independent request/response pairs, not a duplex byte stream — so it
     * gets its own connection type ({@link HttpRpcConnection}) entirely,
     * built directly rather than through {@link #openTransport}. Each
     * connection still does its own {@code catalog_attach}, exactly like the
     * byte-stream transports.
     */
    private static Attached openAndAttachHttp(VgiCatalogConfig config, String location) {
        HttpRpcConnection.Builder builder = HttpRpcConnection.builder(location);
        if (config.httpBearerToken() != null) {
            builder.bearerToken(config.httpBearerToken());
        }
        HttpRpcConnection connection = builder.build();
        boolean ok = false;
        try {
            VgiService service = connection.proxy(VgiService.class);
            CatalogAttachResult attach = service.catalog_attach(
                    CatalogAttachRequest.of(config.catalogName(), null, null, null), null);
            Attached a = new Attached(connection, service, attach);
            ok = true;
            return a;
        } finally {
            if (!ok) closeQuietly(connection);
        }
    }

    private static RpcTransport openTransport(String location) {
        if (location.startsWith("unix://")) {
            String path = location.substring("unix://".length());
            return connectUnixSocket(path);
        }
        if (location.startsWith("launch:")) {
            // A launch: location resolves to a warm, shared worker's unix socket (spawning it
            // if none is running yet) and connects to it exactly like a plain unix:// location.
            List<String> argv = LaunchLocationParser.parseArgv(location.substring("launch:".length()));
            String socketPath;
            try {
                socketPath = LauncherClient.launch(LaunchConfig.of(argv));
            } catch (IOException e) {
                throw new UncheckedIOException("failed to launch VGI worker for " + location, e);
            }
            return connectUnixSocket(socketPath);
        }
        if (location.startsWith("tcp://")) {
            URI uri = URI.create(location);
            try {
                Socket socket = new Socket(uri.getHost(), uri.getPort());
                return new TcpSocketTransport(socket);
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "failed to connect to VGI worker at " + uri.getHost() + ":" + uri.getPort(), e);
            }
        }
        // Bare command: run through a shell so the operator's own quoting,
        // env expansion, and PATH lookup behave the way it would on a
        // terminal — matches the DuckDB extension's own LOCATION contract.
        return new SubprocessTransport(List.of("/bin/sh", "-c", location));
    }

    private static RpcTransport connectUnixSocket(String path) {
        try {
            SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX);
            channel.connect(UnixDomainSocketAddress.of(path));
            return new UnixSocketTransport(channel);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to connect to VGI unix socket " + path, e);
        }
    }

    private static void closeQuietly(Attached a) {
        closeQuietly(a.connection());
    }

    private static void closeQuietly(AutoCloseable a) {
        try {
            a.close();
        } catch (Exception ignore) {
            // best-effort — the pool is shrinking either way
        }
    }

    @Override
    public void close() {
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ignore) {
            // the JVM is already shutting down (this IS the hook running) — fine, proceed to close.
        }
        closeAllQuietly();
    }

    private void closeAllQuietly() {
        for (Attached a : all) closeQuietly(a);
        executor.shutdownNow();
    }
}
