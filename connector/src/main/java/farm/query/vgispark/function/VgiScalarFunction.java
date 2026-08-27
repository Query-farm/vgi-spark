// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.function;

import farm.query.vgi.SettingSpec;
import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.ScalarValue;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.ClientStreamSession;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.client.VgiWorkerClient;
import farm.query.vgispark.settings.SettingSpecDecoder;
import farm.query.vgispark.types.ArrowSchemaCodec;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.util.TaskCompletionListener;

import java.util.Arrays;
import java.util.Map;

/**
 * One bound VGI scalar function call site: ready to {@code exchange()} one
 * row at a time on whichever executor Spark schedules this call to.
 *
 * <h2>One RPC round trip per row — a known, accepted cost</h2>
 *
 * <p>Both Spark's {@code ScalarFunction#produceResult(InternalRow)} and
 * Trino's connector-function SPI are row-at-a-time; neither offers a batched
 * evaluation path a connector can hook into. {@code vgi-trino}'s own {@code
 * VgiScalarFunctions} accepts exactly this cost (one borrow-init-exchange-close
 * per call) — this class mirrors that, with one improvement Spark's execution
 * model (unlike Trino's) makes available: rather than borrowing and releasing
 * a POOLED connection per call (there is no pool on an executor — see {@code
 * VgiPartitionReader}'s own note on why), this class opens ONE unpooled
 * connection lazily on first use and reuses it for every subsequent call
 * within the same Spark task, closed via a {@link TaskContext} completion
 * listener when the task ends.
 *
 * <h2>Two bind modes: eager (driver, once) vs. lazy-const (executor, per
 * observed value)</h2>
 *
 * <p>{@link VgiUnboundScalarFunction#bind} does a real {@code bind()} RPC on
 * the driver, once per call site, and this class simply replays the same
 * {@code bindCallBytes}/{@code opaqueData} at every later {@code init()} —
 * <strong>unless the function has at least one {@code vgi_const} argument</strong>.
 * A {@code vgi_const} argument's actual VALUE (as opposed to its type) is only
 * knowable once a real {@link InternalRow} exists — Spark's {@code
 * UnboundFunction.bind(StructType)} sees types only, never values (the exact
 * limitation {@code vgi-trino}'s own {@code BindCache} was built to work
 * around, for the identical reason — see its javadoc). So for a const-bearing
 * function, {@link VgiUnboundScalarFunction#bind} does NO RPC at all; this
 * class instead binds lazily, right here in {@link #produceResult}, keyed on
 * the actual observed const value(s) — cached in a single-slot, per-task,
 * per-instance cache (simpler than {@code vgi-trino}'s shared LRU across
 * Drivers, since Spark already scopes one {@code VgiScalarFunction} instance
 * to one call site): the FIRST row triggers a real bind, and every later row
 * whose const argument(s) still equal what was last observed reuses it;
 * a genuinely per-row-VARYING "constant" degrades to "no caching benefit,
 * one bind() RPC per row" — never wrong results, just no speedup, the same
 * honest tradeoff {@code vgi-trino}'s {@code BindCache} documents.
 *
 * <h2>What's NOT supported yet</h2>
 *
 * <p>See {@link VgiUnboundScalarFunction}'s own validation: named arguments,
 * a return type that depends on a const argument's actual VALUE (as opposed
 * to any argument's TYPE, which IS supported — see {@link
 * VgiUnboundScalarFunction#bind}), and a {@code map}-typed argument or
 * return value (struct/list ARE supported, recursively — see {@link
 * VgiScalarValueBridge}). Each makes {@code loadFunction} refuse the
 * function outright with a clear message — not a silent gap.
 */
final class VgiScalarFunction implements ScalarFunction<Object> {

    private final VgiCatalogConfig config;
    private final String schemaName;      // only used by the lazy-const bind path
    private final String functionName;
    private final byte[] driverBindCallBytes;     // non-null only in eager mode
    private final byte[] driverOutputSchemaBytes; // non-null only in eager mode
    private final byte[] driverOpaqueData;        // non-null only in eager mode
    private final byte[] rowInputSchemaBytes;
    private final DataType[] inputTypes;          // full, all signature positions
    private final int[] rowArgIndices;            // signature positions of non-const args, in row-schema column order
    private final int[] constArgIndices;          // signature positions of vgi_const args; empty in eager mode
    private final Field[] resolvedFields;         // full, all signature positions; only read for const args
    private final DataType returnType;
    private final boolean deterministic;

    private transient VgiWorkerClient.Attached connection;
    private transient Schema rowInputSchema;
    private transient boolean taskListenerRegistered;

    // Lazy-const-bind cache — see this class's own javadoc. Unused (stays
    // null/empty) whenever constArgIndices.length == 0.
    private transient byte[] cachedBindCallBytes;
    private transient byte[] cachedOutputSchemaBytes;
    private transient byte[] cachedOpaqueData;
    private transient Object[] cachedConstValues;

    private VgiScalarFunction(VgiCatalogConfig config, String schemaName, String functionName,
            byte[] driverBindCallBytes, byte[] driverOutputSchemaBytes, byte[] driverOpaqueData,
            byte[] rowInputSchemaBytes, DataType[] inputTypes, int[] rowArgIndices, int[] constArgIndices,
            Field[] resolvedFields, DataType returnType, boolean deterministic) {
        this.config = config;
        this.schemaName = schemaName;
        this.functionName = functionName;
        this.driverBindCallBytes = driverBindCallBytes;
        this.driverOutputSchemaBytes = driverOutputSchemaBytes;
        this.driverOpaqueData = driverOpaqueData;
        this.rowInputSchemaBytes = rowInputSchemaBytes;
        this.inputTypes = inputTypes;
        this.rowArgIndices = rowArgIndices;
        this.constArgIndices = constArgIndices;
        this.resolvedFields = resolvedFields;
        this.returnType = returnType;
        this.deterministic = deterministic;
    }

    /** No {@code vgi_const} arguments — already bound once, on the driver. */
    static VgiScalarFunction eager(VgiCatalogConfig config, String functionName, byte[] bindCallBytes,
            byte[] outputSchemaBytes, byte[] opaqueData, byte[] rowInputSchemaBytes, DataType[] inputTypes,
            int[] rowArgIndices, DataType returnType, boolean deterministic) {
        return new VgiScalarFunction(config, null, functionName, bindCallBytes, outputSchemaBytes, opaqueData,
                rowInputSchemaBytes, inputTypes, rowArgIndices, new int[0], null, returnType, deterministic);
    }

    /** At least one {@code vgi_const} argument — bound lazily, per observed value, in {@link #produceResult}. */
    static VgiScalarFunction lazyConst(VgiCatalogConfig config, String schemaName, String functionName,
            DataType[] inputTypes, Field[] resolvedFields, int[] rowArgIndices, int[] constArgIndices,
            byte[] rowInputSchemaBytes, DataType returnType, boolean deterministic) {
        return new VgiScalarFunction(config, schemaName, functionName, null, null, null,
                rowInputSchemaBytes, inputTypes, rowArgIndices, constArgIndices, resolvedFields, returnType,
                deterministic);
    }

    @Override
    public String name() {
        return functionName;
    }

    @Override
    public DataType[] inputTypes() {
        return inputTypes;
    }

    @Override
    public DataType resultType() {
        return returnType;
    }

    @Override
    public boolean isDeterministic() {
        return deterministic;
    }

    @Override
    public Object produceResult(InternalRow input) {
        // One bounded retry, not zero: this same connection is reused across
        // every produceResult call this bound function's whole lifetime makes
        // (see ensureConnection's own javadoc) — deliberately, to avoid a
        // fresh connection per scalar-UDF row during normal task execution.
        // Spark's own ConstantFolding optimizer rule evaluates an all-literal
        // expression via repeated produceResult calls on that ONE instance —
        // confirmed empirically (a per-call identity-hashed log during
        // diagnosis showed the SAME instance called three times in a row for
        // a single constant SELECT) — and after enough back-to-back init()
        // exchanges on the same connection without an intervening real batch
        // read in between, the wire desyncs: NoSuchElementException from
        // ClientStreamSession, with no error and no crash on the worker side
        // (confirmed by inspecting its stderr directly), i.e. a framing bug
        // in reusing one connection for rapid repeated exchanges — likely in
        // vgi-rpc-java's own stream-close/drain sequencing, not this class.
        // A real fix belongs there; this retry is the same self-healing
        // philosophy VgiWorkerClient.release already applies to its pool
        // (discard a connection left in an indeterminate state, don't trust
        // it again) — a fresh connection has no accumulated wire state and
        // reliably succeeds.
        RuntimeException lastFailure = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            ensureConnection();
            try {
                byte[] bindCallBytes;
                byte[] outputSchemaBytes;
                byte[] opaqueData;
                if (constArgIndices.length == 0) {
                    bindCallBytes = driverBindCallBytes;
                    outputSchemaBytes = driverOutputSchemaBytes;
                    opaqueData = driverOpaqueData;
                } else {
                    Object[] observed = new Object[constArgIndices.length];
                    for (int i = 0; i < constArgIndices.length; i++) {
                        int sigIndex = constArgIndices[i];
                        observed[i] = VgiScalarValueBridge.readPlainValue(input, sigIndex, inputTypes[sigIndex]);
                    }
                    if (cachedConstValues == null || !Arrays.equals(observed, cachedConstValues)) {
                        rebindWithConstValues(observed);
                    }
                    bindCallBytes = cachedBindCallBytes;
                    outputSchemaBytes = cachedOutputSchemaBytes;
                    opaqueData = cachedOpaqueData;
                }

                try (VectorSchemaRoot root = VectorSchemaRoot.create(rowInputSchema(), Allocators.root())) {
                    root.allocateNew();
                    for (int rowPos = 0; rowPos < rowArgIndices.length; rowPos++) {
                        int sigIndex = rowArgIndices[rowPos];
                        VgiScalarValueBridge.writeAt(root.getVector(rowPos), inputTypes[sigIndex], input, sigIndex, 0);
                    }
                    for (FieldVector v : root.getFieldVectors()) v.setValueCount(1);
                    root.setRowCount(1);

                    InitRequest initRequest = new InitRequest(
                            bindCallBytes, outputSchemaBytes, opaqueData,
                            null, null, null, null, null, null,
                            null, null, null, null,
                            null, null,
                            null, null, null, null);
                    RpcStream<? extends StreamState> stream = connection.service().init(initRequest, null);
                    ClientStreamSession<?> session = (ClientStreamSession<?>) stream;
                    AnnotatedBatch out = session.exchange(new AnnotatedBatch(root, null));
                    try {
                        // Every VGI scalar function's output schema has exactly one column, named "result".
                        FieldVector resultVector = out.root().getVector("result");
                        return VgiScalarValueBridge.read(resultVector, returnType, 0);
                    } finally {
                        session.close();
                    }
                }
            } catch (RuntimeException e) {
                // The connection may be left in an indeterminate wire state after
                // a failed call (same lockstep-framing reasoning as
                // VgiWorkerClient.release) — don't reuse it, or any bind cached
                // against it, for a later row (or the retry below).
                closeConnectionQuietly();
                connection = null;
                cachedBindCallBytes = null;
                cachedConstValues = null;
                lastFailure = e;
            }
        }
        throw lastFailure;
    }

    /**
     * Real {@code bind()} RPC with {@code observed}'s values embedded in
     * {@code BindRequest.arguments} — see this class's own javadoc for when
     * and why this runs (lazily, per distinct observed const value).
     */
    private void rebindWithConstValues(Object[] observed) {
        ArgumentsEncoder encoder = ArgumentsEncoder.builder();
        for (int i = 0; i < constArgIndices.length; i++) {
            ArrowType arrowType = resolvedFields[constArgIndices[i]].getType();
            Object value = observed[i];
            // v1 scope: a named argument is refused entirely at discovery
            // (VgiUnboundScalarFunction.tryBuild) — every const argument
            // reaching here is positional.
            encoder.positional(value == null ? ScalarValue.ofNull(arrowType) : ScalarValue.of(arrowType, value));
        }
        byte[] argumentsBytes = encoder.encode();
        Map<String, SettingSpec> declared = new java.util.LinkedHashMap<>();
        for (SettingSpec spec : SettingSpecDecoder.decodeAll(connection.attach().settings())) {
            declared.put(spec.name(), spec);
        }
        byte[] settingsBytes = VgiUnboundScalarFunction.currentSettingsBytes(declared);
        BindRequest bindRequest = new BindRequest(
                functionName, argumentsBytes, "SCALAR", rowInputSchemaBytes, settingsBytes,
                null,                 // secrets — deferred, see VgiUnboundScalarFunction's own class javadoc
                connection.handle(),  // attach_opaque_data
                null,                 // transaction_opaque_data
                false,                // resolved_secrets_provided
                null, null,           // at_unit / at_value — not applicable to scalars
                null, null,           // copy_from / copy_to
                schemaName);
        BindResponse bound = connection.service().bind(bindRequest, null);
        cachedBindCallBytes = RecordCodec.serializeToBytes(bindRequest);
        cachedOutputSchemaBytes = bound.output_schema();
        cachedOpaqueData = bound.opaque_data();
        cachedConstValues = observed;
    }

    private Schema rowInputSchema() {
        if (rowInputSchema == null) {
            rowInputSchema = ArrowSchemaCodec.deserializeSchema(rowInputSchemaBytes);
        }
        return rowInputSchema;
    }

    private void ensureConnection() {
        if (connection != null) return;
        connection = VgiWorkerClient.connect(config);
        if (!taskListenerRegistered) {
            TaskContext ctx = TaskContext.get();
            if (ctx != null) {
                ctx.addTaskCompletionListener((TaskCompletionListener) context -> closeConnectionQuietly());
            }
            // Set regardless of whether a TaskContext was available (e.g. this
            // function is evaluated from a non-task thread, such as a
            // constant-folding pass): either way, don't try to register a
            // second listener on a later ensureConnection() call after a
            // reconnect.
            taskListenerRegistered = true;
        }
    }

    private void closeConnectionQuietly() {
        VgiWorkerClient.Attached c = connection;
        if (c == null) return;
        try {
            c.connection().close();
        } catch (Exception ignore) {
            // best-effort — this connection is unpooled and being discarded either way
        }
    }
}
