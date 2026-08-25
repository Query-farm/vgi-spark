// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.function;

import farm.query.vgi.protocol.InitRequest;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.ClientStreamSession;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.client.VgiWorkerClient;
import farm.query.vgispark.types.ArrowSchemaCodec;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.util.TaskCompletionListener;

/**
 * One bound VGI scalar function call site: already {@code bind()}-called
 * (once, driver-side, by {@link VgiUnboundScalarFunction#bind}), ready to
 * {@code exchange()} one row at a time on whichever executor Spark schedules
 * this call to.
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
 * <h2>What's NOT supported yet</h2>
 *
 * <p>See {@link VgiUnboundScalarFunction}'s own validation: constant
 * ({@code vgi_const}) arguments, named arguments, varargs, {@code any}-typed
 * arguments, a dynamically bind-time-computed return type, and struct/list/map
 * argument or return values. Each makes {@code loadFunction} refuse the
 * function outright with a clear message — not a silent gap.
 */
final class VgiScalarFunction implements ScalarFunction<Object> {

    private final VgiCatalogConfig config;
    private final String functionName;
    private final byte[] bindCallBytes;
    private final byte[] outputSchemaBytes;
    private final byte[] opaqueData;
    private final byte[] argsSchemaBytes;
    private final DataType[] inputTypes;
    private final DataType returnType;
    private final boolean deterministic;

    private transient VgiWorkerClient.Attached connection;
    private transient Schema argsSchema;
    private transient boolean taskListenerRegistered;

    VgiScalarFunction(VgiCatalogConfig config, String functionName, byte[] bindCallBytes,
            byte[] outputSchemaBytes, byte[] opaqueData, byte[] argsSchemaBytes,
            DataType[] inputTypes, DataType returnType, boolean deterministic) {
        this.config = config;
        this.functionName = functionName;
        this.bindCallBytes = bindCallBytes;
        this.outputSchemaBytes = outputSchemaBytes;
        this.opaqueData = opaqueData;
        this.argsSchemaBytes = argsSchemaBytes;
        this.inputTypes = inputTypes;
        this.returnType = returnType;
        this.deterministic = deterministic;
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
        ensureConnection();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(argsSchema(), Allocators.root())) {
            root.allocateNew();
            for (int i = 0; i < inputTypes.length; i++) {
                VgiScalarValueBridge.write(root.getVector(i), inputTypes[i], input, i);
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
        } catch (RuntimeException e) {
            // The connection may be left in an indeterminate wire state after
            // a failed call (same lockstep-framing reasoning as
            // VgiWorkerClient.release) — don't reuse it for a later row.
            closeConnectionQuietly();
            connection = null;
            throw e;
        }
    }

    private Schema argsSchema() {
        if (argsSchema == null) {
            argsSchema = ArrowSchemaCodec.deserializeSchema(argsSchemaBytes);
        }
        return argsSchema;
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
