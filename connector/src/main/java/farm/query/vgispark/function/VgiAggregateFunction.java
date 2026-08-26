// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.function;

import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.protocol.AggregateBindRequest;
import farm.query.vgi.protocol.AggregateBindResponse;
import farm.query.vgi.protocol.AggregateDestructorRequest;
import farm.query.vgi.protocol.AggregateFinalizeRequest;
import farm.query.vgi.protocol.AggregateFinalizeResponse;
import farm.query.vgi.protocol.AggregateUpdateRequest;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.client.VgiWorkerClient;
import farm.query.vgispark.types.ArrowSchemaCodec;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.connector.catalog.functions.AggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.util.TaskCompletionListener;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

/**
 * One bound VGI aggregate function call site.
 *
 * <h2>Buffer client-side, call the worker once per GROUP — not once per row</h2>
 *
 * <p>Spark's {@code AggregateFunction<S, R>} SPI is strictly per-row
 * (<code>update(S, InternalRow)</code> called once per input row, within one
 * group's own local state); VGI's own aggregate protocol is group-BATCHED
 * (<code>aggregate_update</code>'s <code>input_batch</code> carries a {@code
 * __vgi_group_id} column and can fold many groups' rows in one call — DuckDB's
 * own vectorized hash-aggregate shape). Calling the worker from {@code
 * update()}/{@code merge()} directly would mean one RPC per ROW, unusable at
 * scale. Instead: {@code update()}/{@code merge()} only buffer raw {@link
 * VgiAggState} — {@code merge} is then plain list concatenation, safe because
 * nothing has asked the worker to compute anything yet — and the real RPC
 * sequence (<code>aggregate_bind</code> once per task, <code>aggregate_update
 * </code>/<code>_finalize</code>/<code>_destructor</code> once per GROUP) only
 * runs in {@link #produceResult}, called once per group. Cost is one worker
 * round trip (four calls: bind is amortized across every group a task
 * processes) per group, not per row.
 *
 * <h2>Lazy per-task connection, mirroring {@code VgiScalarFunction}</h2>
 *
 * <p>{@code aggregate_bind}'s own {@code execution_id} is scoped to ONE
 * connection (the worker keys per-execution state off it), and {@code
 * UnboundFunction.bind(StructType)} runs on the driver, long before any
 * executor task — let alone a connection — exists. So the real bind happens
 * lazily, on the FIRST {@code produceResult} call within a given Spark task,
 * over one unpooled connection opened via {@link VgiWorkerClient#connect}
 * (same reasoning as {@code VgiScalarFunction}: no pool exists on an
 * executor) and reused across every subsequent group that same task
 * processes, closed via a {@link TaskContext} completion listener.
 *
 * <h2>Group ids are invented locally, not assigned by Spark</h2>
 *
 * <p>Spark's SPI has no group-id concept at all — {@code produceResult} sees
 * only that one group's own buffered state. Since every group here gets its
 * OWN self-contained bind→update→finalize→destructor sequence (never sharing
 * {@code __vgi_group_id} with another group), any locally-unique id works; a
 * simple per-instance counter is used.
 */
final class VgiAggregateFunction implements AggregateFunction<VgiAggState, Object> {

    private final VgiCatalogConfig config;
    private final String schemaName;
    private final String functionName;
    private final byte[] argsSchemaBytes;
    private final DataType[] inputTypes;
    private final DataType returnType;
    private final boolean deterministic;

    private transient VgiWorkerClient.Attached connection;
    private transient byte[] executionId;
    private transient byte[] outputSchemaBytes;
    private transient Schema argsSchema;
    private transient boolean taskListenerRegistered;
    private transient long nextGroupId;

    VgiAggregateFunction(VgiCatalogConfig config, String schemaName, String functionName, byte[] argsSchemaBytes,
            DataType[] inputTypes, DataType returnType, boolean deterministic) {
        this.config = config;
        this.schemaName = schemaName;
        this.functionName = functionName;
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
    public VgiAggState newAggregationState() {
        return new VgiAggState();
    }

    @Override
    public VgiAggState update(VgiAggState state, org.apache.spark.sql.catalyst.InternalRow input) {
        state.rows.add(input.copy());
        return state;
    }

    @Override
    public VgiAggState merge(VgiAggState left, VgiAggState right) {
        left.rows.addAll(right.rows);
        return left;
    }

    @Override
    public Object produceResult(VgiAggState state) {
        ensureBound();
        long groupId = nextGroupId++;
        try {
            sendUpdate(state, groupId);
            byte[] resultBatch = sendFinalize(groupId);
            return decodeResult(resultBatch);
        } catch (RuntimeException e) {
            // Same lockstep-framing reasoning as VgiScalarFunction/VgiWorkerClient
            // .release: a failed call may leave the wire in an indeterminate
            // state — don't reuse this connection for a later group.
            closeConnectionQuietly();
            connection = null;
            executionId = null;
            throw e;
        } finally {
            sendDestructorQuietly(groupId);
        }
    }

    private void sendUpdate(VgiAggState state, long groupId) {
        if (state.rows.isEmpty()) {
            // Nothing accumulated (e.g. every input value was NULL and this
            // aggregate doesn't buffer nulls — see update() above, which does
            // buffer them; a genuinely empty group only happens for an
            // ungrouped aggregate over zero rows). Skip aggregate_update
            // entirely — finalize below still runs and the worker's own
            // finalizeEmpty path (AggregateRunner's own documented fallback
            // for a group with no saved state) produces the right "no rows"
            // result (e.g. NULL for SUM, 0 for COUNT) without this connector
            // needing to know which is which per function.
            return;
        }
        Schema updateSchema = updateBatchSchema();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(updateSchema, Allocators.root())) {
            root.allocateNew();
            int rowCount = state.rows.size();
            for (int col = 0; col < inputTypes.length; col++) {
                FieldVector vector = root.getVector(col);
                for (int row = 0; row < rowCount; row++) {
                    VgiScalarValueBridge.writeAt(vector, inputTypes[col], state.rows.get(row), col, row);
                }
            }
            BigIntVector gidVector = (BigIntVector) root.getVector(inputTypes.length);
            // setSafe, not set: a plain set() never grows the vector past
            // whatever allocateNew()'s default initial capacity was, and a
            // large ungrouped/high-cardinality aggregate (state.rows in the
            // hundreds of thousands) blows past that — confirmed live, a real
            // IndexOutOfBoundsException at row 504 against range(100000).
            for (int row = 0; row < rowCount; row++) gidVector.setSafe(row, groupId);
            for (FieldVector v : root.getFieldVectors()) v.setValueCount(rowCount);
            root.setRowCount(rowCount);

            byte[] inputBatch = writeSingleBatch(root);
            AggregateUpdateRequest request = new AggregateUpdateRequest(
                    functionName, executionId, inputBatch, connection.handle(), schemaName);
            connection.service().aggregate_update(request);
        }
    }

    private byte[] sendFinalize(long groupId) {
        Schema groupIdSchema = new Schema(List.of(
                new Field("group_id", new FieldType(false, new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true), null), null)));
        byte[] groupIdsBatch;
        try (VectorSchemaRoot root = VectorSchemaRoot.create(groupIdSchema, Allocators.root())) {
            root.allocateNew();
            ((BigIntVector) root.getVector(0)).set(0, groupId);
            root.getVector(0).setValueCount(1);
            root.setRowCount(1);
            groupIdsBatch = writeSingleBatch(root);
        }
        AggregateFinalizeRequest request = new AggregateFinalizeRequest(
                functionName, executionId, groupIdsBatch, outputSchemaBytes, connection.handle(), schemaName);
        AggregateFinalizeResponse response = connection.service().aggregate_finalize(request);
        return response.result_batch();
    }

    private void sendDestructorQuietly(long groupId) {
        if (connection == null || executionId == null) return; // a prior failure already tore this down
        try {
            Schema groupIdSchema = new Schema(List.of(
                    new Field("group_id", new FieldType(false, new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true), null), null)));
            byte[] groupIdsBatch;
            try (VectorSchemaRoot root = VectorSchemaRoot.create(groupIdSchema, Allocators.root())) {
                root.allocateNew();
                ((BigIntVector) root.getVector(0)).set(0, groupId);
                root.getVector(0).setValueCount(1);
                root.setRowCount(1);
                groupIdsBatch = writeSingleBatch(root);
            }
            AggregateDestructorRequest request = new AggregateDestructorRequest(
                    functionName, executionId, groupIdsBatch, connection.handle(), schemaName);
            connection.service().aggregate_destructor(request);
        } catch (RuntimeException ignore) {
            // Best-effort cleanup — a group's worker-side state outliving this
            // call is a resource leak on the worker, never a correctness
            // problem for the result already returned by finalize above.
        }
    }

    private Object decodeResult(byte[] resultBatch) {
        if (resultBatch == null || resultBatch.length == 0) {
            return null;
        }
        try (IpcStreamReader r = new IpcStreamReader(new ByteArrayInputStream(resultBatch), Allocators.root())) {
            if (r.readNextBatch() == null) return null;
            VectorSchemaRoot root = r.root();
            if (root.getRowCount() == 0) return null;
            return VgiScalarValueBridge.read(root.getVector(0), returnType, 0);
        } catch (Exception e) {
            throw new RuntimeException(functionName + ": failed to decode aggregate_finalize result", e);
        }
    }

    /** {@code argsSchema}'s fields plus a trailing non-nullable int64 {@code __vgi_group_id}. */
    private Schema updateBatchSchema() {
        List<Field> fields = new ArrayList<>(argsSchema().getFields());
        fields.add(new Field("__vgi_group_id",
                new FieldType(false, new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true), null), null));
        return new Schema(fields);
    }

    private Schema argsSchema() {
        if (argsSchema == null) {
            Schema decoded = ArrowSchemaCodec.deserializeSchema(argsSchemaBytes);
            argsSchema = decoded == null ? new Schema(List.of()) : decoded;
        }
        return argsSchema;
    }

    private void ensureBound() {
        if (executionId != null) return;
        if (connection == null) {
            connection = VgiWorkerClient.connect(config);
            if (!taskListenerRegistered) {
                TaskContext ctx = TaskContext.get();
                if (ctx != null) {
                    ctx.addTaskCompletionListener((TaskCompletionListener) c -> closeConnectionQuietly());
                }
                taskListenerRegistered = true;
            }
        }
        byte[] emptyArguments = ArgumentsEncoder.builder().encode();
        AggregateBindRequest request = new AggregateBindRequest(
                functionName, emptyArguments, argsSchemaBytes, null, null, connection.handle(), schemaName);
        AggregateBindResponse response = connection.service().aggregate_bind(request);
        executionId = response.execution_id();
        outputSchemaBytes = response.output_schema();
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

    private static byte[] writeSingleBatch(VectorSchemaRoot root) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return baos.toByteArray();
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
