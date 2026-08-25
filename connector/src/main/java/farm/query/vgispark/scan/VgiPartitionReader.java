// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.scan;

import farm.query.vgi.protocol.InitRequest;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.client.VgiWorkerClient;
import farm.query.vgispark.types.ArrowSchemaCodec;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Redeems one {@link VgiInputPartition} via {@code init()} and drains its
 * producer stream into Spark {@link ColumnarBatch}es.
 *
 * <p>Runs entirely on the executor, on the task thread that owns it: unlike
 * {@code vgi-trino}'s {@code VgiPageSource} (which borrows from a
 * coordinator-side pool shared across many concurrent splits and must
 * therefore acquire its connection asynchronously so as not to block a
 * shared engine thread), this reader opens one unpooled connection via
 * {@link VgiWorkerClient#connect} and holds it for its own lifetime — Spark
 * gives each task its own thread and its own {@link #next()}/{@link #get()}
 * call sequence, so a plain blocking open in the constructor is both simpler
 * and correct here.
 *
 * <h2>Zero-copy reads, and why that changes the batch-reuse contract</h2>
 *
 * <p>Each {@link ColumnarBatch} wraps the tick's {@link VectorSchemaRoot}
 * vectors directly via {@link ArrowColumnVector} — no value is copied out the
 * way {@code vgi-trino}'s {@code Block} builders copy every value into a new
 * heap array. This is safe only because of Spark's own {@link PartitionReader}
 * contract: the engine fully consumes one {@link #get()} result before
 * calling {@link #next()} again, and {@code ClientStreamSession.tick()}'s own
 * contract is the mirror image — the batch it returns "is owned by the
 * reader and reused on the next tick", i.e. valid to read only up to that
 * next call. The two contracts compose correctly (this reader never calls
 * {@code tick()} while Spark still holds the previous batch) but would NOT
 * compose safely if anything here cached a batch across ticks — don't.
 *
 * <p>Also following {@code ClientStreamSession}'s own documented ownership
 * split: this class never calls {@code close()} on an individual {@link
 * AnnotatedBatch} — that root is reader-owned and reused/recycled
 * internally, and closing it here would race the reader's own next read
 * (see {@code AnnotatedBatch}'s and {@code ClientStreamSession.tick()}'s
 * javadoc). Only the whole {@link #session} is closed, once, in {@link #close()}.
 */
public final class VgiPartitionReader implements PartitionReader<ColumnarBatch> {

    private final VgiWorkerClient.Attached connection;
    private final RpcStream<? extends StreamState> session;
    private final Schema arrowSchema;
    private final List<Integer> projectionIds;

    private ColumnarBatch currentBatch;
    private boolean finished;

    /**
     * @param config this catalog's configuration — used only to open this
     *        one dedicated connection, not for pooling
     * @param partition the split to redeem
     * @param tableOutputSchemaBytes the owning table's full (bind-time) Arrow
     *        schema, IPC-encoded
     * @param projectionIds the columns to project, as ordinals into the full
     *        schema, or {@code null} for all of them (v1: always {@code null}
     *        — projection pushdown is a later phase)
     */
    public VgiPartitionReader(VgiCatalogConfig config, VgiInputPartition partition,
            byte[] tableOutputSchemaBytes, List<Integer> projectionIds) {
        this.arrowSchema = ArrowSchemaCodec.deserializeSchema(tableOutputSchemaBytes);
        this.projectionIds = projectionIds;
        this.connection = VgiWorkerClient.connect(config);
        boolean ok = false;
        try {
            InitRequest initRequest = new InitRequest(
                    partition.bindCall(),
                    tableOutputSchemaBytes,
                    partition.bindOpaqueData(),
                    projectionIds,
                    null,           // pushdown_filters — not wired yet
                    null,           // join_keys
                    null,           // phase (producer mode)
                    null,           // execution_id — primary init, worker mints one
                    null,           // init_opaque_data
                    null, null, null, null,     // order-by hint
                    null, null,                 // tablesample hint
                    null,           // finalize_state_id
                    null,           // substream_id
                    partition.token().length == 0 ? null : List.of(partition.token()),
                    null);          // row_limit — not wired yet
            this.session = connection.service().init(initRequest, null);
            ok = true;
        } finally {
            if (!ok) closeConnectionQuietly();
        }
    }

    @Override
    public boolean next() throws IOException {
        if (finished) return false;
        AnnotatedBatch batch;
        try {
            batch = session.tick();
        } catch (NoSuchElementException endOfStream) {
            finished = true;
            currentBatch = null;
            return false;
        } catch (RuntimeException e) {
            throw new IOException("VGI stream read failed", e);
        }
        VectorSchemaRoot root = batch.root();
        List<Field> fields = projectionIds == null ? arrowSchema.getFields() : projectedFields();
        ColumnVector[] columns = new ColumnVector[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            FieldVector vector = root.getVector(fields.get(i).getName());
            columns[i] = new ArrowColumnVector(vector);
        }
        currentBatch = new ColumnarBatch(columns, root.getRowCount());
        return true;
    }

    private List<Field> projectedFields() {
        List<Field> all = arrowSchema.getFields();
        List<Field> out = new java.util.ArrayList<>(projectionIds.size());
        for (int ordinal : projectionIds) out.add(all.get(ordinal));
        return out;
    }

    @Override
    public ColumnarBatch get() {
        return currentBatch;
    }

    @Override
    public void close() throws IOException {
        try {
            session.close();
        } finally {
            closeConnectionQuietly();
        }
    }

    private void closeConnectionQuietly() {
        try {
            connection.connection().close();
        } catch (Exception ignore) {
            // best-effort — this connection is unpooled and being discarded either way
        }
    }
}
