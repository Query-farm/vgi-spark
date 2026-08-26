// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgispark.procedure;

import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.ScalarValue;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgispark.VgiCatalogConfig;
import farm.query.vgispark.VgiTable;
import farm.query.vgispark.branch.VgiScanBranch;
import farm.query.vgispark.client.VgiWorkerClient;
import farm.query.vgispark.scan.VgiScan;
import farm.query.vgispark.scan.VgiScanBuilder;
import farm.query.vgispark.types.ArrowSchemaCodec;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure;
import org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter;
import org.apache.spark.sql.connector.catalog.procedures.SimpleProcedure;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.LocalScan;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A VGI table function ({@code FunctionInfo.function_type() == "TABLE"}),
 * exposed as a Spark stored procedure — {@code CALL catalog.schema.func(args)}.
 *
 * <p><strong>Why {@code CALL}, not {@code FROM func(args)}/{@code LATERAL
 * func(...)}</strong>: confirmed by direct inspection of Spark 4.2.0's own
 * grammar and jars (not assumed) that there is no path from a {@code
 * TableCatalog}/{@code FunctionCatalog} plugin into {@code FROM func(args)}
 * resolution — {@code org.apache.spark.sql.connector.catalog.functions} has
 * no {@code TableFunction} type at all, and {@code FROM}'s {@code
 * tableValuedFunction} grammar production resolves only against Spark's
 * session-level {@code TableFunctionRegistry} (builtins plus SQL-body {@code
 * CREATE FUNCTION ... RETURNS TABLE} macros), with no connector hook. That
 * ceiling is real and this class does not attempt to work around it — see
 * {@code docs/ROADMAP.md}'s "Won't implement" section. What Spark 4.1+ DOES
 * offer is {@code ProcedureCatalog} (SPARK-44167): {@link BoundProcedure
 * #call} returns {@code Iterator<Scan>} — literally the same {@link Scan}
 * interface {@code VgiScan} already implements — so a VGI table function can
 * be exposed as a REAL, SQL-level, real-result-set call. The catch,
 * confirmed by fetching Spark's actual {@code SqlBaseParser.g4}: {@code CALL}
 * is reachable only from the top-level {@code statement} rule, never from
 * {@code relation} — so this gives the plain {@code SELECT * FROM func(args)}
 * shape as a (differently-spelled) real statement, never a correlated {@code
 * LATERAL func(t.col)} per-row call. That specific capability remains
 * blocked pending Spark itself adding a table-function catalog SPI.
 *
 * <p>Implements {@link SimpleProcedure} (unbound and bound in one — VGI table
 * function parameters are statically typed from {@code FunctionInfo
 * .arguments}, nothing about them depends on the call site's coerced input
 * types), so {@link #bind} is the trivial identity.
 *
 * <p>v1 scope, mirroring {@code VgiUnboundScalarFunction}'s but LOOSER on one
 * point: every argument must be positional, non-vararg, non-{@code any}-typed,
 * non-{@code table}-typed, of a concrete type this class can read out of an
 * {@link InternalRow}. Unlike scalar functions, a {@code vgi_const} argument
 * is NOT refused — {@link BoundProcedure#call} receives the call site's real
 * argument VALUES (an {@link InternalRow}), not just types, so the "Spark
 * only tells you the type, not the value" limitation that blocks const
 * arguments for scalar functions (see that class's own javadoc) simply
 * doesn't apply here.
 */
public final class VgiUnboundTableProcedure implements SimpleProcedure {

    private final VgiWorkerClient client;
    private final VgiCatalogConfig config;
    private final String schemaName;
    private final FunctionInfo info;
    private final List<Field> argFields;
    // Parallel to argFields — the Spark type each argument reads as from the
    // InternalRow call() receives. NOT the same thing as each field's own
    // ArrowType (used separately, at encode time, for ScalarValue.of): Spark
    // reads InternalRow columns by SPARK type, VGI's wire encodes values by
    // ARROW type, and the two must each use their own type per argument.
    private final DataType[] paramTypes;
    private final ProcedureParameter[] parameters;
    private final boolean deterministic;

    private VgiUnboundTableProcedure(VgiWorkerClient client, VgiCatalogConfig config, String schemaName,
            FunctionInfo info, List<Field> argFields, DataType[] paramTypes, ProcedureParameter[] parameters,
            boolean deterministic) {
        this.client = client;
        this.config = config;
        this.schemaName = schemaName;
        this.info = info;
        this.argFields = argFields;
        this.paramTypes = paramTypes;
        this.parameters = parameters;
        this.deterministic = deterministic;
    }

    @Override
    public String name() {
        return info.name();
    }

    @Override
    public String description() {
        return info.description() == null ? "" : info.description();
    }

    @Override
    public ProcedureParameter[] parameters() {
        return parameters;
    }

    @Override
    public boolean isDeterministic() {
        return deterministic;
    }

    @Override
    public BoundProcedure bind(StructType inputType) {
        return this; // SimpleProcedure default would do the same; explicit for clarity — see class javadoc
    }

    @Override
    public Iterator<Scan> call(InternalRow args) {
        if (args.numFields() != argFields.size()) {
            // Defensive — Spark's own analyzer should already have enforced arity
            // via parameters() before ever reaching call().
            throw new IllegalStateException(schemaName + "." + info.name() + ": expected "
                    + argFields.size() + " argument(s), call() received " + args.numFields());
        }
        ArgumentsEncoder encoder = ArgumentsEncoder.builder();
        for (int i = 0; i < argFields.size(); i++) {
            Field field = argFields.get(i);
            ScalarValue value = args.isNullAt(i)
                    ? ScalarValue.ofNull(field.getType())
                    : ScalarValue.of(field.getType(), ProcedureArgumentBridge.read(args, i, paramTypes[i]));
            // Wire form must match this SPECIFIC argument's own declared kind
            // (argument_spec.py: positional args first by index, named ones
            // after, each stamped {vgi_arg: named}) — confirmed the hard way
            // against a live worker: split_sequence's own args are declared
            // name-position ("n", "splits"), and encoding them as positional_N
            // regardless produced a worker-side "Argument 'n': not found"
            // KeyError. Spark's CALL still supplies every argument by the
            // call site's POSITIONAL order (parameters()' own declared order,
            // which matches argFields' order exactly) regardless of which wire
            // form each one needs — this mirrors what DuckDB's own client-side
            // binder must be doing to make positional split_sequence(30, 6)
            // calls work at all against a name-position-declared function.
            Map<String, String> md = field.getMetadata();
            if (md != null && "named".equals(md.get("vgi_arg"))) {
                encoder.named(field.getName(), value);
            } else {
                encoder.positional(value);
            }
        }
        byte[] encodedArgs = encoder.encode();

        // A first bind() here to learn the REAL, call-site-specific output
        // schema (table functions, unlike v1 scalar functions, are allowed a
        // dynamic on_bind-computed return shape — this connector already has
        // the real argument values, so there's no reason to refuse it the way
        // VgiUnboundScalarFunction does). VgiScan performs its OWN separate
        // bind() when planInputPartitions() runs later (it always binds fresh
        // from a branch's functionName+arguments, never a pre-existing
        // BindResponse) — a second, redundant bind call to the same function
        // with the same arguments. Accepted for v1: correct, just not maximally
        // efficient; avoiding it would mean threading a pre-bound BindResponse
        // through VgiScan/VgiScanBuilder, a larger refactor not attempted here.
        BindRequest bindRequest = new BindRequest(
                info.name(), encodedArgs, "TABLE",
                null,           // input_schema — producer-mode table function
                null, null,     // settings, secrets — not threaded through for procedures yet
                null, null,     // attach_opaque_data / transaction_opaque_data — filled in below
                false,
                null, null,     // at_unit / at_value — not applicable to a direct call
                null, null,     // copy_from / copy_to
                // NOT schemaName: mirrors VgiScan.planBranchPartitions's own
                // choice (see its comment) — null lets the worker's dispatcher
                // search every schema by name, rather than asserting this one
                // specific schema.
                null);
        BindResponse bound = client.withConnection(a -> a.service().bind(
                withAttachHandle(bindRequest, a.handle()), null));

        VgiTable syntheticTable = new VgiTable(schemaName, info.name(),
                List.of(new VgiScanBranch(info.name(), encodedArgs)),
                bound.output_schema(), null, null, null, List.of(),
                client, config);
        // A plain VgiScan (Batch/InputPartition-based, meant to run distributed
        // across executors) is NOT accepted here — confirmed live: Spark 4.2's
        // CALL/procedure execution path currently throws "[INTERNAL_ERROR] Only
        // local scans are temporarily supported as procedure output" for
        // anything but a LocalScan. ("Temporarily" is Spark's own wording —
        // this looks like a real, current version limitation of the still-new
        // ProcedureCatalog SPI (SPARK-44167, shipped 4.1), not a design choice
        // this connector could route around; revisit once a future Spark
        // release lifts it, since VgiScan's normal distributed path would then
        // need no changes at all to plug in directly.) So v1 drains the scan
        // EAGERLY, right here on the driver, into a LocalScan wrapping real
        // materialized rows — correct for the utility/admin-call sizes CALL is
        // realistically used for, but not a distributed read; a huge table
        // function's result set would need to fit in driver memory. Reuses
        // VgiScan's own plan/split/partition-reader pipeline unchanged (see
        // materializeRows) rather than duplicating it.
        VgiScan scan = (VgiScan) new VgiScanBuilder(client, config, syntheticTable).build();
        InternalRow[] rows = materializeRows(scan);
        LocalScan localScan = new LocalScan() {
            @Override public InternalRow[] rows() { return rows; }
            @Override public StructType readSchema() { return scan.readSchema(); }
        };
        return Collections.singletonList((Scan) localScan).iterator();
    }

    /**
     * Drain every partition of {@code scan} synchronously, on the calling
     * (driver) thread, into a flat row array — see {@link #call}'s own
     * javadoc for why this eager materialization is necessary at all.
     * {@code VgiPartitionReaderFactory} only supports columnar reads (see its
     * own {@code createReader} — it throws), so this reads {@link
     * ColumnarBatch}es and unpacks each one via {@code rowIterator()}, {@code
     * copy()}-ing every row before the batch's backing vectors are reused or
     * closed by the next {@code next()} call.
     */
    private static InternalRow[] materializeRows(VgiScan scan) {
        List<InternalRow> rows = new ArrayList<>();
        PartitionReaderFactory factory = scan.createReaderFactory();
        for (InputPartition partition : scan.planInputPartitions()) {
            try (PartitionReader<ColumnarBatch> reader = factory.createColumnarReader(partition)) {
                while (reader.next()) {
                    ColumnarBatch batch = reader.get();
                    Iterator<InternalRow> it = batch.rowIterator();
                    while (it.hasNext()) {
                        rows.add(it.next().copy());
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "table function call failed while reading partition " + partition, e);
            }
        }
        return rows.toArray(new InternalRow[0]);
    }

    private static BindRequest withAttachHandle(BindRequest request, byte[] attachHandle) {
        return new BindRequest(
                request.function_name(), request.arguments(), request.function_type(),
                request.input_schema(), request.settings(), request.secrets(),
                attachHandle, request.transaction_opaque_data(), request.resolved_secrets_provided(),
                request.at_unit(), request.at_value(), request.copy_from(), request.copy_to(),
                request.schema_name());
    }

    /**
     * Build the procedure for {@code info}, or throw a clear {@link
     * UnsupportedOperationException} naming exactly what isn't supported yet
     * — mirrors {@code VgiUnboundScalarFunction.tryBuild}'s own contract and
     * refusal style.
     */
    public static VgiUnboundTableProcedure tryBuild(
            VgiWorkerClient client, VgiCatalogConfig config, String schemaName, FunctionInfo info) {
        String context = schemaName + "." + info.name();
        Schema argsSchema = ArrowSchemaCodec.deserializeSchema(info.arguments());
        List<Field> argFields = argsSchema == null ? List.of() : argsSchema.getFields();
        ProcedureParameter[] parameters = new ProcedureParameter[argFields.size()];
        DataType[] paramTypes = new DataType[argFields.size()];
        for (int i = 0; i < argFields.size(); i++) {
            Field field = argFields.get(i);
            Map<String, String> md = field.getMetadata();
            String vgiType = md == null ? null : md.get("vgi_type");
            if ("table".equals(vgiType)) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' is TABLE-typed — this is a table-in-out function, not a plain table"
                        + " function; not supported yet (see docs/ROADMAP.md)");
            }
            if ("any".equals(vgiType)) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' is any-typed (a dynamic/generic type resolved per call site) — not "
                        + "supported yet");
            }
            // NOT refusing vgi_arg=="named" here, unlike VgiUnboundScalarFunction:
            // confirmed live (against the real split_sequence fixture) that
            // {b"vgi_arg": b"named"} does NOT mean "keyword-only, no positional
            // slot" the way that check assumed — argument_spec.py's own
            // encoder emits it whenever an Arg(name, ...) declares a STRING
            // position (the common case for table-function args, per its own
            // "named arguments follow positional ones" ordering), yet DuckDB's
            // real, documented calling convention (split_sequence(30, 6)) still
            // calls every such argument POSITIONALLY, by declared field order.
            // Every argument here is called by argFields' own order regardless
            // of this marker — matching DuckDB's own behavior, confirmed
            // rather than assumed.
            if (md != null && "true".equals(md.get("vgi_varargs"))) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' is variadic (vgi_varargs) — not supported yet");
            }
            DataType type;
            try {
                type = farm.query.vgispark.types.VgiTypeMapping.toSparkType(field);
            } catch (UnsupportedOperationException e) {
                throw new UnsupportedOperationException(
                        context + ": argument '" + field.getName() + "': " + e.getMessage(), e);
            }
            if (!ProcedureArgumentBridge.isSupported(type)) {
                throw new UnsupportedOperationException(context + ": argument '" + field.getName()
                        + "' has type " + type + ", which table-function calls don't bridge yet "
                        + "(struct/list/map/decimal/date/timestamp arguments are a later phase)");
            }
            parameters[i] = ProcedureParameter.in(field.getName(), type).build();
            paramTypes[i] = type;
        }

        // Unlike scalar functions, no output-schema validation here at all: a
        // table function's real output schema comes from BindResponse at
        // call() time (with real argument values), not a static FunctionInfo
        // field — dynamic-per-call-site schemas are handled naturally, not
        // refused (see the class javadoc).

        // CONSISTENT is VGI's default; anything else means Spark must not
        // assume repeated calls with the same arguments agree.
        boolean deterministic = info.stability() == null || "CONSISTENT".equals(info.stability());

        return new VgiUnboundTableProcedure(
                client, config, schemaName, info, argFields, paramTypes, parameters, deterministic);
    }
}
