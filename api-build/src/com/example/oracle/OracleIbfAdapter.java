package com.example.oracle;

import com.example.core.PeriodicCheckpointer;
import com.example.core.TableRef;
import com.example.core.annotations.DataType;
import com.example.db.Retrier;
import com.example.ibf.ResizableInvertibleBloomFilter;
import com.example.ibf.db_incremental_sync.IbfTableEncoderWithCompoundPK;
import com.example.ibf.metrics.IbfTimerSampler;
import com.example.lambda.Lazy;
import com.example.logger.ExampleLogger;
import com.example.snowflakecritic.ibf.IbfDbUtils;
import com.example.snowflakecritic.ibf.InvertibleBloomFilter;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** This class is responsible for running the IBF SQL Query and retrieving the results. */
public class OracleIbfAdapter implements IbfTableEncoderWithCompoundPK {
    private static final ExampleLogger LOG = ExampleLogger.getMainLogger();

    private final Retrier retrier = new Retrier() {
        @Override
        protected Exception handleException(Exception exception, int attempt) throws Exception {
            return null;
        }

        @Override
        protected void waitForRetry(Exception exception, int attempt) throws InterruptedException {

        }

        @Override
        public DataSource get(Callable<DataSource> dataSourceInit, int i) {
            return null;
        }
    };

    private final OracleIBFQueryBuilder ibfQueryBuilder;
    private final DataSource dataSource;
    private final OracleTableInfo oracleTableInfo;
    private final OracleIbfTableInfo ibfTableInfo;
    private final PeriodicCheckpointer<OracleState> periodicCheckpointer;

    private Lazy<List<DataType>> getKeyTypes;

    public OracleIbfAdapter(
            DataSource dataSource,
            OracleTableInfo oracleTableInfo,
            OracleIbfTableInfo tableInfo,
            OracleIBFQueryBuilder queryBuilder,
            PeriodicCheckpointer periodicCheckpointer) {
        this.oracleTableInfo = oracleTableInfo;
        this.ibfTableInfo = tableInfo;
        this.ibfQueryBuilder = queryBuilder;
        this.dataSource = Objects.requireNonNull(dataSource);
        this.getKeyTypes =
                new Lazy<>(
                        () ->
                                this.oracleTableInfo
                                        .getPrimaryKeys()
                                        .stream()
                                        .map(columnInfo -> columnInfo.getSourceColumn().getWarehouseType())
                                        .collect(Collectors.toList()));
        this.periodicCheckpointer = periodicCheckpointer;
    }

    @Override
    public TableRef tableRef() {
        return ibfTableInfo.getTableRef();
    }

    @Override
    public List<Integer> keyLengths() {
        return ibfQueryBuilder.getKeyLengths();
    }

    @Override
    public List<DataType> keyTypes() {
        return getKeyTypes.get();
    }

    @Override
    public Optional<Long> estimatedRowCount() {
        return oracleTableInfo.getTableRowCount().isPresent()
                ? Optional.of(oracleTableInfo.getTableRowCount().getAsLong())
                : Optional.empty();
    }

    /**
     * Called by
     *
     * <ul>
     *   <li>IbfCheckpointManager.fetchStartingSizedIBF (called by IbfCheckpointManager.reset)
     *   <li>IbfCheckpointManager.fetchSizedIBF (called by IbfCheckpointManager.diff)
     * </ul>
     */
    @Override
    public ResizableInvertibleBloomFilter getResizableInvertibleBloomFilter(
            int smallCellCount, ResizableInvertibleBloomFilter.Sizes size) throws SQLException {
        // ifReplacementRequired() returns true then it means we are being called as part of
        // an IBF Transition Process use case (new columns added to the table) and we therefore
        // must create a REPLACEMENT IBF. If called during reset then create a REGULAR IBF.
        return newResizableInvertibleBloomFilter(
                smallCellCount,
                size,
                ifReplacementRequired()
                        ? OracleIBFQueryBuilder.IBFType.REPLACEMENT
                        : OracleIBFQueryBuilder.IBFType.REGULAR);
    }

    /**
     * Called only when there are modified columns by IbfCheckpointManager.fetchStartingSizedIBFWithColumnDefaults
     * (called by IbfCheckpointManager.diff if ifReplacementRequired() is true). Create a TRANSITION IBF.
     */
    @Override
    public ResizableInvertibleBloomFilter getReplacementResizableInvertibleBloomFilter(
            int smallCellCount, ResizableInvertibleBloomFilter.Sizes size) throws SQLException {
        return newResizableInvertibleBloomFilter(smallCellCount, size, OracleIBFQueryBuilder.IBFType.TRANSITION);
    }

    @SuppressWarnings("java:S1172")
    private ResizableInvertibleBloomFilter newResizableInvertibleBloomFilter(
            int smallCellCount, ResizableInvertibleBloomFilter.Sizes sizes, OracleIBFQueryBuilder.IBFType ibfType)
            throws SQLException {

        createBitXorFunction();

        //IbfMetricsLogging.reportTableSize(tableRef(), oracleTableInfo.getTableSize().orElse(0L));

        return getIbfFromDb(
                new ResizableInvertibleBloomFilter(ibfQueryBuilder.getSumKeyLengths(), smallCellCount, sizes),
                ibfQueryBuilder
                        .setIbfType(ibfType)
                        .setCellCount(smallCellCount)
                        .setFixedSize(false)
                        .setIbfSizes(sizes)
                        .buildQuery());
    }

    @Override
    public InvertibleBloomFilter getInvertibleBloomFilter(int cellCount) throws SQLException {
        return loadIbf(new InvertibleBloomFilter(ibfQueryBuilder.getSumKeyLengths(), cellCount), cellCount);
    }

    private InvertibleBloomFilter loadIbf(InvertibleBloomFilter ibf, int cellCount) throws SQLException {
        createBitXorFunction();

        return getIbfFromDb(ibf, ibfQueryBuilder.setCellCount(cellCount).setFixedSize(true).buildQuery());
    }

    private <T extends InvertibleBloomFilter>T getIbfFromDb(T ibf, String query) throws SQLException {
        try {
            //retrier.run(() -> executeIbfQueryAndLoad(ibf, query));
        } catch (Exception ex) {
            //LOG.warning("Failed to create ibf filter for table " + tableRef().toString(), ex);
            throw ex;
        }

        return ibf;
    }

    private ResizableInvertibleBloomFilter getIbfFromDb(ResizableInvertibleBloomFilter ibf, String query) throws SQLException {
        try {
            //retrier.run(() -> executeIbfQueryAndLoad(ibf, query));
        } catch (Exception ex) {
            //LOG.warning("Failed to create ibf filter for table " + tableRef().toString(), ex);
            throw ex;
        }

        return ibf;
    }

    protected void executeIbfQueryAndLoad(InvertibleBloomFilter ibf, String query) throws SQLException {
        IbfTimerSampler.IBF_IBF_QUERY.start(tableRef());
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {
            //IbfDbUtils.loadInvertibleBloomFilterFromResultSet(ibf, rs, Optional.of(periodicCheckpointer));
            IbfDbUtils.loadInvertibleBloomFilterFromResultSet(ibf, rs);

        } finally {
            IbfTimerSampler.IBF_IBF_QUERY.stop();
        }
    }

    protected void createBitXorFunction() throws SQLException {

        AtomicBoolean bitXorFunctionAvailable = new AtomicBoolean(false);

        //retrier.run(() -> bitXorFunctionAvailable.set(testBitXorFunction()));

        if (bitXorFunctionAvailable.get() == false) {
            LOG.info("Creating FT_BITXOR function");
            try {
                //retrier.run(() -> executeSql(ibfQueryBuilder.getCreateBitXorTypeStatement()));
                //retrier.run(() -> executeSql(ibfQueryBuilder.getCreateBitXorBodyStatement()));
                //retrier.run(() -> executeSql(ibfQueryBuilder.getCreateBitXorFunctionStatement()));
            } catch (Exception ex) {
                //LOG.warning("Failed to create FT_BITXOR function.", ex);
                throw ex;
            }
        }
    }

    protected boolean testBitXorFunction() throws SQLException {
        try {
            return isBitXorFunctionCreated();
        } catch (SQLException ex) {
            if (OracleErrorCode.ORA_00904.is(ex)) {
                return false;
            }

            throw ex;
        }
    }

    /**
     * Query the source to determine if FT_BITXOR function exists.
     *
     * @return true if FT_BITXOR function exists on source
     * @throws SQLException
     */
    protected boolean isBitXorFunctionCreated() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(ibfQueryBuilder.getTestBitXorFunction());
             ResultSet results = statement.executeQuery()) {
            if (results.next()) {
                return true;
            }
        }

        return false;
    }

    protected void executeSql(String sql) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    public boolean ifReplacementRequired() {
        return !ibfTableInfo.getModifiedColumns().isEmpty();
    }

    @Override
    public boolean hasCompoundPkSupport() {
        return true;
    }

    public OracleIbfTableInfo getIbfTableInfo() {
        return ibfTableInfo;
    }

    public OracleTableInfo getOracleTableInfo() {
        return oracleTableInfo;
    }

}
