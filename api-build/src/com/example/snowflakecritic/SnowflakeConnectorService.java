package com.example.snowflakecritic;

import com.example.core.DbCredentials;
import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.core2.Output;
import com.example.db.DbService;
import com.example.db.DbServiceType;
import com.example.oracle.ColumnConfig;
import com.example.oracle.OnboardingSettings;
import com.example.oracle.OracleState;
import com.example.oracle.WizardContext;
import com.example.snowflakecritic.setup.SnowflakeConnectorSetupForm;
import com.example.core2.ConnectionParameters;

import com.example.sql_server.SqlServerSource;
import com.google.common.collect.ImmutableList;

import javax.sql.DataSource;
import java.net.URI;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SnowflakeConnectorService
        extends DbService<
        SnowflakeSourceCredentials,
        SnowflakeConnectorState,
        SnowflakeTableInfo,
        SnowflakeSource,
        SnowflakeInformer> {

    private final SnowflakeServiceType serviceType;

    public SnowflakeConnectorService(SnowflakeServiceType serviceType) {
        this.serviceType = serviceType;
    }

    @Override
    public String id() {
        return serviceType.id();
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String description() {
        return null;
    }

    @Override
    public Path largeIcon() {
        return null;
    }

    @Override
    public URI linkToDocs() {
        return null;
    }

    @Override
    public OnboardingSettings onboardingSettings() {
        return null;
    }

    @Override
    public boolean supportsCreationViaApi() {
        return false;
    }

    @Override
    public DbCredentials prepareApiCredentials(DbCredentials creds, String schema, WizardContext wizardContext) {
        return null;
    }

    @Override
    public LinkedHashMap<String, Object> connectionConfig(DbCredentials creds, OracleState oracleState, String owner, String integration) {
        return null;
    }

    @Override
    public boolean startPaused() {
        return false;
    }

    @Override
    public String schemaRule(Optional<String> schemaNameFromRecord, com.example.oracle.ConnectionParameters params) {
        return null;
    }

    @Override
    public boolean supportsTableResync() {
        return false;
    }

    @Override
    public OracleState clearTableState(OracleState state, TableRef table) {
        return null;
    }

    @Override
    public boolean canDefineSchema() {
        return false;
    }

    @Override
    public Map<String, ColumnConfig> columnConfig(DbCredentials creds, com.example.oracle.ConnectionParameters params, String schema, String table) {
        return null;
    }

    @Override
    public Optional<List<Object>> pricingDimensions(DbCredentials creds, OracleState state, com.example.oracle.ConnectionParameters params) {
        return Optional.empty();
    }

    @Override
    public boolean hidden() {
        return false;
    }

    @Override
    public boolean beta() {
        return false;
    }

    @Override
    public Map<String, String> customMicrometerLabels(DbCredentials credentials, com.example.oracle.ConnectionParameters params) {
        return null;
    }

    @Override
    public StandardConfig standardConfig(DbCredentials creds, com.example.oracle.ConnectionParameters params, OracleState state) {
        return null;
    }

    public StandardConfig standardConfig(
            SnowflakeSourceCredentials snowflakeSourceCredentials,
            ConnectionParameters params,
            SnowflakeConnectorState snowflakeConnectorState) {
        return sqlStandardConfig(snowflakeSourceCredentials, params, snowflakeConnectorState);
    }

    @Override
    public SnowflakeConnectorState initialState() {
        return new SnowflakeConnectorState();
    }

    @Override
    public Class credentialsClass() {
        return SnowflakeSourceCredentials.class;
    }

    @Override
    protected SnowflakeSource source(
            SnowflakeSourceCredentials snowflakeSourceCredentials, ConnectionParameters connectionParameters) {
        return new SnowflakeConnectorServiceConfiguration()
                .setCredentials(snowflakeSourceCredentials)
                .setConnectionParameters(connectionParameters)
                .getSnowflakeSource();
    }

    @Override
    public SnowflakeInformer informer(SnowflakeSource snowflakeSource, StandardConfig standardConfig) {
        return new SnowflakeConnectorServiceConfiguration()
                .setSnowflakeSource(snowflakeSource)
                .setStandardConfig(standardConfig)
                .getSnowflakeInformer();
    }

    @Override
    public SnowflakeImporter importer(
            SnowflakeSource snowflakeSource,
            SnowflakeConnectorState snowflakeConnectorState,
            SnowflakeInformer informer,
            Output<SnowflakeConnectorState> out) {
        return new SnowflakeConnectorServiceConfiguration()
                .setSnowflakeSource(snowflakeSource)
                .setConnectorState(snowflakeConnectorState)
                .setSnowflakeInformer(informer)
                .setOutput(out)
                .getImporter();
    }

    @Override
    protected SnowflakeIncrementalUpdater incrementalUpdater(
            SnowflakeSource snowflakeSource,
            SnowflakeConnectorState snowflakeConnectorState,
            SnowflakeInformer informer,
            Output<SnowflakeConnectorState> out) {
        return new SnowflakeConnectorServiceConfiguration()
                .setCredentials(snowflakeSource.getOriginalCredentials())
                .setConnectionParameters(snowflakeSource.getConnectionParameters())
                .setSnowflakeSource(snowflakeSource)
                .setConnectorState(snowflakeConnectorState)
                .setSnowflakeInformer(informer)
                .setOutput(out)
                .getSnowflakeIncrementalUpdater();
    }

    @Override
    public DbServiceType serviceType() {
        return serviceType;
    }

    @Override
    protected List<String> tableExcludedReasons(
            SnowflakeSourceCredentials snowflakeSourceCredentials, SnowflakeTableInfo tableInfo) {
        ImmutableList.Builder<String> reasons = ImmutableList.builder();
        return reasons.build();
    }

    @Override
    protected List<String> tableExcludedReasons(SnowflakeTableInfo tableInfo) {
        return null;
    }

    @Override
    public SqlServerSource source(DbCredentials credentials, ConnectionParameters params, DataSource dataSource) {
        return null;
    }

    @Override
    public SnowflakeConnectorSetupForm setupForm(WizardContext wizardContext) {
        return new SnowflakeConnectorSetupForm(wizardContext, serviceType);
    }

    private SnowflakeConnectorServiceConfiguration serviceConfiguration(
            SnowflakeSource snowflakeSource,
            SnowflakeConnectorState snowflakeConnectorState,
            SnowflakeInformer informer,
            Output<SnowflakeConnectorState> out) {
        return new SnowflakeConnectorServiceConfiguration()
                .setSnowflakeSource(snowflakeSource)
                .setCredentials(snowflakeSource.originalCredentials)
                .setConnectorState(snowflakeConnectorState)
                .setSnowflakeInformer(informer)
                .setOutput(out);
    }


}
