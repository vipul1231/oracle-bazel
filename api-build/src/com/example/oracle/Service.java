package com.example.oracle;

import com.example.core.DbCredentials;
import com.example.core.StandardConfig;
import com.example.core.TableRef;

import java.net.URI;
import java.nio.file.Path;
import java.util.*;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 6/29/2021<br/>
 * Time: 5:06 PM<br/>
 * To change this template use File | Settings | File Templates.
 */
public interface Service<D, O> {
    String id();

    String name();

    ServiceType type();

    String description();

    Path largeIcon();

    URI linkToDocs();

    OnboardingSettings onboardingSettings();

    OracleState initialState();

    Class<DbCredentials> credentialsClass();

    boolean supportsCreationViaApi();

//    DbCredentials prepareApiCredentials(DbCredentials creds, String schema, WizardContext wizardContext);

    DbCredentials prepareApiCredentials(DbCredentials creds, String schema, WizardContext wizardContext);

    LinkedHashMap<String, Object> connectionConfig(
            DbCredentials creds, OracleState oracleState, String owner, String integration);

//    void update(
//            DbCredentials credentials,
//            OracleState state,
//            ConnectionParameters params,
//            Output<OracleState> output,
//            StandardConfig standardConfig,
//            Set<String> featureFlags) throws CloseConnectionException;

    //    @Override
    //    public void update(
    //            DbCredentials credentials,
    //            OracleState state,
    //            ConnectionParameters params,
    //            Output<OracleState> output,
    //            StandardConfig standardConfig,
    //            Set<String> featureFlags) {
    //        // Automatically call close method on updater
    //        try (OracleUpdater updater = (OracleUpdater) updater(credentials, state, params)) {
    //            updater.update2(output, standardConfig);
    //        }
    //    }
    //
    boolean targetCore2Directly(Set<String> featureFlags);

    boolean startPaused();

    String schemaRule(Optional<String> schemaNameFromRecord, ConnectionParameters params);

    boolean supportsTableResync();

    OracleState clearTableState(OracleState state, TableRef table);

    boolean canDefineSchema();

    Map<String, ColumnConfig> columnConfig(
            DbCredentials creds, ConnectionParameters params, String schema, String table);

    Optional<List<Object>> pricingDimensions(
            DbCredentials creds, OracleState state, ConnectionParameters params);

    boolean hidden();

    boolean beta();

    Map<String, String> customMicrometerLabels(DbCredentials credentials, ConnectionParameters params);

    StandardConfig standardConfig(DbCredentials creds, ConnectionParameters params, OracleState state);
}