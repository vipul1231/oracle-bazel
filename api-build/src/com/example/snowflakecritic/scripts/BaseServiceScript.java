package com.example.snowflakecritic.scripts;

import com.example.core.StandardConfig;
import com.example.ibf.cloud_storage.IbfPersistentStorage;
import com.example.snowflakecritic.*;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public abstract class BaseServiceScript extends BaseScriptRunner {

    protected final SnowflakeConnectorService service = new SnowflakeConnectorService(SnowflakeServiceType.SNOWFLAKE);
    protected final StandardConfigHelper standardConfigHelper;

    public BaseServiceScript(SnowflakeSource source, JsonFileHelper fileHelper) {
        super(source, fileHelper);
        this.standardConfigHelper = new StandardConfigHelper(fileHelper);
    }

    public StandardConfig getStandardConfig(SnowflakeConnectorState state) throws IOException {
        return standardConfigHelper.merge(service.standardConfig(source.originalCredentials, source.params, state));
    }

    protected void optionallySaveStandardConfig(StandardConfig standardConfig) throws IOException {
        standardConfigHelper.optionallySaveStandardConfig(standardConfig);
    }

    protected SnowflakeConnectorState getConnectorState() throws IOException {
        Optional<SnowflakeConnectorState> existingState = fileHelper.loadState();

        return existingState.isPresent() ? existingState.get() : service.initialState();
    }

    protected SnowflakeInformer getInformer(StandardConfig standardConfig) {
        return service.informer(source, standardConfig);
    }

    protected void saveState(SnowflakeConnectorState state) throws IOException {
        fileHelper.writeConnectorState(state);
    }

    protected void printState(String header, SnowflakeConnectorState state) throws Exception {
        System.out.println(header);
        System.out.println(fileHelper.toJson(state));
    }

    protected void printCredentials() throws Exception {
        System.out.println(fileHelper.toJson(source.originalCredentials));
    }

    protected IbfPersistentStorage localTestIbfPersistentStorage(SnowflakeConnectorState state) {
        SecretKeySpec secretKey = null;
        if (state.encryptedKey == null) {
            //secretKey = new SecretKeySpec(new byte[16], 0, 16, Encrypt.DEFAULT_ALGORITHM);
            state.encryptedKey = secretKey.getEncoded();
        } else {
            //secretKey = new SecretKeySpec(state.encryptedKey, 0, 16, Encrypt.DEFAULT_ALGORITHM);
        }

        Path path = fileHelper.getIbfStorageDir().toPath();
        System.out.println("Using local ibf storage location " + path);
        return IbfPersistentStorage.newBuilder(secretKey).withLocalDiskStorage(path).build();
    }
}