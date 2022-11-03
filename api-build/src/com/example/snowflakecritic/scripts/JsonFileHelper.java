package com.example.snowflakecritic.scripts;

import com.example.core.StandardConfig;
import com.example.core.TableRef;
import com.example.db.DefaultObjectMapper;
import com.example.snowflakecritic.SnowflakeConnectorState;
import com.example.snowflakecritic.SnowflakeSourceCredentials;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public class JsonFileHelper {

    public static final String STATE_FILE = "state.json";
    public static final String STANDARD_CONFIG_FILE = "standardConfig.json";
    public static final String Ibf = "ibf";
    private ObjectMapper objectMapper =
            DefaultObjectMapper.create()
                    .enable(SerializationFeature.INDENT_OUTPUT)
                    .configure(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, true)
                    .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

    private File connectorDir;

    private String processId;
    private File outputDir;

    public JsonFileHelper(File dir, String processId) {
        this.connectorDir = dir;
        this.processId = processId;
    }

    /**
     * Use for writing data from Output
     *
     * @return
     */
    public File getOutputDir() {
        if (null == outputDir) {
            this.outputDir = new File(connectorDir, processId);
            outputDir.mkdir();
        }

        return outputDir;
    }

    public File createOutputFile(TableRef tableRef) {
        File file = new File(getOutputDir(), tableRef.schema + "_" + tableRef.name);
        System.out.println("Created output file: " + file);
        return file;
    }

    public String toJson(Object obj) throws Exception {
        return objectMapper.writeValueAsString(obj);
    }

    public SnowflakeSourceCredentials loadCredentials() throws IOException {
        return objectMapper.readValue(new File(connectorDir, "credentials.json"), SnowflakeSourceCredentials.class);
    }

    public <T> T loadObjectFromFile(String filename, Class<T> typeOf) throws IOException {
        return objectMapper.readValue(new File(connectorDir, filename), typeOf);
    }

    public void writeStandardConfig(StandardConfig standardConfig) throws IOException {
        File stdConfigFile = getFileAndBackUp(STANDARD_CONFIG_FILE);
        objectMapper.writeValue(stdConfigFile, standardConfig);
        System.out.println("Wrote standard config to " + stdConfigFile);
    }

    private <T> Optional<T> loadOptionalConnectorFile(Class<T> typeOf, String fileName) throws IOException {
        File file = new File(connectorDir, fileName);
        if (file.exists()) {
            return Optional.of(objectMapper.readValue(file, typeOf));
        }

        return Optional.empty();
    }

    public Optional<StandardConfig> loadStandardConfig() throws IOException {
        return loadOptionalConnectorFile(StandardConfig.class, STANDARD_CONFIG_FILE);
    }

    public Optional<SnowflakeConnectorState> loadState() throws IOException {
        return loadOptionalConnectorFile(SnowflakeConnectorState.class, STATE_FILE);
    }

    public void writeConnectorState(SnowflakeConnectorState state) throws IOException {
        File stateFile = getFileAndBackUp(STATE_FILE);
        objectMapper.writeValue(stateFile, state);
        System.out.println("Wrote state to " + stateFile);
    }

    public void removeConnectorState() {
        File stateFile = getFileAndBackUp(STATE_FILE);

        if (stateFile.exists()) {
            stateFile.delete();
        }
    }

    public void removeLocalStorage() {
        File ibfDir = new File(connectorDir, Ibf);
        if (ibfDir.exists()) {
            // Delete its content
            File[] content = ibfDir.listFiles();
            if (content != null) {
                for (File file : content) {
                    if (!file.delete()) {
                        System.out.println("Could not delete file " + file);
                    }
                }
            }

            if (!ibfDir.delete()) {
                System.out.println("Could not delete " + ibfDir);
            } else {
                System.out.println("Deleted " + ibfDir);
            }
        }
    }

    private File getFileAndBackUp(String name) {
        File file = new File(connectorDir, name);
        if (file.exists()) {
            File bakFile = new File(connectorDir, name + ".bak");
            file.renameTo(bakFile);
            System.out.println("Created backup " + bakFile);
        }
        return file;
    }

    public File getIbfStorageDir() {
        File ibfDir = new File(connectorDir, Ibf);
        ibfDir.mkdir();
        return ibfDir;
    }

    public Path getIbfStoragePath() {
        // create a well-named subfolder for local storage.
        File file = new File("IbfLocal.txt");
        return file.toPath();
    }
}
