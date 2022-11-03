package com.example.snowflakecritic.scripts;


import com.example.core.TableRef;
import com.example.snowflakecritic.SnowflakeConnectorState;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class FileBasedScriptRunnerOutput extends ScriptRunnerOutput {

    private final JsonFileHelper fileHelper;

    private Map<TableRef, File> outputFiles = new HashMap<>();

    public FileBasedScriptRunnerOutput(JsonFileHelper fileHelper) {
        this.fileHelper = fileHelper;
    }

    @Override
    public void checkpoint(SnowflakeConnectorState state) {
        super.checkpoint(state);
        try {
            System.out.println("==============================");
            System.out.println("    Checkpoint: saving state");
            System.out.println(fileHelper.toJson(state));
            fileHelper.writeConnectorState(state);
        } catch (Exception ex) {
            throw new RuntimeException("Exception in checkpoint", ex);
        }
    }

    @Override
    protected void write(TableRef tableRef, String message) {
        File outputFile = outputFiles.computeIfAbsent(tableRef, fileHelper::createOutputFile);

        try (FileWriter writer = new FileWriter(outputFile, true);
             BufferedWriter bw = new BufferedWriter(writer);
             PrintWriter out = new PrintWriter(bw)) {
            out.println(message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        UserInputHelper.printHeader("Table Output Files:");
        outputFiles.values().forEach(file -> System.out.println(file));
        super.close();
    }
}