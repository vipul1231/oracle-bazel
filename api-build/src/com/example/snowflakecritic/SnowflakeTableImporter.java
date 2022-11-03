package com.example.snowflakecritic;

public interface SnowflakeTableImporter {

    void startImport();

    void importPage();

    void stopImport();

    void onImportException(Exception ex);

    boolean isImportStarted();

    boolean isTableImportComplete();
}
