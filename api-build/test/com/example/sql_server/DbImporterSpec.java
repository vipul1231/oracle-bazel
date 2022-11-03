package com.example.sql_server;

import com.example.core.DbCredentials;
import com.example.core.StandardConfig;
import com.example.core.SyncMode;
import com.example.core.TableRef;
import com.example.core2.Output;
import com.example.db.DbImporter;
import com.example.db.DbRow;
import com.example.db.DbRowValue;
import com.google.common.collect.ImmutableList;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class DbImporterSpec<Importer extends DbImporter<TableRef>, Creds extends DbCredentials, State>
        extends DbTest<Creds, State> {

    protected Importer createImporter(State state, Output<State> out, Collection<TableRef> includedTables) {
        return createImporter(state, out, tableHelper.createStandardConfig(toMap(defaultTable, SyncMode.History)));
    }

    private Map<TableRef, SyncMode> toMap(TableRef tableRef, SyncMode syncMode) {
        Map<TableRef, SyncMode> map = new HashMap<>();
        map.put(tableRef, syncMode);

        return map;
    }

    protected abstract Importer createImporter(State state, Output<State> out, StandardConfig config);

    protected abstract void limitPageSize(Importer importerSpy, int maxRowsPerPage);

    protected State preparePreImportState(TableRef tableToImport) {
        return preparePreImportState(newState(), tableToImport);
    }

    // TODO make abstract when all subclasses implement this method
    protected State preparePreImportState(State newState, TableRef tableToImport) {
        return newState;
    }

    protected abstract State prepareMidImportState(
            State newState, TableRef tableToImport, DbRow<DbRowValue> lastImportedRow);

    protected abstract State preparePostImportState(State newState, TableRef tableToImport);

    protected Importer createImporter(State state, Output<State> out, TableRef includedTable) {
        return createImporter(state, out, ImmutableList.of(includedTable));
    }

    protected final void importPageAndAssertOutput(State state, TableRef tableToImport) throws IOException {
        importPageAndAssertOutput(__ -> {
        }, state, tableToImport, (__, ___) -> {
        });
    }

    protected final void importPageAndAssertOutput(
            Consumer<Importer> importerStubber, State state, TableRef tableToImport) throws IOException {

        importPageAndAssertOutput(importerStubber, state, tableToImport, (__, ___) -> {
        });
    }

    protected final void importPageAndAssertOutput(
            State state, TableRef tableToImport, BiConsumer<Importer, MockOutput2<State>> assertMethodInvocations)
            throws IOException {

        importPageAndAssertOutput(__ -> {
        }, state, tableToImport, assertMethodInvocations);
    }

    protected final void importPageAndAssertOutput(
            Consumer<Importer> importerStubber,
            State state,
            TableRef tableToImport,
            BiConsumer<Importer, MockOutput2<State>> assertMethodInvocations)
            throws IOException {

        MockOutput2<State> spiedOut = Mockito.spy(new MockOutput2<>(state));
        Importer spiedImporter = Mockito.spy(createImporter(state, (Output<State>) spiedOut, tableToImport));

        importerStubber.accept(spiedImporter);

        spiedImporter.importPage(tableToImport);

        // assert count and/or ordering of method invocations using Mockito
        assertMethodInvocations.accept(spiedImporter, spiedOut);

//        RecordComparator.compareDbConnectorOutput(spiedOut, this.getClass());
    }

    protected BiConsumer<Importer, MockOutput2<State>> assertImportFinished(TableRef tableToImport) {
        return (importer, __) -> assertTrue(importer.importFinished(tableToImport));
    }

    protected BiConsumer<Importer, MockOutput2<State>> assertImportIncomplete(TableRef tableToImport) {
        return (importer, __) -> assertFalse(importer.importFinished(tableToImport));
    }

    @Ignore
    @Test
    public void importPage_initial_emptyPage() throws Exception {
        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "foo");

        testDb.createTableFromRow(row, defaultTable);

        State preImportState = preparePreImportState(defaultTable);
        importPageAndAssertOutput(preImportState, defaultTable, assertImportFinished(defaultTable));
    }

    @Ignore
    @Test
    public void importPage_initial_belowPageLimit() throws Exception {
        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "foo");

        testDb.createTableFromRow(row, defaultTable);
        testDb.insertRowIntoTable(row, defaultTable);

        State preImportState = preparePreImportState(defaultTable);
        importPageAndAssertOutput(preImportState, defaultTable, assertImportFinished(defaultTable));
    }

    @Ignore
    @Test
    public void importPage_initial_abovePageLimit() throws Exception {
        DbRow<DbRowValue> rowA = rowHelper.simpleRow(1, "foo");
        DbRow<DbRowValue> rowB = rowHelper.simpleRow(2, "bar");

        testDb.createTableFromRow(rowA, defaultTable);

        testDb.insertRowIntoTable(rowA, defaultTable);
        testDb.insertRowIntoTable(rowB, defaultTable);

        State preImportState = preparePreImportState(defaultTable);

        // rowB should be absent from output
        importPageAndAssertOutput(
                i -> limitPageSize(i, 1), preImportState, defaultTable, assertImportIncomplete(defaultTable));
    }

    @Ignore
    @Test
    public void importPage_resume_emptyPage() throws Exception {
        DbRow<DbRowValue> lastImportedRow = rowHelper.simpleRow(1, "foo");

        State midImportState = prepareMidImportState(newState(), defaultTable, lastImportedRow);

        testDb.createTableFromRow(lastImportedRow, defaultTable);
        testDb.insertRowIntoTable(lastImportedRow, defaultTable);

        importPageAndAssertOutput(midImportState, defaultTable, assertImportFinished(defaultTable));
    }

    @Ignore
    @Test
    public void importPage_resume_belowPageLimit() throws Exception {
        DbRow<DbRowValue> lastImportedRow = rowHelper.simpleRow(1, "foo");
        DbRow<DbRowValue> nextRow = rowHelper.simpleRow(2, "bar");

        State midImportState = prepareMidImportState(newState(), defaultTable, lastImportedRow);

        testDb.createTableFromRow(lastImportedRow, defaultTable);
        testDb.insertRowIntoTable(lastImportedRow, defaultTable);
        testDb.insertRowIntoTable(nextRow, defaultTable);

        importPageAndAssertOutput(midImportState, defaultTable, assertImportFinished(defaultTable));
    }

    @Ignore
    @Test
    public void importPage_resume_abovePageLimit() throws Exception {
        DbRow<DbRowValue> lastImportedRow = rowHelper.simpleRow(1, "foo");
        DbRow<DbRowValue> nextRowA = rowHelper.simpleRow(2, "bar");
        DbRow<DbRowValue> nextRowB = rowHelper.simpleRow(3, "baz");

        State midImportState = prepareMidImportState(newState(), defaultTable, lastImportedRow);

        testDb.createTableFromRow(lastImportedRow, defaultTable);
        testDb.insertRowIntoTable(lastImportedRow, defaultTable);
        testDb.insertRowIntoTable(nextRowA, defaultTable);
        testDb.insertRowIntoTable(nextRowB, defaultTable);

        // lastImportedRow and nextRowB should be absent from output
        importPageAndAssertOutput(
                i -> limitPageSize(i, 1), midImportState, defaultTable, assertImportIncomplete(defaultTable));
    }

    @Ignore
    @Test
    public void importStarted_true() throws Exception {
        DbRow<DbRowValue> lastImportedRow = rowHelper.simpleRow(1, "foo");

        testDb.createTableFromRow(lastImportedRow, defaultTable);

        State midImportState = prepareMidImportState(newState(), defaultTable, lastImportedRow);
        Importer importer = createImporter(midImportState, (Output<State>) new MockOutput2<>(midImportState), defaultTable);

        assertTrue(importer.importStarted(defaultTable));
    }

    @Ignore
    @Test
    public void importStarted_false_newState() throws Exception {
        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "foo");

        testDb.createTableFromRow(row, defaultTable);

        State state = newState();
        Importer importer = createImporter(state, (Output<State>) new MockOutput2<>(state), defaultTable);

        assertFalse(importer.importStarted(defaultTable));
    }

    @Ignore
    @Test
    public void importStarted_false_preparedPreImportState() throws Exception {
        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "foo");

        testDb.createTableFromRow(row, defaultTable);

        State preImportState = preparePreImportState(defaultTable);
        Importer importer = createImporter(preImportState, (Output<State>) new MockOutput2<>(preImportState), defaultTable);

        assertFalse(importer.importStarted(defaultTable));
    }

    @Ignore
    @Test
    public void importFinished_true() throws Exception {
        DbRow<DbRowValue> lastRowImported = rowHelper.simpleRow(1, "foo");

        testDb.createTableFromRow(lastRowImported, defaultTable);

        State postImportState = preparePostImportState(newState(), defaultTable);
        Importer importer = createImporter(postImportState, (Output<State>) new MockOutput2<>(postImportState), defaultTable);

        assertTrue(importer.importFinished(defaultTable));
    }

    @Ignore
    @Test
    public void importFinished_false_beforeImport_newState() throws Exception {
        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "foo");

        testDb.createTableFromRow(row, defaultTable);

        State newState = newState();
        Importer importer = createImporter(newState, (Output<State>) new MockOutput2<>(newState), defaultTable);

        assertFalse(importer.importFinished(defaultTable));
    }

    @Ignore
    @Test
    public void importFinished_false_beforeImport_preparedPreImportState() throws Exception {
        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "foo");

        testDb.createTableFromRow(row, defaultTable);

        State preImportState = preparePreImportState(defaultTable);
        Importer importer = createImporter(preImportState, (Output<State>) new MockOutput2<>(preImportState), defaultTable);

        assertFalse(importer.importFinished(defaultTable));
    }

    @Ignore
    @Test
    public void importFinished_false_midImport() throws Exception {
        DbRow<DbRowValue> row = rowHelper.simpleRow(1, "foo");

        testDb.createTableFromRow(row, defaultTable);

        State newState = prepareMidImportState(newState(), defaultTable, row);
        Importer importer = createImporter(newState, (Output<State>) new MockOutput2<>(newState), defaultTable);

        assertFalse(importer.importFinished(defaultTable));
    }

    @Ignore
    @Test
    public abstract void tableSorter() throws Exception;
}

