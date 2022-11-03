package com.example.oracle;

import com.example.core.TableRef;
import com.example.oracle.cache.PartitionedTableCache;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Created by IntelliJ IDEA.<br/>
 * User: Thilanka<br/>
 * Date: 9/14/2021<br/>
 * Time: 7:23 AM<br/>
 * To change this template use File | Settings | File Templates.
 */
public class PartitionedTableCacheSpec {

    private PartitionedTableCache partitionedTableCache;

    @Before
    public void beforePartitionedTableCacheSpec() {
        partitionedTableCache = mock(PartitionedTableCache.class);
    }

    @Test
    public void initPartitionedTablesMultiplePages() {
        List<TableRef> selectedTables = new ArrayList<>();
        TableRef tableRef1 = new TableRef("schema1", "name1");
        TableRef tableRef2 = new TableRef("schema1", "name2");
        TableRef tableRef3 = new TableRef("schema2", "name3");
        TableRef tableRef4 = new TableRef("schema2", "name4");

        selectedTables.add(tableRef1);
        selectedTables.add(tableRef2);
        selectedTables.add(tableRef3);
        selectedTables.add(tableRef4);

        doAnswer(
                invocation -> {
                    Object[] args = invocation.getArguments();
                    Set<TableRef> partitionedTables = partitionedTableCache.getPartitionedTables();
                    partitionedTables.addAll((List<TableRef>) args[0]);
                    Set<TableRef> partitionedTablesWithRME = partitionedTableCache.partitionedTablesWithRowMovementEnabled();
                    partitionedTablesWithRME.addAll((List<TableRef>) args[0]);
                    return null;
                })
                .when(partitionedTableCache)
                .initPartitionedTablePages(any());

        // maxSize is less than selectedTables.size()
        partitionedTableCache.initPartitionedTables(selectedTables, 2);

        // Verify initPartitionedTablePages called twice
        verify(partitionedTableCache, times(2)).initPartitionedTablePages(any());

        assertEquals(4, partitionedTableCache.getPartitionedTables().size());
        assertEquals(4, partitionedTableCache.partitionedTablesWithRowMovementEnabled().size());

        assertTrue(partitionedTableCache.getPartitionedTables().containsAll(selectedTables));
        assertTrue(partitionedTableCache.partitionedTablesWithRowMovementEnabled().containsAll(selectedTables));
    }

    @Test
    public void initPartitionedTablesSinglePage() {
        List<TableRef> selectedTables = new ArrayList<>();
        TableRef tableRef1 = new TableRef("schema1", "name1");
        TableRef tableRef2 = new TableRef("schema1", "name2");
        TableRef tableRef3 = new TableRef("schema2", "name3");
        TableRef tableRef4 = new TableRef("schema2", "name4");

        selectedTables.add(tableRef1);
        selectedTables.add(tableRef2);
        selectedTables.add(tableRef3);
        selectedTables.add(tableRef4);

        doAnswer(
                invocation -> {
                    Object[] args = invocation.getArguments();
                    Set<TableRef> partitionedTables = partitionedTableCache.getPartitionedTables();
                    partitionedTables.addAll((List<TableRef>) args[0]);
                    Set<TableRef> partitionedTablesWithRME = partitionedTableCache.partitionedTablesWithRowMovementEnabled();
                    partitionedTablesWithRME.addAll((List<TableRef>) args[0]);
                    return null;
                })
                .when(partitionedTableCache)
                .initPartitionedTablePages(any());

        // maxSize is greater than selectedTables.size()
        partitionedTableCache.initPartitionedTables(selectedTables, 100);
        assertEquals(4, partitionedTableCache.getPartitionedTables().size());
        assertEquals(4, partitionedTableCache.partitionedTablesWithRowMovementEnabled().size());

        assertTrue(partitionedTableCache.getPartitionedTables().containsAll(selectedTables));
        assertTrue(partitionedTableCache.partitionedTablesWithRowMovementEnabled().containsAll(selectedTables));
    }
}