package com.gcp.kvlookup.dataaccess;

import com.gcp.kvlookup.model.BigtableTableData;
import com.gcp.kvlookup.model.ColumnData;
import com.gcp.kvlookup.model.GCPBigtableTable;
import com.gcp.kvlookup.service.KVLookUpService;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class BigTableDataAccessOperationTest {


    // Initialize the emulator Rule
    @Rule
    public final BigtableEmulatorRule bigtableEmulator = BigtableEmulatorRule.create();

    private static final String TEST_INSTANCE_ID = "test";
    private static final String TEST_PROJECT_ID = "test";
    private BigTableDataAccessOperation tableCreationConfig;
    private Map<String, BigtableDataClient> dataClientMap = new HashMap<>();
    private Map<String, BigtableTableAdminClient> adminClientMap = new HashMap<>();

    @Before
    public void setUp() throws IOException {
        // Initialize the clients to connect to the emulator
        BigtableTableAdminSettings.Builder tableAdminSettings = BigtableTableAdminSettings.newBuilderForEmulator(bigtableEmulator.getPort());
        tableAdminSettings.setProjectId(TEST_PROJECT_ID);
        tableAdminSettings.setInstanceId(TEST_INSTANCE_ID);
        BigtableTableAdminClient tableAdminClient = BigtableTableAdminClient.create(tableAdminSettings.build());

        BigtableDataSettings.Builder dataSettings = BigtableDataSettings.newBuilderForEmulator(bigtableEmulator.getPort());
        dataSettings.setInstanceId(TEST_INSTANCE_ID);
        dataSettings.setProjectId(TEST_PROJECT_ID);
        BigtableDataClient dataClient = BigtableDataClient.create(dataSettings.build());

        dataClientMap.put("test", dataClient);
        adminClientMap.put("test", tableAdminClient);

        tableCreationConfig = new BigTableDataAccessOperation(adminClientMap, dataClientMap, null);
        tableCreationConfig.setColumnQualifierName("name");
        tableCreationConfig.setColumnFamily("cf1");

    }

    @Test
    public void createTable() {
        GCPBigtableTable bigtable = new GCPBigtableTable();
        bigtable.setTableName("testTable");
        bigtable.setColumnFamily("cf1");

        //call test method
        tableCreationConfig.createTable(TEST_INSTANCE_ID, bigtable);

        // assert
        Assert.assertNotNull(adminClientMap.get("test").getTable("testTable"));
    }

    @Test(expected = NotFoundException.class)
    public void deleteTable() {
        CreateTableRequest createTableRequest = CreateTableRequest.of("deleteTableTest");
        adminClientMap.get("test").createTable(createTableRequest);

        tableCreationConfig.deleteTable(TEST_INSTANCE_ID, "deleteTableTest");

        adminClientMap.get("test").getTable("deleteTableTest");
    }

    @Test
    public void writeToTable() {
        GCPBigtableTable bigtable = new GCPBigtableTable();
        bigtable.setTableName("testTable1");
        bigtable.setColumnFamily("cf1");

        tableCreationConfig.createTable(TEST_INSTANCE_ID, bigtable);

        ColumnData columnData = new ColumnData();
        columnData.setColumnFamily("cf1");
        columnData.setColumnName("testColumn");
        columnData.setColumnValue("columnValue");

        BigtableTableData bigtableTableData = new BigtableTableData();
        bigtableTableData.setTableName("testTable1");
        bigtableTableData.setRowKeyId("1");
        bigtableTableData.setData(List.of(columnData));

        // call test method
        tableCreationConfig.writeToTable(TEST_INSTANCE_ID, bigtableTableData);

        // assert
        Assert.assertNotNull(dataClientMap.get("test").readRow("testTable1", "1"));
    }

    @Test
    public void readCellDataById() {
        // create Test Data
        GCPBigtableTable bigtable = new GCPBigtableTable();
        bigtable.setTableName("testTable2");
        bigtable.setColumnFamily("cf1");

        tableCreationConfig.createTable(TEST_INSTANCE_ID, bigtable);

        ColumnData columnData = new ColumnData();
        columnData.setColumnFamily("cf1");
        columnData.setColumnName("name");
        columnData.setColumnValue("columnValue");

        BigtableTableData bigtableTableData = new BigtableTableData();
        bigtableTableData.setTableName("testTable2");
        bigtableTableData.setRowKeyId("1");
        bigtableTableData.setData(List.of(columnData));

        tableCreationConfig.writeToTable(TEST_INSTANCE_ID, bigtableTableData);

        //call test methods
        String value = tableCreationConfig.readCellDataById(TEST_INSTANCE_ID, "testTable2", "1");

        Assert.assertEquals(value, "columnValue");
    }
}