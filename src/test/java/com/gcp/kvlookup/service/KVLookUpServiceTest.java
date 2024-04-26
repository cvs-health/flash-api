package com.gcp.kvlookup.service;

import com.gcp.kvlookup.dataaccess.BigTableDataAccessOperation;


import java.io.IOException;

import com.gcp.kvlookup.model.BigtableTableData;
import com.gcp.kvlookup.model.ColumnData;
import com.gcp.kvlookup.model.GCPBigtableTable;
import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import com.google.cloud.bigtable.data.v2.models.Row;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class KVLookUpServiceTest {


    // Initialize the emulator Rule
    @Rule
    public final BigtableEmulatorRule bigtableEmulator = BigtableEmulatorRule.create();

    private static final String TEST_INSTANCE_ID = "test";
    private static final String TEST_PROJECT_ID = "test";
    private BigTableDataAccessOperation tableCreationConfig;
    private Map<String, BigtableDataClient> dataClientMap = new HashMap<>();
    private Map<String, BigtableTableAdminClient> adminClientMap = new HashMap<>();

    private KVLookUpService service;


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

        service = new KVLookUpService(tableCreationConfig, dataClientMap);

    }

    @Test
    public void createTable() throws Exception {
        // create test data
        GCPBigtableTable bigtable = new GCPBigtableTable();
        bigtable.setTableName("testTable");
        bigtable.setColumnFamily("cf1");

        //call test method
        service.createTable(TEST_INSTANCE_ID, bigtable);

        // assert
        Assert.assertNotNull(adminClientMap.get("test").getTable("testTable"));
    }


    @Test
    public void insertDataToTable() throws Exception {
        // create Test Data
        GCPBigtableTable bigtable = new GCPBigtableTable();
        bigtable.setTableName("testTable1");
        bigtable.setColumnFamily("cf1");

        service.createTable(TEST_INSTANCE_ID, bigtable);

        ColumnData columnData = new ColumnData();
        columnData.setColumnFamily("cf1");
        columnData.setColumnName("testColumn");
        columnData.setColumnValue("columnValue");

        BigtableTableData bigtableTableData = new BigtableTableData();
        bigtableTableData.setTableName("testTable1");
        bigtableTableData.setRowKeyId("1");
        bigtableTableData.setData(List.of(columnData));

        // call test method
        service.insertDataToTable(TEST_INSTANCE_ID, bigtableTableData);

        // assert
        Assert.assertNotNull(dataClientMap.get("test").readRow("testTable1", "1"));
    }


    @Test
    public void readCellDataById() throws Exception {
        // create Test Data
        GCPBigtableTable bigtable = new GCPBigtableTable();
        bigtable.setTableName("testTable2");
        bigtable.setColumnFamily("cf1");

        service.createTable(TEST_INSTANCE_ID, bigtable);

        ColumnData columnData = new ColumnData();
        columnData.setColumnFamily("cf1");
        columnData.setColumnName("name");
        columnData.setColumnValue("columnValue");

        BigtableTableData bigtableTableData = new BigtableTableData();
        bigtableTableData.setTableName("testTable2");
        bigtableTableData.setRowKeyId("1");
        bigtableTableData.setData(List.of(columnData));

        service.insertDataToTable(TEST_INSTANCE_ID, bigtableTableData);

        //call test methods
        String value = service.readCellDataById(TEST_INSTANCE_ID, "testTable2", "1");

        Assert.assertEquals(value, "columnValue");
    }


    @Test(expected = NotFoundException.class)
    public void deleteTable() {
        CreateTableRequest createTableRequest = CreateTableRequest.of("deleteTableTest");
        adminClientMap.get("test").createTable(createTableRequest);

        service.deleteTable(TEST_INSTANCE_ID, List.of("deleteTableTest"));

        adminClientMap.get("test").getTable("deleteTableTest");
    }
}
