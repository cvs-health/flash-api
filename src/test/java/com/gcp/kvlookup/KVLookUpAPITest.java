package com.gcp.kvlookup;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gcp.kvlookup.controller.KVLookUpController;
import com.gcp.kvlookup.dataaccess.BigTableDataAccessOperation;
import com.gcp.kvlookup.model.BigtableTableData;
import com.gcp.kvlookup.model.ColumnData;
import com.gcp.kvlookup.model.GCPBigtableTable;
import com.gcp.kvlookup.model.TableConfig;
import com.gcp.kvlookup.service.KVLookUpService;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
public class KVLookUpAPITest {

    private static MockMvc mockMvc;
    private static KVLookUpController kvLookUpController;

    @ClassRule
    public static final BigtableEmulatorRule bigtableEmulator = BigtableEmulatorRule.create();

    private static final String TEST_INSTANCE_ID = "test";
    private static final String TEST_PROJECT_ID = "test";
    private static BigTableDataAccessOperation tableCreationConfig;
    private static Map<String, BigtableDataClient> dataClientMap = new HashMap<>();
    private static Map<String, BigtableTableAdminClient> adminClientMap = new HashMap<>();

    private static KVLookUpService service;


    public static String asJsonString(final Object obj) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            final String jsonContent = mapper.writeValueAsString(obj);
            return jsonContent;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void emulatorSetup() throws Exception {
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

        kvLookUpController = new KVLookUpController(service, adminClientMap);

        GCPBigtableTable bigtable = new GCPBigtableTable();
        bigtable.setTableName("testTable");
        bigtable.setColumnFamily("cf1");

        service.createTable("test", bigtable);


        GCPBigtableTable tableToDelete = new GCPBigtableTable();
        tableToDelete.setTableName("tableToDelete");
        tableToDelete.setColumnFamily("cf1");

        service.createTable("test", tableToDelete);


        ColumnData columnData = new ColumnData();
        columnData.setColumnFamily("cf1");
        columnData.setColumnName("name");
        columnData.setColumnValue("columnValue");

        BigtableTableData bigtableTableData = new BigtableTableData();
        bigtableTableData.setTableName("testTable");
        bigtableTableData.setRowKeyId("0");
        bigtableTableData.setData(List.of(columnData));

        // call test method
        service.insertDataToTable(TEST_INSTANCE_ID, bigtableTableData);


    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(kvLookUpController).build();
    }


    @Test
    public void createTable() throws Exception {
        GCPBigtableTable bigtable = new GCPBigtableTable();
        bigtable.setTableName("testTable1");
        bigtable.setColumnFamily("cf1");


        mockMvc.perform(post("/v1/test/createTable")
                        .header("Content-Type", "application/json")
                        .content(asJsonString(bigtable)))
                .andExpect(status().is2xxSuccessful());
    }

    @Test
    public void insertData() throws Exception {
        BigtableTableData bigtableTableData = new BigtableTableData();
        bigtableTableData.setTableName("testTable");
        bigtableTableData.setRowKeyId("1");

        ColumnData columnData = new ColumnData();
        columnData.setColumnName("testColum");
        columnData.setColumnValue("testValue");
        columnData.setColumnFamily("cf1");


        bigtableTableData.setData(List.of(columnData));

        mockMvc.perform(post("/v1/test/insertData")
                        .header("Content-Type", "application/json")
                        .content(asJsonString(bigtableTableData)))
                .andExpect(status().is2xxSuccessful());
    }

    @Test
    public void readCellData() throws Exception {

        mockMvc.perform(get("/v1/test/readCellData?tableName=testTable&id=0").header("accept", "application/json"))
                .andExpect(status().is2xxSuccessful());
    }


    @Test
    public void deleteTable() throws Exception {

        TableConfig tableConfig = new TableConfig();
        tableConfig.setTableList(List.of("tableToDelete"));
        mockMvc.perform(delete("/v1/test/deleteTable")
                        .header("Content-Type", "application/json")
                        .content(asJsonString(tableConfig)))
                .andExpect(status().is2xxSuccessful());
    }

}
