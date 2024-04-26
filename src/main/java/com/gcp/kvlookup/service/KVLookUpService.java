/*
    Copyright 2024 CVS Health and/or one of its affiliates

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.gcp.kvlookup.service;

import com.gcp.kvlookup.dataaccess.BigTableDataAccessOperation;
import com.gcp.kvlookup.controller.KVLookUpController;
import com.gcp.kvlookup.model.BigtableTableData;
import com.gcp.kvlookup.model.GCPBigtableTable;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Service
public class KVLookUpService {
    private static final Logger logger = LoggerFactory.getLogger(KVLookUpController.class);

    private final BigTableDataAccessOperation bigTableDataAccessOperation;

    private final Map<String, BigtableDataClient> dataClient;

    public KVLookUpService(BigTableDataAccessOperation tableCreationConfig, Map<String, BigtableDataClient> dataClient) {
        this.bigTableDataAccessOperation = tableCreationConfig;
        this.dataClient = dataClient;
    }

    public void createTable(String instanceID, GCPBigtableTable gcpBigtableTable) throws Exception {
        logger.info("creating table: " + gcpBigtableTable.getTableName());
        bigTableDataAccessOperation.createTable(instanceID, gcpBigtableTable);
    }

    public String readCellDataById(String instanceID, String tableName, String id) {
        logger.info("Reading specific cells by tableName and id");
        String cellDataById = bigTableDataAccessOperation.readCellDataById(instanceID, tableName, id);
        logger.info("cellData {} Retrieved for a given tableName {} and rowId {} ", kv("cellData", cellDataById), kv("tableName", tableName), kv("rowId", id));
        return cellDataById;
    }

    public void deleteTable(String instanceID, List<String> tableList) {
        for (String table : tableList) {
            bigTableDataAccessOperation.deleteTable(instanceID, table);
        }
    }

    public void insertDataToTable(String instanceID, BigtableTableData bigtableTableData) {
        bigTableDataAccessOperation.writeToTable(instanceID, bigtableTableData);
    }

    public void createInstance(String instanceId, String clusterName) throws IOException {
        bigTableDataAccessOperation.createInstance(instanceId, clusterName);
    }

    public long countRecords(String instanceID, String tableName) throws IOException, ExecutionException, InterruptedException {
        return bigTableDataAccessOperation.countRecords(instanceID, tableName);
    }
}
