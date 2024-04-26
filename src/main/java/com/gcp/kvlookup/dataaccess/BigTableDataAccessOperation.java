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

package com.gcp.kvlookup.dataaccess;

import com.gcp.kvlookup.exception.KVLookUpException;
import com.gcp.kvlookup.model.BigtableTableData;
import com.gcp.kvlookup.model.ColumnData;
import com.gcp.kvlookup.model.GCPBigtableTable;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateInstanceRequest;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Instance;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.collect.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Component
public class BigTableDataAccessOperation {

    private static final Logger logger = LoggerFactory.getLogger(BigTableDataAccessOperation.class);
    @Value("${gcp.bigtable.column.QualifierName}")
    private String columnQualifierName;   // GCP_BIGTABLE_COLUMN_QUALIFIERNAME

    @Value("${gcp.bigtable.column.family}")
    private String columnFamily;   // GCP_BIGTABLE_COLUMN_FAMILY

    private Map<String, BigtableTableAdminClient> adminClient;
    private Map<String, BigtableDataClient> dataClient;
    private BigtableInstanceAdminClient instanceAdminClient;

    public BigTableDataAccessOperation(Map<String, BigtableTableAdminClient> adminClient, Map<String, BigtableDataClient> dataClient, BigtableInstanceAdminClient instanceAdminClient) {
        this.adminClient = adminClient;
        this.dataClient = dataClient;
        this.instanceAdminClient = instanceAdminClient;
    }

    public void createTable(String instanceID, GCPBigtableTable gcpBigtableTable) {
        String tableName = gcpBigtableTable.getTableName();
        logger.info("creating table: {}", kv("table", tableName));
        CreateTableRequest createTableRequest = CreateTableRequest.of(tableName).addFamily(gcpBigtableTable.getColumnFamily());
        adminClient.get(instanceID).createTable(createTableRequest);
        logger.info("Table created successfully {}", kv("table", tableName));
    }

    public void deleteTable(String instanceID, String tableId) {
        logger.info("Deleting table: " + tableId);
        try {
            adminClient.get(instanceID).deleteTable(tableId);
            logger.info("Table {} deleted successfully", tableId);
        } catch (NotFoundException e) {
            logger.error("Failed to delete a non-existent table: " + e.getMessage());
        }
    }

    public void writeToTable(String instanceID, BigtableTableData bigtableTableData) {
        try {
            if (adminClient.get(instanceID).exists(bigtableTableData.getTableName())) {
                logger.info("Writing data to the table");
                String tableName = bigtableTableData.getTableName();
                String rowKeyId = bigtableTableData.getRowKeyId();
                List<ColumnData> data = bigtableTableData.getData();
                for (ColumnData columnData : data) {
                    RowMutation rowMutation = RowMutation.create(tableName, rowKeyId)
                            .setCell(columnData.getColumnFamily(), columnData.getColumnName(), columnData.getColumnValue());
                    dataClient.get(instanceID).mutateRow(rowMutation);
                    logger.info("Wrote data to table successfully");
                }
            } else {
                throw new KVLookUpException("Tried to insert data into table that doesn't exist", HttpStatus.NOT_FOUND);
            }
        } catch (Exception e) {
            logger.error("Error occurred when inserting data to table" + e.getMessage());
            throw new KVLookUpException(e.getMessage(), HttpStatus.NOT_FOUND);
        }
    }

    public String readCellDataById(String instanceID, String tableName, String rowId) {
        logger.info("Reading specific cells by tableName {} and id {}", kv("tableName", tableName), kv("keyId", rowId));
        Row row = dataClient.get(instanceID).readRow(tableName, rowId);
        if (Objects.isNull(row)) {
            logger.info("No Data returned for the given tableName {} and id {}", kv("tableName", tableName), kv("keyId", rowId));
            throw new KVLookUpException("NOT FOUND", HttpStatus.NOT_FOUND);
        }
        logger.info("Row: " + row.getKey().toStringUtf8());
        List<RowCell> cells = row.getCells(columnFamily, columnQualifierName);
        if (CollectionUtils.isEmpty(cells)) {
            throw new KVLookUpException("NOT FOUND", HttpStatus.NOT_FOUND);
        }
        return cells.get(0).getValue().toStringUtf8();
    }

    public void createInstance(String instanceId, String clusterName) {
        // Create the instance
        // Checks if instance exists, creates instance if does not exists.
        if (!instanceAdminClient.exists(instanceId)) {
            System.out.println("Instance does not exist, creating a DEVELOPMENT instance");
            CreateInstanceRequest createInstanceRequest =
                    CreateInstanceRequest.of(instanceId)
                            .addCluster(clusterName, "us-east4-a", 1, StorageType.SSD)
                            .addLabel("name", "gcp-kv-crud-rest-api");
            try {
                Instance instance = instanceAdminClient.createInstance(createInstanceRequest);
                System.out.printf("DEVELOPMENT type instance %s created successfully%n", instance.getId());
            } catch (Exception e) {
                throw new KVLookUpException("Error occurred when creating instance");
            }
        }
    }

    public long countRecords(String instanceID, String tableName) throws IOException {
        Query query = Query.create(tableName);
        ServerStream<Row> rows = dataClient.get(instanceID).readRows(query);

        System.out.println("retrieved data");
        long count = Streams.stream(rows).count();

        return count;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public void setColumnQualifierName(String columnQualifierName) {
        this.columnQualifierName = columnQualifierName;
    }
}
