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

package com.gcp.kvlookup.controller;

import com.gcp.kvlookup.exception.KVLookUpException;
import com.gcp.kvlookup.model.BigtableTableData;
import com.gcp.kvlookup.model.GCPBigtableTable;
import com.gcp.kvlookup.model.TableConfig;
import com.gcp.kvlookup.service.KVLookUpService;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

import static net.logstash.logback.argument.StructuredArguments.kv;

@CrossOrigin
@RestController
@RequestMapping("/v1")
public class KVLookUpController {

    private static final Logger logger = LoggerFactory.getLogger(KVLookUpController.class);

    private KVLookUpService UMAAFeatureLookUpService;

    private Map<String, BigtableTableAdminClient> bigtableTableAdminClientMap;


    public KVLookUpController(KVLookUpService UMAAFeatureLookUpService, Map<String, BigtableTableAdminClient> bigtableTableAdminClientMap) {
        this.UMAAFeatureLookUpService = UMAAFeatureLookUpService;
        this.bigtableTableAdminClientMap = bigtableTableAdminClientMap;
    }

    @Operation(summary = "Gets the data from given table for the given member id")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "retrieved data for given member id",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = String.class))}),
            @ApiResponse(responseCode = "404", description = "No Data Present for the given RowID in the table",
                    content = @Content),
            @ApiResponse(responseCode = "500", description = "Internal Server error",
                    content = @Content)})
    @GetMapping("/{instanceID}/readCellData")
    public ResponseEntity<String> retrieveDataFromGivenTableForGivenId(@PathVariable String instanceID, @RequestParam String tableName, @RequestParam(name = "id") String id) {
        try {
            logger.info("Retrieving data from table {} for given id {}", kv("tableName", tableName), kv("id", id));
            String cellData = UMAAFeatureLookUpService.readCellDataById(instanceID, tableName, id);
            return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(cellData);
        } catch (KVLookUpException umfe) {
            return ResponseEntity.status(umfe.getStatus()).contentType(MediaType.TEXT_PLAIN).body(umfe.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            if (e instanceof NotFoundException) {
                logger.error("Error occurred when getting tableName, Table not found in the bigtable");
                return ResponseEntity.status(HttpStatus.NOT_FOUND).contentType(MediaType.TEXT_PLAIN).body("NOT FOUND");
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).contentType(MediaType.TEXT_PLAIN).body(e.getMessage());
            }
        }
    }

    @Operation(summary = "create table in GCP Bigtable")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Table created successfully"),
            @ApiResponse(responseCode = "400", description = "Empty list passed in for table creation"),
            @ApiResponse(responseCode = "500", description = "Internal Server error",
                    content = @Content)})
    @PostMapping(value = "/{instanceID}/createTable")
    public ResponseEntity<Object> createTable(@PathVariable String instanceID, @RequestBody GCPBigtableTable gcpBigtableTable) throws Exception {
        try {
            String tableName = gcpBigtableTable.getTableName();
            if (!bigtableTableAdminClientMap.get(instanceID).exists(tableName)) {
                logger.info("Creating table {}", kv("table", tableName));
                UMAAFeatureLookUpService.createTable(instanceID, gcpBigtableTable);
                return ResponseEntity.status(HttpStatus.OK).build();
            } else {
                return ResponseEntity.status(HttpStatus.IM_USED).body("Table with same name exists in bigtable already");
            }
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).contentType(MediaType.TEXT_PLAIN).body(e.getMessage());
        }
    }

    @Operation(summary = "Deletes table from GCP Bigtable")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Table Deleted successfully"),
            @ApiResponse(responseCode = "400", description = "Empty list passed in for table Deletion"),
            @ApiResponse(responseCode = "500", description = "Internal Server error")})
    @DeleteMapping(value = "/{instanceID}/deleteTable")
    public ResponseEntity<Object> deleteTable(@PathVariable String instanceID, @RequestBody TableConfig tableConfig) {
        List<String> tableList = tableConfig.getTableList();
        if (!CollectionUtils.isEmpty(tableList)) {
            logger.info("deleting tables {}", kv("tables", tableConfig));
            UMAAFeatureLookUpService.deleteTable(instanceID, tableList);
            return ResponseEntity.status(HttpStatus.OK).build();
        } else {
            logger.info("cannot create Bigtable table with no valid data");
            return ResponseEntity.badRequest().build();
        }

    }

    @Operation(summary = "Inserts a new record to GCP Bigtable")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "row inserted successfully"),
            @ApiResponse(responseCode = "500", description = "Internal Server error")})
    @PostMapping(value = "/{instanceID}/insertData")
    public ResponseEntity<Object> writeDataToTable(@PathVariable String instanceID, @RequestBody BigtableTableData tableData) {
        try {
            logger.info("writing data to table {} with id {}", kv("tableName", tableData.getTableName()), kv("id", tableData.getRowKeyId()));
            UMAAFeatureLookUpService.insertDataToTable(instanceID, tableData);
            return ResponseEntity.status(HttpStatus.OK).build();
        } catch (KVLookUpException umfe) {
            return ResponseEntity.status(umfe.getStatus()).contentType(MediaType.TEXT_PLAIN).body(umfe.getMessage());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).contentType(MediaType.TEXT_PLAIN).body(e.getMessage());
        }
    }

//    @Operation(summary = "creates Instance in GCP Bigtable")
//    @ApiResponses(value = {
//            @ApiResponse(responseCode = "200", description = "Instance created successfully")})
//    @PostMapping(value = "createInstance/{instanceId}")
//    public ResponseEntity<Object> createTable(@PathVariable String instanceId) throws Exception{
//            logger.info("Creating instanceId {}", kv("instanceId", instanceId));
//            bigtableService.createInstance(instanceId);
//            return ResponseEntity.status(HttpStatus.OK).build();
//    }
}

