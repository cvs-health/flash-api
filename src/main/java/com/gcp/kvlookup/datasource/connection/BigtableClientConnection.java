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

package com.gcp.kvlookup.datasource.connection;

import com.gcp.kvlookup.exception.BigtableAdminClientConnectionException;
import com.gcp.kvlookup.exception.BigtableDataClientConnectionException;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.logstash.logback.argument.StructuredArguments.kv;

@Configuration
public class BigtableClientConnection {

    private static final Logger logger = LoggerFactory.getLogger(BigtableClientConnection.class);

    @Value("${gcp.projectId}")
    private String projectId;   // GCP_PROJECTID

    @Value("#{'${gcp.instanceId.list}'.split(',')}")
    private List<String> instanceIdList; // GCP_INSTANCEID_LIST

    private Map<String, BigtableTableAdminClient> adminClientMap = new HashMap<>();
    ;
    private Map<String, BigtableDataClient> dataClientMap = new HashMap<>();
    private BigtableInstanceAdminClient bigtableInstanceAdminClient;


    @Bean
    public Map<String, BigtableDataClient> getDataClientMap() {
        try {
            for (String instanceId : instanceIdList) {
                logger.info("Establishing connecting to Bigtable dataClient with params projectId and InstanceId {} {}", kv("projectId", projectId), kv("instanceId", instanceId));
                BigtableDataSettings settings = BigtableDataSettings.newBuilder().setProjectId(projectId).setInstanceId(instanceId).build();
                BigtableDataClient bigtableDataClient = BigtableDataClient.create(settings);
                logger.info("connection established successfully with {} and {}", kv("projectId", projectId), kv("instanceId", instanceId));
                dataClientMap.put(instanceId, bigtableDataClient);
            }
        } catch (Exception e) {
            String errorMessage = "Error occurred when tried to create instance of BigtableDataClient using the given projectId and InstanceId";
            logger.error(errorMessage);
            throw new BigtableDataClientConnectionException(errorMessage, e);
        }
        return dataClientMap;
    }


    @Bean
    public Map<String, BigtableTableAdminClient> getBigtableAdminClientMap() {
        try {
            for (String instanceId : instanceIdList) {
                logger.info("Establishing connecting to Bigtable adminClient with params projectId and InstanceId {} {}", kv("projectId", projectId), kv("instanceId", instanceId));
                BigtableTableAdminSettings settings = BigtableTableAdminSettings.newBuilder().setProjectId(projectId).setInstanceId(instanceId).build();
                BigtableTableAdminClient bigtableTableAdminClient = BigtableTableAdminClient.create(settings);
                logger.info("connection established successfully with {} and {} ", kv("projectId", projectId), kv("instanceId", instanceId));
                adminClientMap.put(instanceId, bigtableTableAdminClient);
            }
        } catch (Exception e) {
            String errorMessage = "Error occurred when tried to create instance of BigtableAdminClient using the given projectId and InstanceId";
            logger.error(errorMessage);
            throw new BigtableAdminClientConnectionException(errorMessage, e);
        }
        return adminClientMap;
    }

    @Bean
    public BigtableInstanceAdminClient getBigtableInstanceAdminClient() {
        try {
            logger.info("Establishing connecting to Bigtable instanceAdminClient with projectId  {}", kv("projectId", projectId));
            BigtableInstanceAdminSettings instanceAdminSettings =
                    BigtableInstanceAdminSettings.newBuilder().setProjectId(projectId).build();
            bigtableInstanceAdminClient = BigtableInstanceAdminClient.create(instanceAdminSettings);
            logger.info("connection established successfully with {}", kv("projectId", projectId));
            return bigtableInstanceAdminClient;
        } catch (Exception e) {
            String errorMessage = "Error occurred when tried to create instance of BigtableInstanceAdminClient using the given projectId";
            logger.error(errorMessage);
            throw new BigtableAdminClientConnectionException(errorMessage, e);
        }
    }

}
