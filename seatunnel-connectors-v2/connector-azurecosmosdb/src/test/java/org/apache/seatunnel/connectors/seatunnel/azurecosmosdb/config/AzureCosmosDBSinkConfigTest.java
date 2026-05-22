/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AzureCosmosDBSinkConfigTest {

    @Test
    public void testResolveEndpointAndKeyWithoutSchema() {
        AzureCosmosDBConfig config =
                new AzureCosmosDBConfig(ReadonlyConfig.fromMap(buildSinkOptions()));

        Assertions.assertEquals(
                "https://sink-account.documents.azure.com:443/", config.getResolvedEndpoint());
        Assertions.assertEquals("sink-primary-key", config.getResolvedKey());
    }

    @Test
    public void testSinkBatchSizeDefault() {
        AzureCosmosDBConfig config =
                new AzureCosmosDBConfig(ReadonlyConfig.fromMap(buildSinkOptions()));

        Assertions.assertEquals(100, config.getBatchSize());
    }

    private Map<String, Object> buildSinkOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put("endpoint", "https://sink-account.documents.azure.com:443/");
        options.put("primary_key", "sink-primary-key");
        options.put("database", "sink-db");
        options.put("container", "sink-container");
        return options;
    }
}
