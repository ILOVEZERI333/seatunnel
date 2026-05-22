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

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.sink;

import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBConfig;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;

import java.util.List;
import java.util.Map;

public class AzureCosmosDBSinkClient implements CosmosSinkWriterClient {

    private final AzureCosmosDBConfig config;
    private final CosmosClient client;

    public AzureCosmosDBSinkClient(AzureCosmosDBConfig config) {
        this(
                config,
                new CosmosClientBuilder()
                        .endpoint(config.getResolvedEndpoint())
                        .key(config.getResolvedKey())
                        .buildClient());
    }

    AzureCosmosDBSinkClient(AzureCosmosDBConfig config, CosmosClient client) {
        this.config = config;
        this.client = client;
    }

    @Override
    public void upsertItems(String containerName, List<Map<String, Object>> items) {
        CosmosContainer container =
                client.getDatabase(config.getDatabase()).getContainer(containerName);
        for (Map<String, Object> item : items) {
            container.upsertItem(item);
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
