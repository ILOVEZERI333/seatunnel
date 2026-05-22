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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBConfig;
import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.serialize.CosmosItemSerializer;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AzureCosmosDBSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private final AzureCosmosDBConfig config;
    private final CosmosSinkWriterClient sinkClient;
    private final CosmosItemSerializer serializer;
    private final Map<String, List<Map<String, Object>>> bufferByContainer = new HashMap<>();

    public AzureCosmosDBSinkWriter(AzureCosmosDBConfig config, CatalogTable catalogTable) {
        this(config, catalogTable.getSeaTunnelRowType(), new AzureCosmosDBSinkClient(config));
    }

    AzureCosmosDBSinkWriter(
            AzureCosmosDBConfig config,
            SeaTunnelRowType rowType,
            CosmosSinkWriterClient sinkClient) {
        this.config = config;
        this.sinkClient = sinkClient;
        this.serializer = new CosmosItemSerializer(rowType);
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        String containerName = row.getTableId();
        if (StringUtils.isBlank(containerName)) {
            containerName = config.getContainer();
        }
        bufferByContainer.computeIfAbsent(containerName, ignored -> new ArrayList<>());
        bufferByContainer.get(containerName).add(serializer.serialize(row));

        if (bufferByContainer.get(containerName).size() >= config.getBatchSize()) {
            flushContainer(containerName);
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        flush();
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        flush();
        sinkClient.close();
    }

    private void flush() {
        new ArrayList<>(bufferByContainer.keySet()).forEach(this::flushContainer);
    }

    private void flushContainer(String containerName) {
        List<Map<String, Object>> items = bufferByContainer.get(containerName);
        if (items == null || items.isEmpty()) {
            return;
        }
        sinkClient.upsertItems(containerName, items);
        items.clear();
    }
}
