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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AzureCosmosDBSinkWriterTest {

    @Test
    public void testWriteFlushesByBatchSize() throws Exception {
        RecordingSinkClient client = new RecordingSinkClient();
        AzureCosmosDBSinkWriter writer =
                new AzureCosmosDBSinkWriter(createConfig(2), createRowType(), client);

        writer.write(new SeaTunnelRow(new Object[] {"1", "alpha"}));
        writer.write(new SeaTunnelRow(new Object[] {"2", "beta"}));

        Assertions.assertEquals(1, client.upsertCalls.size());
        Assertions.assertEquals("orders", client.upsertCalls.get(0).containerName);
        Assertions.assertEquals(2, client.upsertCalls.get(0).items.size());
    }

    @Test
    public void testPrepareCommitUsesTableIdAsContainer() throws Exception {
        RecordingSinkClient client = new RecordingSinkClient();
        AzureCosmosDBSinkWriter writer =
                new AzureCosmosDBSinkWriter(createConfig(100), createRowType(), client);

        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"3", "gamma"});
        row.setTableId("orders-override");
        writer.write(row);
        writer.prepareCommit();

        Assertions.assertEquals(1, client.upsertCalls.size());
        Assertions.assertEquals("orders-override", client.upsertCalls.get(0).containerName);
        Assertions.assertEquals(1, client.upsertCalls.get(0).items.size());
    }

    @Test
    public void testCloseFlushesAndClosesClient() throws Exception {
        RecordingSinkClient client = new RecordingSinkClient();
        AzureCosmosDBSinkWriter writer =
                new AzureCosmosDBSinkWriter(createConfig(100), createRowType(), client);

        writer.write(new SeaTunnelRow(new Object[] {"4", "delta"}));
        writer.close();

        Assertions.assertEquals(1, client.upsertCalls.size());
        Assertions.assertTrue(client.closed);
    }

    private AzureCosmosDBConfig createConfig(int batchSize) {
        Map<String, Object> options = new HashMap<>();
        options.put("endpoint", "https://account.documents.azure.com:443/");
        options.put("primary_key", "account-key");
        options.put("database", "sales");
        options.put("container", "orders");
        options.put("batch_size", batchSize);
        return new AzureCosmosDBConfig(ReadonlyConfig.fromMap(options));
    }

    private SeaTunnelRowType createRowType() {
        return new SeaTunnelRowType(
                new String[] {"id", "name"},
                new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});
    }

    private static class RecordingSinkClient implements CosmosSinkWriterClient {
        private final List<UpsertCall> upsertCalls = new ArrayList<>();
        private boolean closed;

        @Override
        public void upsertItems(String containerName, List<Map<String, Object>> items) {
            upsertCalls.add(new UpsertCall(containerName, new ArrayList<>(items)));
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static class UpsertCall {
        private final String containerName;
        private final List<Map<String, Object>> items;

        private UpsertCall(String containerName, List<Map<String, Object>> items) {
            this.containerName = containerName;
            this.items = items;
        }
    }
}
