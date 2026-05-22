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

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBConfig;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSinkOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSinkOptions.CONTAINER;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSinkOptions.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSinkOptions.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSinkOptions.KEY;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSinkOptions.PRIMARY_CONNECTION_STRING;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSinkOptions.PRIMARY_KEY;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSinkOptions.SECONDARY_CONNECTION_STRING;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSinkOptions.SECONDARY_KEY;
import static org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBSinkOptions.URI;

@AutoService(Factory.class)
public class AzureCosmosDBSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "AzureCosmosDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(DATABASE, CONTAINER)
                .optional(
                        URI,
                        ENDPOINT,
                        KEY,
                        PRIMARY_KEY,
                        SECONDARY_KEY,
                        PRIMARY_CONNECTION_STRING,
                        SECONDARY_CONNECTION_STRING,
                        BATCH_SIZE,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () ->
                new AzureCosmosDBSink(
                        context.getCatalogTable(), new AzureCosmosDBConfig(context.getOptions()));
    }
}
