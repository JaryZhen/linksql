/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jz.linksql.core.parser;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.planner.delegation.PlannerContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Supplier;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

/**
 * 废弃。之后删除
 * Date: 2021/3/31
 *
 *
 * @author  Jaryzhen
 */
public class FlinkPlanner {

    private final TableConfig tableConfig = new TableConfig();
    private final Catalog catalog = new GenericInMemoryCatalog("MockCatalog", "default");
    private final CatalogManager catalogManager =
            CatalogManagerMocks.preparedCatalogManager().defaultCatalog("builtin", catalog).build();
    private final ModuleManager moduleManager = new ModuleManager();
    private final FunctionCatalog functionCatalog =
            new FunctionCatalog(tableConfig, catalogManager, moduleManager);
    private final PlannerContext plannerContext =
            new PlannerContext(
                    false,
                    tableConfig,
                    functionCatalog,
                    catalogManager,
                    asRootSchema(new CatalogManagerCalciteSchema(catalogManager, true)),
                    Collections.emptyList());

    private final Supplier<FlinkPlannerImpl> plannerSupplier =
            () ->
                    getPlannerContext()
                            .createFlinkPlanner(
                                    catalogManager.getCurrentCatalog(),
                                    catalogManager.getCurrentDatabase());
    private final Parser parser =
            new ParserImpl(
                    catalogManager,
                    plannerSupplier,
                    () -> plannerSupplier.get().parser(),
                    getPlannerContext().getSqlExprToRexConverterFactory());

    public FlinkPlanner() {
        catalogManager.initSchemaResolver(
                true,
                ExpressionResolverMocks.basicResolver(catalogManager, functionCatalog, parser));
    }

    private PlannerContext getPlannerContext() {
        return plannerContext;
    }

    public CalciteParser getParser() {
        return getParserBySqlDialect(SqlDialect.DEFAULT);
    }

    private CalciteParser getParserBySqlDialect(SqlDialect sqlDialect) {
        tableConfig.setSqlDialect(sqlDialect);
        return plannerContext.createCalciteParser();
    }
}
