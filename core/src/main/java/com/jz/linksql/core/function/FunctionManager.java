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


package com.jz.linksql.core.function;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.AggregateFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  自定义函数管理类
 * Reason:
 * Date: 2021/10/21
 *
 * @author Jaryzhen
 */

public class FunctionManager {
    private static final Logger logger = LoggerFactory.getLogger(FunctionManager.class);

    /**
     * TABLE|SCALA|AGGREGATE
     * 注册UDF到table env
     */
    public static void registerUDF(String type, String classPath, String funcName, TableEnvironment tableEnv, ClassLoader classLoader) {
        if ("SCALA".equalsIgnoreCase(type)) {
            registerScalaUDF(classPath, funcName, tableEnv, classLoader);
        } else if ("TABLE".equalsIgnoreCase(type)) {
            registerTableUDF(classPath, funcName, tableEnv, classLoader);
        } else if ("AGGREGATE".equalsIgnoreCase(type)) {
            registerAggregateUDF(classPath, funcName, tableEnv, classLoader);
        } else {
            throw new RuntimeException("not support of UDF which is not in (TABLE, SCALA, AGGREGATE)");
        }
    }

    /**
     * 注册自定义方法到env上
     * @param classPath
     * @param funcName
     * @param tableEnv
     */
    public static void registerScalaUDF(String classPath, String funcName, TableEnvironment tableEnv, ClassLoader classLoader) {
        try {
            ScalarFunction udfFunc = Class.forName(classPath, false, classLoader)
                    .asSubclass(ScalarFunction.class).newInstance();
            tableEnv.registerFunction(funcName, udfFunc);
            logger.info("register scala function:{} success.", funcName);
        } catch (Exception e) {
            logger.error("", e);
            throw new RuntimeException("register UDF exception:", e);
        }
    }

    /**
     * 注册自定义TABLEFFUNC方法到env上
     *
     * @param classPath
     * @param funcName
     * @param tableEnv
     */
    public static void registerTableUDF(String classPath, String funcName, TableEnvironment tableEnv, ClassLoader classLoader) {
        try {
            checkStreamTableEnv(tableEnv);
            TableFunction udtf = Class.forName(classPath, false, classLoader)
                    .asSubclass(TableFunction.class).newInstance();

            ((StreamTableEnvironment) tableEnv).registerFunction(funcName, udtf);
            logger.info("register table function:{} success.", funcName);
        } catch (Exception e) {
            logger.error("", e);
            throw new RuntimeException("register Table UDF exception:", e);
        }
    }

    private static void checkStreamTableEnv(TableEnvironment tableEnv) {
        if (!(tableEnv instanceof StreamTableEnvironment)) {
            throw new RuntimeException("no support tableEnvironment class for " + tableEnv.getClass().getName());
        }
    }

    /**
     * 注册自定义Aggregate FUNC方法到env上
     *
     * @param classPath
     * @param funcName
     * @param tableEnv
     */
    public static void registerAggregateUDF(String classPath, String funcName, TableEnvironment tableEnv, ClassLoader classLoader) {
        try {
            checkStreamTableEnv(tableEnv);

            AggregateFunction udaf = Class.forName(classPath, false, classLoader)
                    .asSubclass(AggregateFunction.class).newInstance();
            ((StreamTableEnvironment) tableEnv).registerFunction(funcName, udaf);
            logger.info("register Aggregate function:{} success.", funcName);
        } catch (Exception e) {
            logger.error("", e);
            throw new RuntimeException("register Aggregate UDF exception:", e);
        }
    }


    public static TypeInformation[] transformTypes(Class[] fieldTypes) {
        TypeInformation[] types = new TypeInformation[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            types[i] = TypeInformation.of(fieldTypes[i]);
        }

        return types;
    }
}
