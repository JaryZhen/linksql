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



package com.jz.linksql.core.source;


import com.jz.linksql.core.classloader.ClassLoaderManager;
import com.jz.linksql.core.table.AbstractSourceParser;
import com.jz.linksql.core.table.AbstractSourceTableInfo;
import com.jz.linksql.core.util.DtStringUtil;
import com.jz.linksql.core.util.PluginUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * streamTableSource
 * Date: 2021/3/10
 *
 * @author Jaryzhen
 */

public class StreamSourceFactory {

    private static final String CURR_TYPE = "source";

    private static final String DIR_NAME_FORMAT = "%ssource";

    public static AbstractSourceParser getSqlParser(String pluginType, String sqlRootDir, String pluginLoadMode) throws Exception {

        String pluginJarPath = PluginUtil.getJarFileDirPath(String.format(DIR_NAME_FORMAT, pluginType), sqlRootDir, pluginLoadMode);
        String typeNoVersion = DtStringUtil.getPluginTypeWithoutVersion(pluginType);
        String className = PluginUtil.getSqlParserClassName(typeNoVersion, CURR_TYPE);
        return ClassLoaderManager.newInstance(pluginJarPath, (cl) -> {
            Class<?> sourceParser = cl.loadClass(className);
            if(!AbstractSourceParser.class.isAssignableFrom(sourceParser)){
                throw new RuntimeException("class " + sourceParser.getName() + " not subClass of AbsSourceParser");
            }
            return sourceParser.asSubclass(AbstractSourceParser.class).newInstance();
        });
    }

    /**
     * The configuration of the type specified data source
     * @param sourceTableInfo
     * @return
     */
    public static Table getStreamSource(AbstractSourceTableInfo sourceTableInfo, StreamExecutionEnvironment env,
                                        StreamTableEnvironment tableEnv, String sqlRootDir, String pluginLoadMode) throws Exception {

        String sourceTypeStr = sourceTableInfo.getType();
        String typeNoVersion = DtStringUtil.getPluginTypeWithoutVersion(sourceTypeStr);
        String pluginJarPath = PluginUtil.getJarFileDirPath(String.format(DIR_NAME_FORMAT, sourceTypeStr), sqlRootDir, pluginLoadMode);
        String className = PluginUtil.getGenerClassName(typeNoVersion, CURR_TYPE);

        return ClassLoaderManager.newInstance(pluginJarPath, (cl) -> {
            Class<?> sourceClass = cl.loadClass(className);
            if(!IStreamSourceGener.class.isAssignableFrom(sourceClass)){
                throw new RuntimeException("class " + sourceClass.getName() + " not subClass of IStreamSourceGener");
            }

            IStreamSourceGener sourceGener = sourceClass.asSubclass(IStreamSourceGener.class).newInstance();
            return (Table) sourceGener.genStreamSource(sourceTableInfo, env, tableEnv);
        });
    }
}
