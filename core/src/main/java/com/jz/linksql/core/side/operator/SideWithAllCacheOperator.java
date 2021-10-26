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


package com.jz.linksql.core.side.operator;

import com.jz.linksql.core.classloader.ClassLoaderManager;
import com.jz.linksql.core.side.BaseAllReqRow;
import com.jz.linksql.core.side.FieldInfo;
import com.jz.linksql.core.side.JoinInfo;
import com.jz.linksql.core.side.AbstractSideTableInfo;
import com.jz.linksql.core.util.PluginUtil;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

/**
 * get plugin which implement from RichFlatMapFunction
 * Date: 2021/9/18
 *
 *
 * @author xuchao
 */

public class SideWithAllCacheOperator {

    private static final String PATH_FORMAT = "%sallside";

    private static final String OPERATOR_TYPE = "All";

    private static BaseAllReqRow loadFlatMap(String sideType, String sqlRootDir, RowTypeInfo rowTypeInfo,
                                             JoinInfo joinInfo, List<FieldInfo> outFieldInfoList,
                                             AbstractSideTableInfo sideTableInfo, String pluginLoadMode) throws Exception {

        String pathOfType = String.format(PATH_FORMAT, sideType);
        String pluginJarPath = PluginUtil.getJarFileDirPath(pathOfType, sqlRootDir, pluginLoadMode);
        String className = PluginUtil.getSqlSideClassName(sideType, "side", OPERATOR_TYPE);

        return ClassLoaderManager.newInstance(pluginJarPath, (cl) -> cl.loadClass(className).asSubclass(BaseAllReqRow.class)
                .getConstructor(RowTypeInfo.class, JoinInfo.class, List.class, AbstractSideTableInfo.class)
                .newInstance(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }

    public static DataStream getSideJoinDataStream(DataStream inputStream, String sideType, String sqlRootDir, RowTypeInfo rowTypeInfo, JoinInfo joinInfo,
                                                   List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo, String pluginLoadMode) throws Exception {
        BaseAllReqRow allReqRow = loadFlatMap(sideType, sqlRootDir, rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo, pluginLoadMode);
        return inputStream.flatMap(allReqRow);
    }
}
