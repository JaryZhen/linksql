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



package com.jz.linksql.core.table;

import com.jz.linksql.core.enums.ETableType;
import com.jz.linksql.core.parser.CreateTableParser;
import com.jz.linksql.core.side.AbstractSideTableInfo;
import com.jz.linksql.core.side.StreamSideFactory;
import com.jz.linksql.core.sink.StreamSinkFactory;
import com.jz.linksql.core.source.StreamSourceFactory;
import com.jz.linksql.core.util.MathUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Create table statement parsing table structure to obtain specific information
 * Date: 2021/6/25
 *
 * @author Jaryzhen
 */

public class AbstractTableInfoParser {

    private final static String TYPE_KEY = "'connector'";

    private final static String SIDE_TABLE_SIGN = "(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$";

    private final static Pattern SIDE_PATTERN = Pattern.compile(SIDE_TABLE_SIGN);

    private  Map<String, AbstractTableParser> sourceTableInfoMap = Maps.newConcurrentMap();

    private  Map<String, AbstractTableParser> targetTableInfoMap = Maps.newConcurrentMap();

    private  Map<String, AbstractTableParser> sideTableInfoMap = Maps.newConcurrentMap();

    //Parsing loaded plugin
    public AbstractTableInfo parseWithTableType(int tableType, CreateTableParser.SqlParserResult parserResult,
                                                String localPluginRoot, String pluginLoadMode) throws Exception {
        AbstractTableParser absTableParser = null;
        Map<String, Object> props = parserResult.getPropMap();
        String type = MathUtil.getString(props.get(TYPE_KEY));

        if(Strings.isNullOrEmpty(type)){
            throw new RuntimeException("create table statement requires property of type");
        }

        if(tableType == ETableType.SOURCE.getType()){
            //TODO: validation side
            boolean isSideTable = checkIsSideTable(parserResult.getFieldsInfoStr());

            if(!isSideTable){
                absTableParser = sourceTableInfoMap.get(type);
                if(absTableParser == null){
                    absTableParser = StreamSourceFactory.getSqlParser(type, localPluginRoot, pluginLoadMode);
                    sourceTableInfoMap.put(type, absTableParser);
                }
            }else{
                absTableParser = sideTableInfoMap.get(type);
                if(absTableParser == null){
                    String cacheType = MathUtil.getString(props.get(AbstractSideTableInfo.CACHE_KEY));
                    absTableParser = StreamSideFactory.getSqlParser(type, localPluginRoot, cacheType, pluginLoadMode);
                    sideTableInfoMap.put(type + cacheType, absTableParser);
                }
            }

        }else if(tableType == ETableType.SINK.getType()){
            absTableParser = targetTableInfoMap.get(type);
            if(absTableParser == null){
                absTableParser = StreamSinkFactory.getSqlParser(type, localPluginRoot, pluginLoadMode);
                targetTableInfoMap.put(type, absTableParser);
            }
        }

        if(absTableParser == null){
            throw new RuntimeException(String.format("not support %s type of table", type));
        }

        Map<String, Object> prop = Maps.newHashMap();

        //Shield case
        parserResult.getPropMap().forEach((key,val) -> prop.put(key.toLowerCase(), val));

        return absTableParser.getTableInfo(parserResult.getTableName(), parserResult.getFieldsInfoStr(), prop);
    }

    /**
     * judge dim table of PERIOD FOR SYSTEM_TIME
     * @param tableField
     * @return
     */
    private static boolean checkIsSideTable(String tableField){
        String[] fieldInfos = StringUtils.split(tableField, ",");
        for(String field : fieldInfos){
            Matcher matcher = SIDE_PATTERN.matcher(field.trim());
            if(matcher.find()){
                return true;
            }
        }

        return false;
    }
}
