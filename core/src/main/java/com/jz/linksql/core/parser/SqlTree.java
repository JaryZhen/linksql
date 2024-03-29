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


import com.jz.linksql.core.table.AbstractTableInfo;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * parser sql to get the Sql Tree structure
 * Date: 2021/6/25
 *
 * @author Jaryzhen
 */

public class SqlTree {

    private List<CreateFuncParser.SqlParserResult> functionList = Lists.newArrayList();

    //create table details
    private Map<String, CreateTableParser.SqlParserResult> preDealTableMap = Maps.newHashMap();

    private Map<String, AbstractTableInfo> tableInfoMap = Maps.newLinkedHashMap();

    private List<InsertSqlParser.SqlParseResult> execSqlList = Lists.newArrayList();

    private List<CreateTmpTableParser.SqlParserResult> tmpSqlList = Lists.newArrayList();

    private Map<String, CreateTmpTableParser.SqlParserResult> tmpTableMap = Maps.newHashMap();

    public List<CreateFuncParser.SqlParserResult> getFunctionList() {
        return functionList;
    }

    public Map<String, CreateTableParser.SqlParserResult> getPreDealTableMap() {
        return preDealTableMap;
    }

    public Map<String, CreateTmpTableParser.SqlParserResult> getTmpTableMap() {
        return tmpTableMap;
    }

    public List<InsertSqlParser.SqlParseResult> getExecSqlList() {
        return execSqlList;
    }

    public void addFuncTableInfo(CreateFuncParser.SqlParserResult func){
        functionList.add(func);
    }

    public void addCreateTableInfo(String tableName, CreateTableParser.SqlParserResult table){
        preDealTableMap.put(tableName, table);
    }

    public void addTmplTableInfo(String tableName, CreateTmpTableParser.SqlParserResult table){
        tmpTableMap.put(tableName, table);
    }
    public void addTmpSql(CreateTmpTableParser.SqlParserResult tmpSql){
        tmpSqlList.add(tmpSql);
    }
    public void addExecSql(InsertSqlParser.SqlParseResult execSql){
        execSqlList.add(execSql);
    }

    public List<CreateTmpTableParser.SqlParserResult> getTmpSqlList(){
        return tmpSqlList;
    }

    public Map<String, AbstractTableInfo> getTableInfoMap() {
        return tableInfoMap;
    }

    public void addTableInfo(String tableName, AbstractTableInfo tableInfo){
        tableInfoMap.put(tableName, tableInfo);
    }
}
