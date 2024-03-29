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

package com.jz.linksql.core.util;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Jaryzhen
 * @date 2021-05-20
 */
public class RowDataComplete {

    public static void completeRow(ResultFuture<RowData> resultFuture, Row row) {
        RowData RowData = RowDataConvert.convertToBaseRow(row);
        resultFuture.complete(Collections.singleton(RowData));
    }

    public static void completeTupleRow(ResultFuture<Tuple2<Boolean, RowData>> resultFuture, Tuple2<Boolean, Row> tupleRow) {
        RowData baseRow = RowDataConvert.convertToBaseRow(tupleRow.f1);
        resultFuture.complete(Collections.singleton(new Tuple2<>(tupleRow.f0, baseRow)));
    }

    public static void completeRow(ResultFuture<RowData> resultFuture, List<Row> rowList) {

        List<RowData> baseRowList = Lists.newArrayList();
        for (Row row : rowList) {
            baseRowList.add(RowDataConvert.convertToBaseRow(row));
        }

        resultFuture.complete(baseRowList);
    }

    public static void completeTupleRow(ResultFuture<Tuple2<Boolean, RowData>> resultFuture, Collection<Tuple2<Boolean, Row>> tupleRowList) {
        List<Tuple2<Boolean, RowData>> baseRowList = Lists.newArrayList();
        for (Tuple2<Boolean, Row> rowTuple : tupleRowList) {
            baseRowList.add(new Tuple2<>(rowTuple.f0, RowDataConvert.convertToBaseRow(rowTuple.f1)));
        }
        resultFuture.complete(baseRowList);
    }

    public static void collectRow(Collector<RowData> out, Row row) {
        RowData baseRow = RowDataConvert.convertToBaseRow(row);
        out.collect(baseRow);
    }

    public static void collectTupleRow(Collector<Tuple2<Boolean, RowData>> out, Tuple2<Boolean, Row> tupleRow) {
        RowData baseRow = RowDataConvert.convertToBaseRow(tupleRow.f1);
        out.collect(Tuple2.of(tupleRow.f0, baseRow));
    }

}
