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

import com.sun.rowset.internal.BaseRow;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.util.DataFormatConverters;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.Row;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;

/**
 *
 *
 * @author xuchao
 * @date 2021-05-20
 */
public class RowDataConvert {

    public static RowData convertToBaseRow(Row row) {
        int length = row.getArity();
        GenericRowData genericRow = new GenericRowData(length);
        for (int i = 0; i < length; i++) {
            if (row.getField(i) instanceof String) {
                genericRow.setField(i, BinaryStringData.fromString((String) row.getField(i)));
            } else if (row.getField(i) instanceof Timestamp) {
                TimestampData newTimestamp = TimestampData.fromTimestamp(((Timestamp) row.getField(i)));
                genericRow.setField(i, newTimestamp);
            } else if (row.getField(i) instanceof Time) {
                genericRow.setField(i, DataFormatConverters.TimeConverter.INSTANCE.toInternal((Time) row.getField(i)));
            } else if (row.getField(i) instanceof Double) {
                genericRow.setField(i, DataFormatConverters.DoubleConverter.INSTANCE.toInternal((Double) row.getField(i)));
            } else if (row.getField(i) instanceof Float) {
                genericRow.setField(i, DataFormatConverters.FloatConverter.INSTANCE.toInternal((Float) row.getField(i)));
            } else if (row.getField(i) instanceof Long) {
                genericRow.setField(i, DataFormatConverters.LongConverter.INSTANCE.toInternal((Long) row.getField(i)));
            } else if (row.getField(i) instanceof Date) {
                genericRow.setField(i, DataFormatConverters.DateConverter.INSTANCE.toInternal((Date) row.getField(i)));
            } else if (row.getField(i) instanceof LocalDate) {
                genericRow.setField(i, DataFormatConverters.LocalDateConverter.INSTANCE.toInternal((LocalDate) row.getField(i)));
            } else {
                genericRow.setField(i, row.getField(i));
            }
        }

        return genericRow;
    }
}
