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

package com.jz.linksql.core.watermarker;

import com.jz.linksql.core.table.AbstractSourceTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import com.google.common.base.Strings;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import java.sql.Timestamp;
/**
 * define watermarker
 * Date: 2021/6/29
 *
 * @author Jaryzhen
 */

public class WaterMarkerAssigner {

    public boolean checkNeedAssignWaterMarker(AbstractSourceTableInfo tableInfo){
        return !Strings.isNullOrEmpty(tableInfo.getEventTimeField());
    }

    public DataStream assignWaterMarker(DataStream<Row> dataStream, RowTypeInfo typeInfo, AbstractSourceTableInfo sourceTableInfo){

        String eventTimeFieldName = sourceTableInfo.getEventTimeField();

        int maxOutOrderness = sourceTableInfo.getMaxOutOrderness();

        String[] fieldNames = typeInfo.getFieldNames();
        TypeInformation<?>[] fieldTypes = typeInfo.getFieldTypes();

        if(Strings.isNullOrEmpty(eventTimeFieldName)){
            return dataStream;
        }

        int pos = -1;
        for(int i=0; i<fieldNames.length; i++){
            if(eventTimeFieldName.equals(fieldNames[i])){
                pos = i;
            }
        }

        Preconditions.checkState(pos != -1, "can not find specified eventTime field:" +
                eventTimeFieldName + " in defined fields.");

        TypeInformation fieldType = fieldTypes[pos];

        AbstractCustomerWaterMarker waterMarker = null;
        if(fieldType.getTypeClass().isAssignableFrom(Timestamp.class)){
            waterMarker = new CustomerWaterMarkerForTimeStamp(Time.milliseconds(maxOutOrderness), pos);
        }else if(fieldType.getTypeClass().isAssignableFrom(Long.class)){
            waterMarker = new CustomerWaterMarkerForLong(Time.milliseconds(maxOutOrderness), pos);
        }else{
            throw new IllegalArgumentException("not support type of " + fieldType + ", current only support(timestamp, long).");
        }

        String fromTag = "Source:" + sourceTableInfo.getName();
        waterMarker.setFromSourceTag(fromTag);
        return dataStream.assignTimestampsAndWatermarks(waterMarker);
    }
}
