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

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Custom watermark --- for eventtime
 * Date: 2021/10/25
 *
 * @author xuchao
 */

public class CustomerWaterMarkerForTimeStamp extends AbstractCustomerWaterMarker<Row> {

    private static final Logger logger = LoggerFactory.getLogger(CustomerWaterMarkerForTimeStamp.class);

    public CustomerWaterMarkerForTimeStamp(Time maxOutOfOrderness, int pos) {
        super(maxOutOfOrderness);
        this.pos = pos;
    }

    @Override
    public long extractTimestamp(Row row) {
        try {
            Timestamp time = (Timestamp) row.getField(pos);
            long extractTime=time.getTime();
            return getExtractTimestamp(extractTime);
        } catch (RuntimeException e) {
            logger.error("", e);
        }
        return lastTime;
    }

}
