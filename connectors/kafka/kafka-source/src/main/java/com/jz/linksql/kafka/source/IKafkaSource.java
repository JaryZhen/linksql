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

package com.jz.linksql.kafka.source;

import com.jz.linksql.core.table.AbstractSourceTableInfo;
import com.jz.linksql.core.util.DtStringUtil;
import com.jz.linksql.kafka.source.table.KafkaSourceTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @author: JaryZhen
 * @create: 2021-10-05 10:55
 * @description:
 **/
public class IKafkaSource extends AbstractKafkaSource {

    @Override
    public Table genStreamSource(AbstractSourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        KafkaSourceTableInfo kafkaSourceTableInfo = (KafkaSourceTableInfo) sourceTableInfo;
        String topicName = kafkaSourceTableInfo.getTopic();

        Long startTimestamp = null;
        if (DtStringUtil.isInteger(kafkaSourceTableInfo.getOffsetReset())) {
            startTimestamp = Long.parseLong(kafkaSourceTableInfo.getOffsetReset());
            kafkaSourceTableInfo.setOffsetReset("latest");
        }

        Properties kafkaProperties = getKafkaProperties(kafkaSourceTableInfo);
        TypeInformation<Row> typeInformation = getRowTypeInformation(kafkaSourceTableInfo);
        KafkaSource kafkaSrc =  new KafkaConsumerFactory()
                .createKafkaTableSource(kafkaSourceTableInfo, typeInformation, kafkaProperties);

        String sourceOperatorName = generateOperatorName(sourceTableInfo.getName(), topicName);
        //DataStreamSource kafkaSource = env.addSource(kafkaSrc, sourceOperatorName, typeInformation);

        DataStreamSource kafkaSource = env.fromSource(KafkaSource.builder().build(), WatermarkStrategy.noWatermarks(), sourceOperatorName);

        if (startTimestamp != null) {
            //kafkaSrc.setStartFromTimestamp(startTimestamp);
        } else {
            //setStartPosition(kafkaSourceTableInfo.getOffsetReset(), topicName, kafkaSrc);
        }
        setParallelism(kafkaSourceTableInfo.getParallelism(), kafkaSource);
//        setStartPosition(kafkaSourceTableInfo.getOffsetReset(), topicName, kafkaSrc, () -> kafkaSrc.setStartFromTimestamp(kafkaSourceTableInfo.getTimestampOffset()));
        String fields = StringUtils.join(kafkaSourceTableInfo.getFields(), ",");

        //return tableEnv.fromDataStream(kafkaSource, fields);
        return tableEnv.fromDataStream(kafkaSource, Schema
                .newBuilder()
                //.fromFields(kafkaSourceTableInfo.getFields(),typeInformation)
                .build());
    }
}
