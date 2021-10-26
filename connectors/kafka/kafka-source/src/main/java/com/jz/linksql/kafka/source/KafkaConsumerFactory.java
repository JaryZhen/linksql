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

import com.jz.linksql.core.format.DeserializationMetricWrapper;
import com.jz.linksql.kafka.source.table.KafkaSourceTableInfo;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author: JaryZhen
 * @create: 2021/10/24
 */
public class KafkaConsumerFactory extends AbstractKafkaConsumerFactory {

    @Override
    public KafkaSource<Row> createKafkaTableSource(KafkaSourceTableInfo kafkaSourceTableInfo, TypeInformation<Row> typeInformation, Properties props) {
        KafkaSource kafkaSrc = null;
        if (kafkaSourceTableInfo.getTopicIsPattern()) {
            DeserializationMetricWrapper deserMetricWrapper = createDeserializationMetricWrapper(kafkaSourceTableInfo,
                    typeInformation,
                    (Calculate & Serializable) (subscriptionState, tp)
                            // -> subscriptionState.requestOffsetReset(tp, IsolationLevel.READ_UNCOMMITTED));
                            -> 0L);

            kafkaSrc = createKafkaSource(deserMetricWrapper, kafkaSourceTableInfo);
        } else {
            DeserializationMetricWrapper deserMetricWrapper = createDeserializationMetricWrapper(kafkaSourceTableInfo,
                    typeInformation,
                    (Calculate & Serializable) (subscriptionState, tp)
                            //-> subscriptionState.partitionLag(tp, IsolationLevel.READ_UNCOMMITTED));
                            -> 0L);
            kafkaSrc = createKafkaSource(deserMetricWrapper, kafkaSourceTableInfo);
        }
        return kafkaSrc;
    }

    private KafkaSource createKafkaSource(DeserializationMetricWrapper deserializationMetricWrapper, KafkaSourceTableInfo sourceTableInfo) {
        KafkaSource<Row> source = KafkaSource
                .builder()
                .setBootstrapServers(sourceTableInfo.getBootstrapServers())
                .setGroupId(sourceTableInfo.getGroupId())
                .setTopics(Arrays.asList(sourceTableInfo.getTopic()))
                .setDeserializer(deserializationMetricWrapper)
                .setStartingOffsets(sourceTableInfo.getOffsetReset())
                .build();
        return source;

    }

}
