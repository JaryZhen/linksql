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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.types.Row;
import org.apache.kafka.common.requests.IsolationLevel;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @author: JaryZhen
 * @create: 2021/10/24
 */
public class KafkaConsumerFactory extends AbstractKafkaConsumerFactory {

    @Override
    public FlinkKafkaConsumerBase<Row> createKafkaTableSource(KafkaSourceTableInfo kafkaSourceTableInfo, TypeInformation<Row> typeInformation, Properties props) {
        KafkaSource kafkaSrc = null;
        if (kafkaSourceTableInfo.getTopicIsPattern()) {
            DeserializationMetricWrapper deserMetricWrapper = createDeserializationMetricWrapper(kafkaSourceTableInfo,
                    typeInformation,
                    (Calculate & Serializable) (subscriptionState, tp)
                            -> subscriptionState.requestOffsetReset(tp, IsolationLevel.READ_UNCOMMITTED));

            //kafkaSrc = new KafkaConsumer(Pattern.compile(kafkaSourceTableInfo.getTopic()), deserMetricWrapper, props);
            kafkaSrc = createKafkaSource();
        } else {
            DeserializationMetricWrapper deserMetricWrapper = createDeserializationMetricWrapper(kafkaSourceTableInfo,
                    typeInformation,
                    (Calculate & Serializable) (subscriptionState, tp)
                            -> subscriptionState.partitionLag(tp, IsolationLevel.READ_UNCOMMITTED));
            /// kafkaSrc = new KafkaConsumer(kafkaSourceTableInfo.getTopic(), deserMetricWrapper, props);
        }
        return kafkaSrc;
    }

    private KafkaSource createKafkaSource(DeserializationMetricWrapper deserializationMetricWrapper) {
        KafkaSource<Row> source = KafkaSource
                .builder()
                .setBootstrapServers("")
                .setGroupId("MyGroup")
                .setTopics(Arrays.asList("TOPIC1, TOPIC2"))
                .setDeserializer(deserializationMetricWrapper)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
        return source;

    }

}
