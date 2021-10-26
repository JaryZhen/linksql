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
import org.apache.flink.types.Row;

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
        String offerset = kafkaSourceTableInfo.getOffsetReset();
        OffsetsInitializer offinin = OffsetsInitializer.earliest();

        if (kafkaSourceTableInfo.getTopicIsPattern()) {
            DeserializationMetricWrapper deserMetricWrapper = createDeserializationMetricWrapper(kafkaSourceTableInfo,
                    typeInformation,
                    (Calculate & Serializable) (subscriptionState, tp)
                            // -> subscriptionState.requestOffsetReset(tp, IsolationLevel.READ_UNCOMMITTED));
                            -> 0L);

            kafkaSrc = createKafkaSource(deserMetricWrapper, kafkaSourceTableInfo, offinin);
        } else {
            DeserializationMetricWrapper deserMetricWrapper = createDeserializationMetricWrapper(kafkaSourceTableInfo,
                    typeInformation,
                    (Calculate & Serializable) (subscriptionState, tp)
                            //-> subscriptionState.partitionLag(tp, IsolationLevel.READ_UNCOMMITTED));
                            -> 0L);
            kafkaSrc = createKafkaSource(deserMetricWrapper, kafkaSourceTableInfo, offinin);
        }
        return kafkaSrc;
    }

    private KafkaSource createKafkaSource(DeserializationMetricWrapper deserializationMetricWrapper, KafkaSourceTableInfo sourceTableInfo, OffsetsInitializer offsetsInitializer) {
        KafkaSource<Row> source = KafkaSource
                .<Row>builder()
                .setBootstrapServers(sourceTableInfo.getBootstrapServers())
                .setGroupId(sourceTableInfo.getGroupId())
                .setTopics(Arrays.asList(sourceTableInfo.getTopic()))
                .setDeserializer(deserializationMetricWrapper)
                .setStartingOffsets(offsetsInitializer)
                .build();
        return source;

    }

}
