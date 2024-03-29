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
package com.jz.linksql.kafka.sink;

import com.jz.linksql.core.format.FormatType;
import com.jz.linksql.core.format.SerializationMetricWrapper;
import com.jz.linksql.kafka.sink.serialization.AvroTuple2SerializationSchema;
import com.jz.linksql.kafka.sink.serialization.CsvTupleSerializationSchema;
import com.jz.linksql.kafka.sink.serialization.JsonTupleSerializationSchema;
import com.jz.linksql.kafka.sink.table.KafkaSinkTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.types.Row;

import java.util.Optional;
import java.util.Properties;

/**
 * 抽象的kafka producer 的工厂类
 * 包括序统一的序列化工具的构造

 * @author: JaryZhen
 * create: 2021/10/24
 */
public abstract class AbstractKafkaProducerFactory {

    /**
     *  获取具体的KafkaProducer
     * eg create KafkaProducer010
     * @param kafkaSinkTableInfo
     * @param typeInformation
     * @param properties
     * @param partitioner
     * @return
     */
    public abstract RichSinkFunction<Tuple2<Boolean,Row>> createKafkaProducer(KafkaSinkTableInfo kafkaSinkTableInfo, TypeInformation<Tuple2<Boolean,Row>> typeInformation,
                                                                              Properties properties, Optional<FlinkKafkaPartitioner<Tuple2<Boolean,Row>>> partitioner, String[] partitionKeys);

    protected SerializationMetricWrapper createSerializationMetricWrapper(KafkaSinkTableInfo kafkaSinkTableInfo, TypeInformation<Tuple2<Boolean,Row>> typeInformation) {
        SerializationSchema<Tuple2<Boolean,Row>> serializationSchema = createSerializationSchema(kafkaSinkTableInfo, typeInformation);
        return new SerializationMetricWrapper(serializationSchema);
    }

    private SerializationSchema<Tuple2<Boolean,Row>> createSerializationSchema(KafkaSinkTableInfo kafkaSinkTableInfo, TypeInformation<Tuple2<Boolean,Row>> typeInformation) {
        SerializationSchema<Tuple2<Boolean,Row>> serializationSchema = null;
        if (FormatType.JSON.name().equalsIgnoreCase(kafkaSinkTableInfo.getSinkDataType())) {
            if (StringUtils.isNotBlank(kafkaSinkTableInfo.getSchemaString())) {
                serializationSchema = new JsonTupleSerializationSchema(kafkaSinkTableInfo.getSchemaString(), kafkaSinkTableInfo.getUpdateMode());
            } else if (typeInformation != null && typeInformation.getArity() != 0) {
                serializationSchema = new JsonTupleSerializationSchema(typeInformation, kafkaSinkTableInfo.getUpdateMode());
            } else {
                throw new IllegalArgumentException("sinkDataType:" + FormatType.JSON.name() + " must set schemaString（JSON Schema）or TypeInformation<Row>");
            }
        } else if (FormatType.CSV.name().equalsIgnoreCase(kafkaSinkTableInfo.getSinkDataType())) {
            if (StringUtils.isBlank(kafkaSinkTableInfo.getFieldDelimiter())) {
                throw new IllegalArgumentException("sinkDataType:" + FormatType.CSV.name() + " must set fieldDelimiter");
            }
            final CsvTupleSerializationSchema.Builder serSchemaBuilder = new CsvTupleSerializationSchema.Builder(typeInformation);
            serSchemaBuilder.setFieldDelimiter(kafkaSinkTableInfo.getFieldDelimiter().toCharArray()[0]);
            serSchemaBuilder.setUpdateMode(kafkaSinkTableInfo.getUpdateMode());

            serializationSchema = serSchemaBuilder.build();
        } else if (FormatType.AVRO.name().equalsIgnoreCase(kafkaSinkTableInfo.getSinkDataType())) {
            if (StringUtils.isBlank(kafkaSinkTableInfo.getSchemaString())) {
                throw new IllegalArgumentException("sinkDataType:" + FormatType.AVRO.name() + " must set schemaString");
            }
            serializationSchema = new AvroTuple2SerializationSchema(kafkaSinkTableInfo.getSchemaString(), kafkaSinkTableInfo.getUpdateMode());
        }

        if (null == serializationSchema) {
            throw new UnsupportedOperationException("FormatType:" + kafkaSinkTableInfo.getSinkDataType());
        }

        return serializationSchema;
    }

}
