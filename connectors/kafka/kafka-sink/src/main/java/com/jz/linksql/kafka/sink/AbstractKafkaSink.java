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

import com.jz.linksql.core.enums.EUpdateMode;
import com.jz.linksql.core.sink.IStreamSinkGener;
import com.jz.linksql.kafka.sink.table.KafkaSinkTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * Date: 2021/3/30

 *
 * @author  Jaryzhen
 */
public abstract class AbstractKafkaSink implements RetractStreamTableSink<Row>, IStreamSinkGener {
    public static final String SINK_OPERATOR_NAME_TPL = "${topic}_${table}";

    protected String[] fieldNames;
    protected TypeInformation<?>[] fieldTypes;

    protected String[] partitionKeys;
    protected String sinkOperatorName;
    protected Properties properties;
    protected int parallelism;
    protected String topic;
    protected String tableName;
    protected String updateMode;

    protected TableSchema schema;
    protected SinkFunction<Tuple2<Boolean, Row>> kafkaProducer011;

    protected Optional<FlinkKafkaPartitioner<Tuple2<Boolean, Row>>> partitioner;

    protected Properties getKafkaProperties(KafkaSinkTableInfo KafkaSinkTableInfo) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSinkTableInfo.getBootstrapServers());

        for (String key : KafkaSinkTableInfo.getKafkaParamKeys()) {
            props.setProperty(key, KafkaSinkTableInfo.getKafkaParam(key));
        }
        return props;
    }

    protected TypeInformation[] getTypeInformations(KafkaSinkTableInfo kafka11SinkTableInfo) {
        Class<?>[] fieldClasses = kafka11SinkTableInfo.getFieldClasses();
        TypeInformation[] types = IntStream.range(0, fieldClasses.length)
                .mapToObj(i -> TypeInformation.of(fieldClasses[i]))
                .toArray(TypeInformation[]::new);
        return types;
    }


    protected TableSchema buildTableSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        Preconditions.checkArgument(fieldNames.length == fieldTypes.length, "fieldNames length must equals fieldTypes length !");

        DataType[] dataTypes = IntStream.range(0, fieldTypes.length)
                .mapToObj(i -> fromLegacyInfoToDataType(fieldTypes[i]))
                .toArray(DataType[]::new);

        TableSchema tableSchema = TableSchema.builder()
                .fields(fieldNames, dataTypes)
                .build();
        return tableSchema;
    }

    protected String[] getPartitionKeys(KafkaSinkTableInfo kafkaSinkTableInfo) {
        if (StringUtils.isNotBlank(kafkaSinkTableInfo.getPartitionKeys())) {
            return StringUtils.split(kafkaSinkTableInfo.getPartitionKeys(), ',');
        }
        return null;
    }

    @Override
    public DataStreamSink<Tuple2<Boolean, Row>> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        if (updateMode.equalsIgnoreCase(EUpdateMode.APPEND.name())) {
            dataStream = dataStream.filter((Tuple2<Boolean, Row> record) -> record.f0);
        }
        DataStreamSink<Tuple2<Boolean, Row>> dataStreamSink = dataStream.addSink(kafkaProducer011).name(sinkOperatorName);
        if (parallelism > 0) {
            dataStreamSink.setParallelism(parallelism);
        }
        return dataStreamSink;
    }

    //@Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), new RowTypeInfo(fieldTypes, fieldNames));
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }
}
