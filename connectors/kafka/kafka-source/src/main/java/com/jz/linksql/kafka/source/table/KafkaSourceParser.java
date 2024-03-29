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


package com.jz.linksql.kafka.source.table;

import com.jz.linksql.core.format.FormatType;
import com.jz.linksql.core.table.AbstractSourceParser;
import com.jz.linksql.core.table.AbstractTableInfo;
import com.jz.linksql.core.util.MathUtil;
import com.jz.linksql.kafka.source.enums.EKafkaOffset;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reason:
 * Date: 2021/10/18

 *
 * @author sishu.yss
 */
public class KafkaSourceParser extends AbstractSourceParser {
    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {

        KafkaSourceTableInfo kafkaSourceTableInfo = new KafkaSourceTableInfo();
        parseFieldsInfo(fieldsInfo, kafkaSourceTableInfo);

        kafkaSourceTableInfo.setName(tableName);
        kafkaSourceTableInfo.setType(MathUtil.getString(props.get(KafkaSourceTableInfo.TYPE_KEY.toLowerCase())));
        kafkaSourceTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(KafkaSourceTableInfo.PARALLELISM_KEY.toLowerCase())));
        kafkaSourceTableInfo.setBootstrapServers(MathUtil.getString(props.get(KafkaSourceTableInfo.BOOTSTRAPSERVERS_KEY.toLowerCase())));
        kafkaSourceTableInfo.setGroupId(MathUtil.getString(props.get(KafkaSourceTableInfo.GROUPID_KEY.toLowerCase())));
        kafkaSourceTableInfo.setTopic(MathUtil.getString(props.get(KafkaSourceTableInfo.TOPIC_KEY.toLowerCase())));
        kafkaSourceTableInfo.setTopicIsPattern(MathUtil.getBoolean(props.get(KafkaSourceTableInfo.TOPICISPATTERN_KEY.toLowerCase()), false));
        kafkaSourceTableInfo.setOffsetReset(MathUtil.getString(props.getOrDefault(KafkaSourceTableInfo.OFFSETRESET_KEY.toLowerCase(), EKafkaOffset.LATEST.name().toLowerCase())));
        kafkaSourceTableInfo.setCharsetName(MathUtil.getString(props.getOrDefault(KafkaSourceTableInfo.CHARSET_NAME_KEY.toLowerCase(),"UTF-8")));

        kafkaSourceTableInfo.setSchemaString(MathUtil.getString(props.get(KafkaSourceTableInfo.SCHEMA_STRING_KEY.toLowerCase())));
        kafkaSourceTableInfo.setFieldDelimiter(MathUtil.getString(props.getOrDefault(KafkaSourceTableInfo.CSV_FIELD_DELIMITER_KEY.toLowerCase(), "|")));
        kafkaSourceTableInfo.setSourceDataType(MathUtil.getString(props.getOrDefault(KafkaSourceTableInfo.SOURCE_DATA_TYPE_KEY.toLowerCase(), FormatType.DT_NEST.name())));

        if(props.containsKey(KafkaSourceTableInfo.TIMESTAMP_OFFSET.toLowerCase())){
            kafkaSourceTableInfo.setTimestampOffset(MathUtil.getLongVal(props.getOrDefault(KafkaSourceTableInfo.TIMESTAMP_OFFSET.toLowerCase(), System.currentTimeMillis())));
        }
        Map<String, String> kafkaParams = props.keySet().stream()
                .filter(key -> !key.isEmpty() && key.startsWith("kafka."))
                .collect(Collectors.toMap(
                        key -> key.substring(6), key -> props.get(key).toString())
                );

        kafkaSourceTableInfo.addKafkaParam(kafkaParams);
        kafkaSourceTableInfo.check();

        return kafkaSourceTableInfo;
    }
}
