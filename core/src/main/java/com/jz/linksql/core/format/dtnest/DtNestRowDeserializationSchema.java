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

package com.jz.linksql.core.format.dtnest;

import com.jz.linksql.core.table.AbstractTableInfo;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.NullNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * source data parse to json format
 * <p>
 * Date: 2021/10/12
 *
 *
 * @author  Jaryzhen
 */
public class DtNestRowDeserializationSchema extends AbstractDeserializationSchema<Row> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Map<String, String> rowAndFieldMapping;
//    private final Map<String, JsonNode> nodeAndJsonNodeMapping = Maps.newHashMap();
    private final Map<Long, Map<String, JsonNode>> nodeAndJsonNodeMappingForThread = Maps.newHashMap();

    private final String[] fieldNames;
    private final TypeInformation<?>[] fieldTypes;
    private TypeInformation<Row> typeInfo;
    private final List<AbstractTableInfo.FieldExtraInfo> fieldExtraInfos;
    private final String charsetName;

    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("^\\d+$");
    private static final Pattern TIME_FORMAT_PATTERN = Pattern.compile("\\w+\\d+:\\d+:\\d+");

    public DtNestRowDeserializationSchema(TypeInformation<Row> typeInfo, Map<String, String> rowAndFieldMapping,
                                          List<AbstractTableInfo.FieldExtraInfo> fieldExtraInfos,
                                          String charsetName) {
        this.typeInfo = typeInfo;
        this.fieldNames = ((RowTypeInfo) typeInfo).getFieldNames();
        this.fieldTypes = ((RowTypeInfo) typeInfo).getFieldTypes();
        this.rowAndFieldMapping = rowAndFieldMapping;
        this.fieldExtraInfos = fieldExtraInfos;
        this.charsetName = charsetName;
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        //可能会并发访问
        if(!nodeAndJsonNodeMappingForThread.containsKey(Thread.currentThread().getId())){
            nodeAndJsonNodeMappingForThread.put(Thread.currentThread().getId(), Maps.newHashMap());
        }
        String decoderStr = new String(message, charsetName);
        final JsonNode root = objectMapper.readTree(decoderStr);
        this.parseTree(root, null);
        return convertTopRow();
    }

    private void parseTree(JsonNode jsonNode, String prefix) {
        Map<String, JsonNode> nodeAndJsonNodeMapping = nodeAndJsonNodeMappingForThread.get(Thread.currentThread().getId());
        if (jsonNode.isArray()) {
            ArrayNode array = (ArrayNode) jsonNode;
            for (int i = 0; i < array.size(); i++) {
                JsonNode child = array.get(i);
                String nodeKey = getNodeKey(prefix, i);

                if (child.isValueNode()) {
                    nodeAndJsonNodeMapping.put(nodeKey, child);
                } else {
                    if (rowAndFieldMapping.containsValue(nodeKey)) {
                        nodeAndJsonNodeMapping.put(nodeKey, child);
                    }
                    parseTree(child, nodeKey);
                }
            }
            return;
        }
        Iterator<String> iterator = jsonNode.fieldNames();
        while (iterator.hasNext()) {
            String next = iterator.next();
            JsonNode child = jsonNode.get(next);
            String nodeKey = getNodeKey(prefix, next);

            nodeAndJsonNodeMapping.put(nodeKey, child);
            parseTree(child, nodeKey);
        }
    }

    private JsonNode getIgnoreCase(String key) {
        String nodeMappingKey = rowAndFieldMapping.getOrDefault(key, key);
        return nodeAndJsonNodeMappingForThread.get(Thread.currentThread().getId()).get(nodeMappingKey);
    }

    private String getNodeKey(String prefix, String nodeName) {
        if (Strings.isNullOrEmpty(prefix)) {
            return nodeName;
        }
        return prefix + "." + nodeName;
    }

    private String getNodeKey(String prefix, int i) {
        if (Strings.isNullOrEmpty(prefix)) {
            return "[" + i + "]";
        }
        return prefix + "[" + i + "]";
    }

    private Object convert(JsonNode node, TypeInformation<?> info) {
        if (info.getTypeClass().equals(Types.BOOLEAN.getTypeClass())) {
            return node.asBoolean();
        } else if (info.getTypeClass().equals(Types.STRING.getTypeClass())) {
            if (node instanceof ObjectNode) {
                return node.toString();
            } else if (node instanceof NullNode) {
                return null;
            } else {
                return node.asText();
            }
        } else if (info.getTypeClass().equals(Types.SQL_DATE.getTypeClass())) {
            return Date.valueOf(node.asText());
        } else if (info.getTypeClass().equals(Types.SQL_TIME.getTypeClass())) {
            // local zone
            return convertToTime(node.asText());
        } else if (info.getTypeClass().equals(Types.SQL_TIMESTAMP.getTypeClass())) {
            // local zone
            return convertToTimestamp(node.asText());
        } else if (info instanceof RowTypeInfo) {
            return convertRow(node, (RowTypeInfo) info);
        } else if (info instanceof ObjectArrayTypeInfo) {
            return convertObjectArray(node, ((ObjectArrayTypeInfo) info).getComponentInfo());
        } else {
            // for types that were specified without JSON schema
            // e.g. POJOs
            try {
                return objectMapper.treeToValue(node, info.getTypeClass());
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Unsupported type information '" + info + "' for node: " + node);
            }
        }
    }

    /**
     * 将 2021-09-07 14:49:10.0 和 1598446699685 两种格式都转化为 Timestamp
     */
    private Timestamp convertToTimestamp(String timestamp) {
        if (TIMESTAMP_PATTERN.matcher(timestamp).find()) {
            return new Timestamp(Long.parseLong(timestamp));
        }
        if (TIME_FORMAT_PATTERN.matcher(timestamp).find()) {
            return Timestamp.valueOf(timestamp);
        }
        throw new IllegalArgumentException("Incorrect time format of timestamp");
    }

    private Time convertToTime(String timestamp) {
        if (TIMESTAMP_PATTERN.matcher(timestamp).find()) {
            return new Time(Long.parseLong(timestamp));
        }
        if (TIME_FORMAT_PATTERN.matcher(timestamp).find()) {
            return Time.valueOf(timestamp);
        }
        throw new IllegalArgumentException("Incorrect time format of time");
    }

    private Row convertTopRow() {
        Row row = new Row(fieldNames.length);
        try {
            for (int i = 0; i < fieldNames.length; i++) {
                JsonNode node = getIgnoreCase(fieldNames[i]);
                AbstractTableInfo.FieldExtraInfo fieldExtraInfo = fieldExtraInfos.get(i);

                if (node == null) {
                    if (fieldExtraInfo != null && fieldExtraInfo.getNotNull()) {
                        throw new IllegalStateException("Failed to find field with name '"
                                + fieldNames[i] + "'.");
                    } else {
                        row.setField(i, null);
                    }
                } else {
                    // Read the value as specified type
                    Object value = convert(node, fieldTypes[i]);
                    row.setField(i, value);
                }
            }
            return row;
        } finally {

            nodeAndJsonNodeMappingForThread.get(Thread.currentThread().getId()).clear();
        }
    }

    private Row convertRow(JsonNode node, RowTypeInfo info) {
        final String[] names = info.getFieldNames();
        final TypeInformation<?>[] types = info.getFieldTypes();

        final Row row = new Row(names.length);
        for (int i = 0; i < names.length; i++) {
            final String name = names[i];
            final JsonNode subNode = node.get(name);
            if (subNode == null) {
                row.setField(i, null);
            } else {
                row.setField(i, convert(subNode, types[i]));
            }
        }

        return row;
    }

    private Object convertObjectArray(JsonNode node, TypeInformation<?> elementType) {
        final Object[] array = (Object[]) Array.newInstance(elementType.getTypeClass(), node.size());
        for (int i = 0; i < node.size(); i++) {
            array[i] = convert(node.get(i), elementType);
        }
        return array;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return typeInfo;
    }
}
