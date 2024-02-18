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

package org.apache.ratu.second;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Utils {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSecondaryIndex.class);

    public static String pattern(String str) {
        return str.replaceAll("(\\w+)\\s*:", "\"$1\":");
    }


    public static Map<String, Object> toMaps(Row newRow, Map<String, Map<String, String>> options) throws CharacterCodingException, UnknownHostException {
        List<Object> list = new ArrayList<>();
        Set<Object> set = new HashSet<>();
        Map<String, Object> mp = new HashMap<>();
        Map<String, Object> maps = new HashMap<>();
        for (Cell row : newRow.cells()) {
            String filedKeyName = row.column().name.toString();
            if (options.get(filedKeyName) != null) {
                String filedType = row.column().type.asCQL3Type().toString();
                switch (filedType) {
                    case "float":
                        ByteBuffer buffer1 = row.buffer();
                        if (buffer1.capacity() != 0) {
                            float filedValue1 = ByteBufferUtil.toFloat(buffer1);
                            maps.put(filedKeyName, filedValue1);
                        } else {
                            maps.put(filedKeyName, 0);
                        }
                        break;
                    case "decimal":
                        ByteBuffer buffer9 = (ByteBuffer) row.value();
                        if (buffer9.capacity() != 0) {
                            String filedValue9 = DecimalType.instance.getString(buffer9);
                            maps.put(filedKeyName, Double.valueOf(filedValue9));
                        } else {
                            maps.put(filedKeyName, 0);
                        }
                        break;
                    case "double":
                        ByteBuffer buffer2 = row.buffer();
                        if (buffer2.capacity() != 0) {
                            double filedValue2 = ByteBufferUtil.toDouble(buffer2);
                            maps.put(filedKeyName, filedValue2);
                        } else {
                            maps.put(filedKeyName, 0);
                        }
                        break;
                    case "int":
                        ByteBuffer buffer3 = row.buffer();
                        if (buffer3.capacity() != 0) {
                            int filedValue3 = ByteBufferUtil.toInt(buffer3);
                            maps.put(filedKeyName, filedValue3);
                        } else {
                            maps.put(filedKeyName, 0);
                        }
                        break;
                    case "long":
                        ByteBuffer buffer4 = row.buffer();
                        if (buffer4.capacity() != 0) {
                            long filedValue4 = ByteBufferUtil.toLong(buffer4);
                            maps.put(filedKeyName, filedValue4);
                        } else {
                            maps.put(filedKeyName, 0);
                        }
                        break;
                    case "short":
                        ByteBuffer buffer5 = row.buffer();
                        if (buffer5.capacity() != 0) {
                            short filedValue5 = ByteBufferUtil.toShort(buffer5);
                            maps.put(filedKeyName, filedValue5);
                        } else {
                            maps.put(filedKeyName, 0);
                        }
                        break;
                    case "byte":
                        ByteBuffer buffer6 = row.buffer();
                        if (buffer6.capacity() != 0) {
                            byte filedValue6 = ByteBufferUtil.toByte(buffer6);
                            maps.put(filedKeyName, filedValue6);
                        } else {
                            maps.put(filedKeyName, ByteBufferUtil.bytes("null"));
                        }
                        break;
                    case "timestamp":
                        ByteBuffer buffer7 = row.buffer();
                        if (buffer7.capacity() != 0) {
                            Date date = new Date(ByteBufferUtil.toLong(buffer7));
                            //yyyy-MM-dd'T'HH:mm:ss'Z'
                            String pattern = options.get(filedKeyName).get("pattern");
                            if (!StringUtils.isBlank(pattern) && pattern.equals("yyyy-MM-dd HH:mm:ss")) {
                                pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
                            } else if (!StringUtils.isBlank(pattern) && pattern.equals("yyyy-MM-dd")) {
                                pattern = "yyyy-MM-dd";
                            } else if (!StringUtils.isBlank(pattern) && pattern.equals("yyyy/MM/dd")) {
                                pattern = "yyyy-MM-dd";
                            } else {
                                pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
                            }
                            SimpleDateFormat df = new SimpleDateFormat(pattern);
                            String format = df.format(date);
                            maps.put(filedKeyName, format);
                        } else {
                            // 如果时间为null或者为空，写入es后写入当前默认时间
                            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                            String format = df.format(new Date());
                            maps.put(filedKeyName, format);
                        }
                        break;
                    case "ascii":
                        ByteBuffer buffer10 = row.buffer();
                        if (buffer10.capacity() != 0) {
                            String fieldValue10 = ByteBufferUtil.string(buffer10);
                            maps.put(filedKeyName, fieldValue10);
                        } else {
                            maps.put(filedKeyName, "null");
                        }
                        break;
                    case "bigint":
                        ByteBuffer buffer11 = row.buffer();
                        if (buffer11.capacity() != 0) {
                            long fieldValue11 = ByteBufferUtil.toLong(buffer11);
                            maps.put(filedKeyName, fieldValue11);
                        } else {
                            maps.put(filedKeyName, 0);
                        }
                        break;
                    case "blob":
                        ByteBuffer buffer12 = row.buffer();
                        if (buffer12.capacity() != 0) {
                            String fieldValue12 = ByteBufferUtil.string(buffer12);
                            maps.put(filedKeyName, fieldValue12);
                        } else {
                            maps.put(filedKeyName, "null");
                        }
                        break;
                    case "boolean":
                        ByteBuffer buffer13 = row.buffer();
                        if (buffer13.capacity() != 0) {
                            String text = BooleanType.instance.toJSONString(buffer13, ProtocolVersion.V4);
                            Boolean fieldValue13 = Boolean.valueOf(text);
                            maps.put(filedKeyName, fieldValue13);
                        } else {
                            maps.put(filedKeyName, false);
                        }
                        break;
                    case "inet":
                        ByteBuffer buffer14 = row.buffer();
                        if (buffer14.capacity() != 0) {
                            String ip = InetAddressType.instance.getString(buffer14);
                            maps.put(filedKeyName, ip);
                        } else {
                            maps.put(filedKeyName, "0.0.0.0");
                        }
                        break;
                    case "text":
                        ByteBuffer buffer15 = row.buffer();
                        if (buffer15.capacity() != 0) {
                            String string = ByteBufferUtil.string(buffer15);
                            maps.put(filedKeyName, string);
                        } else {
                            maps.put(filedKeyName, new ArrayList<>());
                        }
                        break;
                    case "uuid":
                        ByteBuffer buffer16 = row.buffer();
                        if (buffer16.capacity() != 0) {
                            String string = UUIDType.instance.getString(buffer16);
                            maps.put(filedKeyName, string);
                        } else {
                            maps.put(filedKeyName, 0);
                        }
                        break;
                    case "varchar":
                        ByteBuffer buffer17 = row.buffer();
                        if (buffer17.capacity() != 0) {
                            String string = ByteBufferUtil.string(buffer17);
                            maps.put(filedKeyName, string);
                        } else {
                            maps.put(filedKeyName, "null");
                        }
                        break;
                    case "varint":
                        ByteBuffer buffer18 = row.buffer();
                        if (buffer18.capacity() != 0) {
                            BigInteger bigInteger = new BigInteger(ByteBufferUtil.getArray(buffer18));
                            maps.put(filedKeyName, bigInteger);
                        } else {
                            maps.put(filedKeyName, new BigInteger("0".getBytes(UTF_8)));
                        }
                        break;
                    case "duration":
                        ByteBuffer buffer19 = row.buffer();
                        if (buffer19.capacity() != 0) {
                            String string = DurationType.instance.getString(buffer19);
                            maps.put(filedKeyName, string);
                        } else {
                            maps.put(filedKeyName, "0");
                        }
                        break;
                    case "smallint":
                        ByteBuffer buffer20 = row.buffer();
                        if (buffer20.capacity() != 0) {
                            String value20 = IntegerType.instance.getString(buffer20);
                            maps.put(filedKeyName, value20);
                        } else {
                            maps.put(filedKeyName, 0);
                        }
                        break;
                    case "tinyint":
                        ByteBuffer buffer21 = row.buffer();
                        if (buffer21.capacity() != 0) {
                            byte value21 = ByteBufferUtil.toByte(buffer21);
                            int i = Byte.toUnsignedInt(value21);
                            maps.put(filedKeyName, i);
                        } else {
                            maps.put(filedKeyName, 0);
                        }
                        break;
                    case "time":
                        ByteBuffer buffer22 = row.buffer();
                        if (buffer22.capacity() != 0) {
                            long l = ByteBufferUtil.toLong(buffer22);
                            maps.put(filedKeyName, l);
                        }
                        break;
                    default:
                        ByteBuffer buffer8 = row.buffer();
                        if (filedType.contains("list")) {
                            String string = ByteBufferUtil.string(buffer8);
                            list.add(string);
                            maps.put(filedKeyName, list);
                        } else if (filedType.contains("set")) {
                            CellPath path = row.path();
                            for (int i = 0; i < path.size(); i++) {
                                String string = ByteBufferUtil.string(path.get(i));
                                set.add(string);
                            }
                            maps.put(filedKeyName, set);
                        } else if (filedType.contains("map")) {
                            CellPath path = row.path();
                            for (int i = 0; i < path.size(); i++) {
                                String key = ByteBufferUtil.string(path.get(i));
                                String val = ByteBufferUtil.string(row.buffer());
                                mp.put(key, val);
                            }
                            maps.put(filedKeyName, mp);
                        } else {

                            if (buffer8.capacity() == 0) {
                                maps.put(filedKeyName, "null");
                            } else {
                                String filedValue8 = ByteBufferUtil.string(buffer8);
                                maps.put(filedKeyName, filedValue8);
                            }
                        }
                        break;
                }
            }
        }
        return maps;
    }


    public static Object toPrimaryKey(Row newRow) throws CharacterCodingException {
        Map<String, Object> maps = new HashMap<>();
        for (Cell row : newRow.cells()) {
            String filedKeyName = row.column().name.toString();
            String filedType = row.column().type.asCQL3Type().toString();

            switch (filedType) {
                case "float":
                    float filedValue1 = ByteBufferUtil.toFloat(row.buffer());
                    maps.put(filedKeyName, filedValue1);
                    break;
                case "double":
                    double filedValue2 = ByteBufferUtil.toDouble(row.buffer());
                    maps.put(filedKeyName, filedValue2);
                    break;
                case "int":
                    int filedValue3 = ByteBufferUtil.toInt(row.buffer());
                    maps.put(filedKeyName, filedValue3);
                    break;
                case "long":
                    long filedValue4 = ByteBufferUtil.toLong(row.buffer());
                    maps.put(filedKeyName, filedValue4);
                    break;
                case "short":
                    short filedValue5 = ByteBufferUtil.toShort(row.buffer());
                    maps.put(filedKeyName, filedValue5);
                    break;
                case "byte":
                    byte filedValue6 = ByteBufferUtil.toByte(row.buffer());
                    maps.put(filedKeyName, filedValue6);
                    break;
                case "timestamp":
                    Date date = new Date(ByteBufferUtil.toLong(row.buffer()));
                    maps.put(filedKeyName, date);
                    break;
                default:
                    String filedValue8 = ByteBufferUtil.string(row.buffer());
                    maps.put(filedKeyName, filedValue8);
                    break;
            }
        }
        return maps;
    }


    public static String queryString(@Nonnull ReadCommand command) {
        RowFilter filter = command.rowFilter();
        List<RowFilter.Expression> clause = filter.getExpressions();
        RowFilter.Expression expression = clause.isEmpty() ? null : clause.get(0);
        if (expression == null) {
            throw new InvalidRequestException("Missing clause:" + filter);
        }

        try {
            return ByteBufferUtil.string(expression.getIndexValue(), UTF_8);
        } catch (CharacterCodingException e) {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    @Nonnull
    public static ByteBuffer getPartitionKeys(@Nonnull String[] keys, @Nonnull TableMetadata tableMetadata) {
        List<ColumnMetadata> columns = tableMetadata.partitionKeyColumns();
        ColumnMetadata[] pkColDefinitions = columns.toArray(new ColumnMetadata[columns.size()]);

        AbstractType<?> pkValidator = tableMetadata.partitionKeyType;

        // PK is composite we need to extract sub-keys
        if (pkValidator instanceof CompositeType) {
            CompositeType type = (CompositeType) pkValidator;

            Object[] objects = new Object[pkColDefinitions.length];
            int pos = 0;

            for (ColumnMetadata column : columns) {
                if (column.type.asCQL3Type().equals(CQL3Type.Native.INT)) {
                    objects[pos] = Integer.valueOf(keys[pos]);
                } else {
                    objects[pos] = keys[pos];
                }
                pos++;
            }
            return type.decompose(objects);

        } else { // PK is a single column

            return pkValidator.fromString(keys[0]);
        }
    }


    @Nonnull
    public static List<String> getPartitionKeyNames(@Nonnull TableMetadata metadata) throws CharacterCodingException {
        List<ColumnMetadata> partitionKeys = metadata.partitionKeyColumns();
        List<String> primaryKeys = new ArrayList<>(partitionKeys.size());

        for (ColumnMetadata colDef : partitionKeys) {
            String keyName = ByteBufferUtil.string(colDef.name.bytes);
            primaryKeys.add(keyName);
        }

        return primaryKeys;
    }

    /**
     * Get clustering keys
     *
     * @param metadata table metadata
     * @return Clustering keys, can be empty
     */
    @Nonnull
    public static List<String> getClusteringColumnsNames(@Nonnull TableMetadata metadata) throws CharacterCodingException {
        List<ColumnMetadata> clusteringColumns = metadata.clusteringColumns();
        List<String> clusteringColumnsNames = new ArrayList<>(clusteringColumns.size());

        for (ColumnMetadata colDef : clusteringColumns) {
            String keyName = ByteBufferUtil.string(colDef.name.bytes);
            clusteringColumnsNames.add(keyName);
        }

        return clusteringColumnsNames;
    }

    @Nullable
    public static String getString(@Nullable JsonElement element, @Nonnull String... path) {
        if (element == null) {
            return null;
        }
        for (int i = 0; i < path.length; i++) {
            String key = path[i];
            if (i + 1 == path.length) {
                JsonElement value = element.getAsJsonObject().get(key);
                return value == null ? null : value.getAsString();
            } else if (element.isJsonObject()) {
                element = element.getAsJsonObject().get(key);
                if (element == null || !element.isJsonObject()) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return null;
    }

    @Nullable
    public static JsonElement jsonObjectToJsonElement(JSONObject jsonObject) {
        Gson gson = new Gson();
        return gson.fromJson(JSON.toJSONString(jsonObject), JsonElement.class);
    }

    @Nonnull
    public static JsonObject filterPath(@Nonnull JsonObject jsonObject, @Nonnull String... path) {
        if (path.length == 0) {
            return new JsonObject();

        } else {
            JsonObject result = new JsonObject();

            jsonObject.entrySet().stream().forEach(e -> {
                String key = e.getKey();
                JsonElement value = e.getValue();

                if (path[0].equals(e.getKey())) {
                    if (path.length > 1) { // if length == 1, this is the key to remove
                        if (value instanceof JsonObject) { // if value is an object, filter further
                            value = Utils.filterPath((JsonObject) value, Arrays.copyOfRange(path, 1, path.length));
                        }
                        result.add(key, value);
                    }
                } else {
                    result.add(key, value);
                }
            });
            return result;
        }
    }


    @Nullable
    public static List<Object> JsonObjectToList(@Nonnull JsonObject jsonObject) {
        List<Object> result = new ArrayList<>();
        jsonObject.entrySet().forEach(json -> {
            String key = json.getKey();
            JsonElement value = json.getValue();

            if (value instanceof JsonObject) {
            }
            result.add(value);
        });

        return result;
    }


    @Nonnull
    public static Map<String, Object> getPartitionKeys(@Nonnull ByteBuffer rowKey, @Nonnull TableMetadata tableMetadata) {
        Map<String, Object> maps = new HashMap<>();

        List<ColumnMetadata> columns = tableMetadata.partitionKeyColumns();
        ColumnMetadata[] pkColDefinitions = columns.toArray(new ColumnMetadata[columns.size()]);
        try {
            String string = ByteBufferUtil.string(rowKey);
        } catch (CharacterCodingException e) {
            e.printStackTrace();
        }
        return maps;
    }

}
