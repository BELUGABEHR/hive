/**
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

package org.apache.hadoop.hive.serde2.json;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hive.common.util.TimestampParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Preconditions;

public class HiveJsonStructReader {

  private static final Logger LOG =
      LoggerFactory.getLogger(HiveJsonStructReader.class);

  private static final String DEFAULT_BINARY_DECODING = "default";

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final Map<String, StructField> discoveredFields = new HashMap<>();
  private final Set<String> discoveredUnknownFields = new HashSet<>();

  private final ObjectInspector oi;
  private TimestampParser tsParser;

  private boolean ignoreUnknownFields;
  private boolean hiveColIndexParsing;
  private boolean writeablePrimitives;
  private String binaryEncodingType;

  public HiveJsonStructReader(TypeInfo t) {
    this(t, new TimestampParser());
  }

  public HiveJsonStructReader(TypeInfo t, TimestampParser tsParser) {
    this.ignoreUnknownFields = false;
    this.writeablePrimitives = false;
    this.hiveColIndexParsing = false;
    this.binaryEncodingType = DEFAULT_BINARY_DECODING;
    this.tsParser = tsParser;
    this.oi = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(t);
  }

  public Object parseStruct(String text)
      throws JsonParseException, IOException, SerDeException {
    final JsonNode rootNode = this.objectMapper.readTree(text);
    return visitNode(rootNode, this.oi);
  }

  public Object parseStruct(InputStream is)
      throws JsonParseException, IOException, SerDeException {
    final JsonNode rootNode = this.objectMapper.readTree(is);
    return visitNode(rootNode, this.oi);
  }

  private Object visitNode(final JsonNode rootNode, ObjectInspector oi)
      throws SerDeException {

    switch (oi.getCategory()) {
    case PRIMITIVE:
      return visitPrimativeNode(rootNode, oi);
    case LIST:
      return visitArrayNode(rootNode, oi);
    case STRUCT:
      return visitStructNode(rootNode, oi);
    case MAP:
      return visitMapNode(rootNode, oi);
    default:
      throw new SerDeException(
          "Parsing of: " + oi.getCategory() + " is not supported");
    }

  }

  private Object visitMapNode(final JsonNode rootNode, final ObjectInspector oi)
      throws SerDeException {
    Preconditions.checkArgument(JsonNodeType.OBJECT == rootNode.getNodeType());

    final Map<Object, Object> ret = new LinkedHashMap<>();

    final ObjectInspector mapKeyInspector =
        ((MapObjectInspector) oi).getMapKeyObjectInspector();

    final ObjectInspector mapValueInspector =
        ((MapObjectInspector) oi).getMapValueObjectInspector();

    if (!(mapKeyInspector instanceof PrimitiveObjectInspector)) {
      throw new SerDeException("Map key must be a primitive");
    }

    final Iterator<Entry<String, JsonNode>> it = rootNode.fields();
    while (it.hasNext()) {
      final Entry<String, JsonNode> field = it.next();
      final Object key =
          visitNode(new TextNode(field.getKey()), mapKeyInspector);
      final Object val = visitNode(field.getValue(), mapValueInspector);
      ret.put(key, val);
    }

    return ret;
  }

  private Object visitStructNode(final JsonNode rootNode,
      final ObjectInspector oi) throws SerDeException {

    Preconditions.checkArgument(JsonNodeType.OBJECT == rootNode.getNodeType());

    final Object[] ret =
        new Object[((StructObjectInspector) oi).getAllStructFieldRefs().size()];

    final Iterator<Entry<String, JsonNode>> it = rootNode.fields();
    while (it.hasNext()) {
      final Entry<String, JsonNode> field = it.next();
      final String fieldName = field.getKey();
      final JsonNode childNode = field.getValue();
      final StructField structField =
          getStructField((StructObjectInspector) oi, fieldName);
      if (structField != null) {
        final Object childValue =
            visitNode(childNode, structField.getFieldObjectInspector());
        ret[structField.getFieldID()] = childValue;
      }
    }

    return ret;
  }

  private Object visitArrayNode(final JsonNode rootNode, ObjectInspector oi)
      throws SerDeException {
    Preconditions.checkArgument(JsonNodeType.ARRAY == rootNode.getNodeType());

    final ObjectInspector eOI =
        ((ListObjectInspector) oi).getListElementObjectInspector();

    final List<Object> ret = new ArrayList<>();
    final Iterator<JsonNode> it = rootNode.elements();

    while (it.hasNext()) {
      final JsonNode element = it.next();
      ret.add(visitNode(element, eOI));
    }

    return ret;
  }

  private Object visitPrimativeNode(final JsonNode rootNode,
      final ObjectInspector oi) throws SerDeException {
    final PrimitiveTypeInfo typeInfo =
        ((PrimitiveObjectInspector) oi).getTypeInfo();
    final String value = rootNode.asText();
    if (writeablePrimitives) {
      Converter c = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector, oi);
      return c.convert(value);
    }

    switch (typeInfo.getPrimitiveCategory()) {
    case INT:
      return Integer.valueOf(value);
    case BYTE:
      return Byte.valueOf(value);
    case SHORT:
      return Short.valueOf(value);
    case LONG:
      return Long.valueOf(value);
    case BOOLEAN:
      return Boolean.valueOf(value);
    case FLOAT:
      return Float.valueOf(value);
    case DOUBLE:
      return Double.valueOf(value);
    case STRING:
      return value;
    case BINARY:
      return getByteValue(value);
    case DATE:
      return Date.valueOf(value);
    case TIMESTAMP:
      return tsParser.parseTimestamp(value);
    case DECIMAL:
      return HiveDecimal.create(value);
    case VARCHAR:
      return new HiveVarchar(value, ((BaseCharTypeInfo) typeInfo).getLength());
    case CHAR:
      return new HiveChar(value, ((BaseCharTypeInfo) typeInfo).getLength());
    default:
      throw new SerDeException("Could not convert from string to map type "
          + typeInfo.getTypeName());
    }
  }

  private byte[] getByteValue(final String byteText) throws SerDeException {
    switch (this.binaryEncodingType) {
    case "default":
      try {
        return Text.decode(byteText.getBytes(), 0, byteText.getBytes().length)
            .getBytes(StandardCharsets.UTF_8);
      } catch (CharacterCodingException e) {
        throw new SerDeException(
            "Error generating json binary type from object.", e);
      }
    case "base64":
      return Base64.getDecoder().decode(byteText);

    default:
      throw new SerDeException(
          "Decoded type not available: " + this.binaryEncodingType);
    }
  }

  private StructField getStructField(final StructObjectInspector oi,
      final String fieldName) throws SerDeException {

    // Ignore the field if it has been ignored before
    if (this.discoveredUnknownFields.contains(fieldName)) {
      return null;
    }

    // Return from cache if the field has already been discovered
    StructField structField = this.discoveredFields.get(fieldName);
    if (structField != null) {
      return structField;
    }

    // Otherwise attempt to discover the field
    if (hiveColIndexParsing) {
      int colIndex = getColIndex(fieldName);
      if (colIndex >= 0) {
        structField = oi.getAllStructFieldRefs().get(colIndex);
      }
    }
    if (structField == null) {
      try {
        structField = oi.getStructFieldRef(fieldName);
      } catch (Exception e) {
        // No such field
      }
    }
    if (structField != null) {
      // cache it for next time
      this.discoveredFields.put(fieldName, structField);
    } else {
      // Tried everything and did not discover this field
      if (this.ignoreUnknownFields) {
        if (this.discoveredUnknownFields.add(fieldName)) {
          LOG.warn("Discovered unknown field: {}. Ignoring.", fieldName);
        }
      } else {
        throw new SerDeException("No such field exists: " + fieldName);
      }
    }

    return structField;
  }

  Pattern internalPattern = Pattern.compile("^_col([0-9]+)$");

  private int getColIndex(String internalName) {
    // The above line should have been all the implementation that
    // we need, but due to a bug in that impl which recognizes
    // only single-digit columns, we need another impl here.
    Matcher m = internalPattern.matcher(internalName);
    if (!m.matches()) {
      return -1;
    } else {
      return Integer.parseInt(m.group(1));
    }
  }

  public void setIgnoreUnknownFields(boolean ignore) {
    this.ignoreUnknownFields = ignore;
  }

  public void enableHiveColIndexParsing(boolean indexing) {
    hiveColIndexParsing = indexing;
  }

  public void setWritablesUsage(boolean writables) {
    writeablePrimitives = writables;
  }

  public ObjectInspector getObjectInspector() {
    return oi;
  }

  public String getBinaryEncodingType() {
    return binaryEncodingType;
  }

  public void setBinaryEncodingType(String encodingType) {
    this.binaryEncodingType = encodingType;
  }
}
