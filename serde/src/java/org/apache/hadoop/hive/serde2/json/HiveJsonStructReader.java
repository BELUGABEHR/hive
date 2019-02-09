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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Preconditions;

/**
 * JavaScript Object Notation (JSON) objects group items of possibly different
 * types into a single type, much like a classic 'struct'. Each JSON object is
 * surrounded by curly braces {} and is presented as key/value pairs. An item
 * with the JSON object may itself be a JSON object and therefore allows for
 * nested fields. This class parses JSON objects and returns each item in the
 * struct in an array of Objects.
 */
public class HiveJsonStructReader {

  private static final Logger LOG =
      LoggerFactory.getLogger(HiveJsonStructReader.class);

  private static final String DEFAULT_BINARY_DECODING = "default";

  private final ObjectMapper objectMapper = new ObjectMapper();

  private final Map<String, StructField> discoveredFields = new HashMap<>();
  private final Set<String> discoveredUnknownFields = new HashSet<>();

  private final StructObjectInspector soi;
  private TimestampParser tsParser;

  private boolean ignoreUnknownFields;
  private boolean hiveColIndexParsing;
  private boolean writeablePrimitives;
  private String binaryEncodingType;

  /**
   * Constructor with default the Hive default timestamp parser.
   *
   * @param typeInfo Type info for all the fields in the JSON object
   */
  public HiveJsonStructReader(TypeInfo typeInfo) {
    this(typeInfo, new TimestampParser());
  }

  /**
   * Constructor with default the Hive default timestamp parser.
   *
   * @param typeInfo Type info for all the fields in the JSON object
   * @param tsParser Custom timestamp parser
   */
  public HiveJsonStructReader(TypeInfo typeInfo, TimestampParser tsParser) {
    this.ignoreUnknownFields = false;
    this.writeablePrimitives = false;
    this.hiveColIndexParsing = false;
    this.binaryEncodingType = DEFAULT_BINARY_DECODING;
    this.tsParser = tsParser;
    this.soi = (StructObjectInspector) TypeInfoUtils
        .getStandardWritableObjectInspectorFromTypeInfo(typeInfo);
  }

  /**
   * Parse text containing a complete JSON object.
   *
   * @param text The text to parse
   * @return An array of Objects, one for each field in the JSON object
   * @throws IOException Unable to parse the JSON text
   * @throws SerDeException The SerDe is not configured correctly
   */
  public Object parseStruct(String text) throws IOException, SerDeException {
    final JsonNode rootNode = this.objectMapper.readTree(text);
    return visitNode(rootNode, this.soi);
  }

  /**
   * Parse text containing a complete JSON object.
   *
   * @param is The InputStream to read the text from
   * @return An array of Objects, one for each field in the JSON object
   * @throws IOException Unable to parse the JSON text
   * @throws SerDeException The SerDe is not configured correctly
   */
  public Object parseStruct(InputStream is) throws IOException, SerDeException {
    final JsonNode rootNode = this.objectMapper.readTree(is);
    return visitNode(rootNode, this.soi);
  }

  /**
   * Visit a node and parse it based on the provided ObjectInspector
   *
   * @param rootNode The root node to process
   * @param oi The ObjectInspector to use
   * @return The value in this node (may be a complex type if nested)
   * @throws SerDeException The SerDe is not configured correctly
   */
  private Object visitNode(final JsonNode rootNode, final ObjectInspector oi)
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

  /**
   * Visit a node if it is expected to be a Map (a.k.a. JSON Object)
   *
   * @param rootNode The node pointing at the JSON object
   * @param oi The ObjectInspector to parse the Map (must be a
   *          MapObjectInspector)
   * @return A Java Map containing the contents of the JSON map
   * @throws SerDeException The SerDe is not configured correctly
   */
  private Object visitMapNode(final JsonNode rootNode, final ObjectInspector oi)
      throws SerDeException {
    Preconditions.checkArgument(JsonNodeType.OBJECT == rootNode.getNodeType());

    final Map<Object, Object> ret = new LinkedHashMap<>();

    final ObjectInspector mapKeyInspector =
        ((MapObjectInspector) oi).getMapKeyObjectInspector();

    final ObjectInspector mapValueInspector =
        ((MapObjectInspector) oi).getMapValueObjectInspector();

    if (!(mapKeyInspector instanceof PrimitiveObjectInspector)) {
      throw new SerDeException("Map key must be a primitive type");
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

  /**
   * Visit a node if it is expected to be a Struct data type (a.k.a. JSON
   * Object)
   *
   * @param rootNode The node pointing at the JSON object
   * @param oi The ObjectInspector to parse the Map (must be a
   *          StructObjectInspector)
   * @return A primitive array of Objects, each element is an element of the
   *         struct
   * @throws SerDeException The SerDe is not configured correctly
   */
  private Object visitStructNode(final JsonNode rootNode,
      final ObjectInspector oi) throws SerDeException {

    Preconditions.checkArgument(JsonNodeType.OBJECT == rootNode.getNodeType());

    final StructObjectInspector structInspector = (StructObjectInspector) oi;

    final Object[] ret =
        new Object[structInspector.getAllStructFieldRefs().size()];

    final Iterator<Entry<String, JsonNode>> it = rootNode.fields();
    while (it.hasNext()) {
      final Entry<String, JsonNode> field = it.next();
      final String fieldName = field.getKey();
      final JsonNode childNode = field.getValue();
      final StructField structField =
          getStructField(structInspector, fieldName);

      // If the struct field is null it is because there is a field defined in
      // the JSON object that was not defined in the table definition. Skip it.
      if (structField != null) {
        final Object childValue =
            visitNode(childNode, structField.getFieldObjectInspector());
        ret[structField.getFieldID()] = childValue;
      }
    }

    return ret;
  }

  /**
   * Visit a node if it is expected to be a JSON Array data type (a.k.a. Hive
   * Array type)
   *
   * @param rootNode The node pointing at the JSON object
   * @param oi The ObjectInspector to parse the List (must be a
   *          ListObjectInspector)
   * @return A Java List of Objects, each element is an element of the array
   * @throws SerDeException The SerDe is not configured correctly
   */
  private Object visitArrayNode(final JsonNode rootNode,
      final ObjectInspector oi) throws SerDeException {
    Preconditions.checkArgument(JsonNodeType.ARRAY == rootNode.getNodeType());

    final ObjectInspector loi =
        ((ListObjectInspector) oi).getListElementObjectInspector();

    final List<Object> ret = new ArrayList<>();
    final Iterator<JsonNode> it = rootNode.elements();

    while (it.hasNext()) {
      final JsonNode element = it.next();
      ret.add(visitNode(element, loi));
    }

    return ret;
  }

  /**
   * Visit a node if it is expected to be a primitive value (JSON leaf node).
   *
   * @param leafNode The node pointing at the JSON object
   * @param oi The ObjectInspector to parse the value (must be a
   *          PrimitiveObjectInspector)
   * @return A Java primitive Object
   * @throws SerDeException The SerDe is not configured correctly
   */
  private Object visitPrimativeNode(final JsonNode leafNode,
      final ObjectInspector oi) throws SerDeException {

    final String value = leafNode.asText();
    final PrimitiveTypeInfo typeInfo =
        ((PrimitiveObjectInspector) oi).getTypeInfo();

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

  /**
   * A user may configure the encoding for binary data represented as text
   * within a JSON object. This method applies that encoding to the text.
   *
   * @param byteText The text containing the binary data
   * @return A byte array with the binary data
   * @throws SerDeException The SerDe is not configured correctly
   */
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

  /**
   * Matches the JSON object's field name with the Hive data type.
   *
   * @param oi The ObjectInsepctor to lookup the matching in
   * @param fieldName The name of the field parsed from the JSON text
   * @return The meta data of regarding this field
   * @throws SerDeException The SerDe is not configured correctly
   */
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

  private final Pattern internalPattern = Pattern.compile("^_col([0-9]+)$");

  /**
   * Look up a column based on its index
   *
   * @param internalName The name of the column
   * @return The index of the field or -1 if the field name does not contain its
   *         index number too
   */
  private int getColIndex(final String internalName) {
    // The above line should have been all the implementation that
    // we need, but due to a bug in that impl which recognizes
    // only single-digit columns, we need another impl here.
    final Matcher m = internalPattern.matcher(internalName);
    return m.matches() ? Integer.valueOf(m.group(1)) : -1;
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

  public StructObjectInspector getObjectInspector() {
    return soi;
  }

  public String getBinaryEncodingType() {
    return binaryEncodingType;
  }

  public void setBinaryEncodingType(String encodingType) {
    this.binaryEncodingType = encodingType;
  }

  @Override
  public String toString() {
    return "HiveJsonStructReader [ignoreUnknownFields=" + ignoreUnknownFields
        + ", hiveColIndexParsing=" + hiveColIndexParsing
        + ", writeablePrimitives=" + writeablePrimitives
        + ", binaryEncodingType=" + binaryEncodingType + "]";
  }
}
