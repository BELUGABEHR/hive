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
package org.apache.hadoop.hive.serde2.text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * A base class Hive Serializer-Deserializer (SerDe) that supports regular
 * expression (regex) for parsing text files. The regular expression must
 * specify one or more capturing groups. If a line of text is processed that
 * generates a number of capture groups that is different than the number of
 * columns defined in the table, a {@link SerDeException} is thrown.<br/>
 * <br/>
 * This SerDe does not support serializing table data into rows of text and will
 * throw a {@link SerDeException} if it is used in that way.
 *
 * @see Pattern
 * @see Matcher
 */
public abstract class AbstractRegexTextSerDe
    extends AbstractEncodingAwareSerDe {

  private Pattern pattern;

  @Override
  public void initialize(final Configuration configuration,
      final Properties tableProperties) throws SerDeException {
    Objects.requireNonNull(this.pattern);
    super.initialize(configuration, tableProperties);
  }

  @Override
  protected String doSerialize(final Object obj,
      final ObjectInspector objInspector) throws SerDeException {
    throw new UnsupportedOperationException(
        getClass() + " does not support serialization");
  }

  /**
   * Based on the provided regex, deserialize a UTF-8 encoded string into a list
   * of strings.
   *
   * @param clob The unicode string to deserialize
   * @return A list of strings pulled from the value provided
   * @throws SerDeException If the number of capture groups generated by
   *           applying the regex to the clob value provided does not equal the
   *           number of columns defined in the Hive table
   */
  @Override
  protected List<String> doDeserialize(final String clob)
      throws SerDeException {

    LOG.trace("Splitting with regex [{}]:[{}]", this.pattern, clob);

    final int columnCount = getColumnNames().size();

    final List<String> fields = new ArrayList<>(columnCount);

    if (!clob.isEmpty()) {
      final Matcher matcher = this.pattern.matcher(clob);
      if (matcher.matches()) {
        for (int i = 1; i <= matcher.groupCount(); i++) {
          final String group = matcher.group(i);
          LOG.trace("Found a match: {}]", group);
          fields.add(group);
        }
      }
    } else {
      LOG.debug("Processed a blank line");
      fields.addAll(Collections.nCopies(columnCount, null));
    }

    final int fieldCountDelta = getColumnNames().size() - fields.size();

    LOG.debug("Expected number of fields: {} [delta:{}]", columnCount,
        fieldCountDelta);

    if (fieldCountDelta > 0) {
      throw new SerDeException(
          "Number of fields parsed from text data is less than number of "
              + "columns defined in the table schema");
    }

    if (fieldCountDelta < 0) {
      throw new SerDeException(
          "Number of fields parsed from text data is more than number of "
              + "columns defined in the table schema. Data would be lost.");
    }

    return Collections.unmodifiableList(fields);
  }

  public Pattern getPattern() {
    return pattern;
  }

  public void setPattern(Pattern pattern) {
    this.pattern = pattern;
  }
}
