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

package org.apache.hive.common.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Timestamp parser using Joda DateTimeFormatter. Parser accepts 0 or more date
 * time format patterns. If no format patterns are provided it will default to
 * the normal Timestamp parsing. Datetime formats are compatible with Java
 * SimpleDateFormat. Also added special case pattern "millis" to parse the
 * string as milliseconds since Unix epoch. Since this uses Joda
 * DateTimeFormatter, this parser should be thread safe.
 */
public class TimestampParser {

  private final Logger LOG = LoggerFactory.getLogger(TimestampParser.class);

  public final static String MILLIS_FORMAT_STR = "millis";
  public final static String ISO_8601_FORMAT_STR = "iso8601";
  public final static String RFC_1123_FORMAT_STR = "rfc1123";

  private final Collection<DateTimeFormatter> dtFormatters;
  private final boolean supportMillisEpoch;

  public TimestampParser() {
    this(Collections.emptyList());
  }

  public TimestampParser(final String[] formatStrings) {
    this(Arrays.asList(formatStrings));
  }

  /**
   * Create a timestamp parser with one ore more date patterns. When parsing,
   * the first pattern in the list is selected for parsing. If it fails, the
   * next is chosen, and so on. If none of these patterns succeeds, then the
   * output of the pattern that made the greatest progress is returned.
   *
   * @see DateTimeFormat
   * @param patterns a collection of timestamp formats
   */
  public TimestampParser(final Collection<String> patterns) {
    final Collection<String> patternSet = new HashSet<>(patterns);
    this.supportMillisEpoch = patternSet.remove(MILLIS_FORMAT_STR);

    if (patternSet.isEmpty()) {
      this.dtFormatters = Collections.emptyList();
      return;
    }

    this.dtFormatters = new ArrayList<>();

    for (final String patternText : patternSet) {
      final DateTimeFormatter formatter;
      switch (patternText) {
      case ISO_8601_FORMAT_STR:
        formatter = DateTimeFormatter.ISO_INSTANT;
        break;
      case RFC_1123_FORMAT_STR:
        formatter = DateTimeFormatter.RFC_1123_DATE_TIME;
        break;
      default:
        formatter = DateTimeFormatter.ofPattern(patternText);
        break;
      }

      this.dtFormatters.add(formatter);
    }
  }

  /**
   * Parse the input string and return a timestamp value
   *
   * @param text
   * @return
   * @throws IllegalArgumentException if input string cannot be parsed into
   *           timestamp
   */
  public Timestamp parseTimestamp(final String text)
      throws IllegalArgumentException {
    if (supportMillisEpoch) {
      try {
        final long millis = new BigDecimal(text).setScale(0, RoundingMode.DOWN)
            .longValueExact();
        return Timestamp.ofEpochMilli(millis);
      } catch (NumberFormatException e) {
        LOG.debug("Could not format millis: {}", text);
      }
    }
    for (DateTimeFormatter formatter : this.dtFormatters) {
      try {
        final TemporalAccessor parsed = formatter.parse(text);
        final Instant inst = Instant.from(wrap(parsed));
        return Timestamp.ofEpochMilli(inst.toEpochMilli());
      } catch (DateTimeParseException dtpe) {
        LOG.debug("Could not parse timestamp text: {}", text);
      }
    }
    return Timestamp.valueOf(text);
  }

  private TemporalAccessor wrap(final TemporalAccessor in) {
    if (in.isSupported(ChronoField.INSTANT_SECONDS)
        && in.isSupported(ChronoField.NANO_OF_SECOND)) {
      return in;
    }
    return new DefaultingTemporalAccessor(in);
  }

  public static class DefaultingTemporalAccessor implements TemporalAccessor {
    private static final EnumSet<ChronoField> FIELDS =
        EnumSet.of(ChronoField.YEAR, ChronoField.MONTH_OF_YEAR,
            ChronoField.DAY_OF_MONTH, ChronoField.HOUR_OF_DAY,
            ChronoField.MINUTE_OF_HOUR, ChronoField.SECOND_OF_MINUTE,
            ChronoField.MILLI_OF_SECOND, ChronoField.NANO_OF_SECOND);

    private final TemporalAccessor wrapped;

    public DefaultingTemporalAccessor(TemporalAccessor in) {
      ZonedDateTime dateTime =
          ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
      for (ChronoField field : FIELDS) {
        if (in.isSupported(field)) {
          dateTime = dateTime.with(field, in.getLong(field));
        }
      }
      this.wrapped = dateTime.toInstant();
    }

    @Override
    public long getLong(TemporalField field) {
      return wrapped.getLong(field);
    }

    @Override
    public boolean isSupported(TemporalField field) {
      return wrapped.isSupported(field);
    }
  }
}
