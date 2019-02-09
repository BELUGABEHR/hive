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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.serde2;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TestJsonSerDe {

  @Test
  public void testPrimativeDataTypes() throws Exception {
    JsonSerDe serde = new JsonSerDe();
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "name,height,weight,endangered,born");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,float,int,boolean,timestamp");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");
    serde.initialize(null, props);

    final Map<String, String> row = new HashMap<>();
    row.put("name", "giraffe");
    row.put("height", "5.5");
    row.put("weight", "1360");
    row.put("endangered", "true");
    row.put("born", "1549751270013");

    final String rowText = new ObjectMapper().writeValueAsString(row);

    final Text text = new Text(rowText);
    final Object[] results = (Object[]) serde.deserialize(text);

    final Object[] expected =
        new Object[] { new Text("giraffe"), new FloatWritable(5.5f),
            new IntWritable(1360), new BooleanWritable(true),
            TimestampWritable.longToTimestamp(1549751270013L, false) };

    Assert.assertArrayEquals(expected, results);
  }


  @Test
  public void testArray() throws Exception {
    JsonSerDe serde = new JsonSerDe();
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "names");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "array<string>");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");
    serde.initialize(null, props);

    Text text = new Text("{\"names\":[\"Samantha\", \"Sarah\"]}");
    Object[] O = (Object[]) serde.deserialize(text);
    System.out.println(O[0]);
  }
}
