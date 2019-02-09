package org.apache.hadoop.hive.serde2;

import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestJsonSerDe {

  @Test
  public void test() throws Exception {
    JsonSerDe serde = new JsonSerDe();
    Properties props = new Properties();
    props.setProperty(serdeConstants.LIST_COLUMNS, "firstName");
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string");
    props.setProperty(serdeConstants.TIMESTAMP_FORMATS, "millis");
    serde.initialize(null, props);

    Text text = new Text("{\"firstName\":\"Samantha\"}");
    Object[] O = (Object[]) serde.deserialize(text);
    System.out.println(O[0]);
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
