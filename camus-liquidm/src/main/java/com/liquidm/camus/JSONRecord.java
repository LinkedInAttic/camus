package com.liquidm.camus;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class JSONRecord implements GenericRecord {
  public static final org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"JSONEvent\",\"namespace\":\"com.linkedin.camus.etl.kafka.common\",\"doc\":\"json event\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\",\"default\":0}]}");
  private final JsonNode json;


  public JSONRecord(JsonNode jsonNode) {
    json = jsonNode;
  }

  @Override
  public void put(String key, Object v) {
    throw new org.apache.avro.AvroRuntimeException("Unsupported");
  }

  @SuppressWarnings(value="unchecked")
  @Override
  public void put(int field$, java.lang.Object value$) {
    throw new org.apache.avro.AvroRuntimeException("Bad index");
  }

  @Override
  public Object get(String field) {
    return json.get(field);
  }

  @Override
  public Object get(int field) {
    return json.get(field);
  }

  @Override
  public Schema getSchema() {
    return SCHEMA;
  }

  @Override
  public String toString() {
    return json.toString();
  }
}
