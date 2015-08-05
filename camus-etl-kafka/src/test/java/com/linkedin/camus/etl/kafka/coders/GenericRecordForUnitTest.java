package com.linkedin.camus.etl.kafka.coders;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class GenericRecordForUnitTest implements GenericRecord {

  @Override
  public void put(int i, Object v) {
  }

  @Override
  public Object get(int i) {
    return null;
  }

  @Override
  public Schema getSchema() {
    return null;
  }

  @Override
  public void put(String key, Object v) {

  }

  @Override
  public Object get(String key) {
    return null;
  }

  @Override
  public String toString() {
    return "genericRecordForUnitTest";
  }

}