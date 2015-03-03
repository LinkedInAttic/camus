package com.linkedin.camus.etl.kafka.coders;

import org.apache.avro.generic.IndexedRecord;

import com.linkedin.camus.coders.MessageEncoder;

public class EncoderForUnitTest extends MessageEncoder<IndexedRecord, byte[]> {

  @Override
  public byte[] toBytes(IndexedRecord record) {
    return record.toString().getBytes();
  }
}