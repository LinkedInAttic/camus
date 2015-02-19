package com.linkedin.camus.etl.kafka.coders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.linkedin.camus.coders.MessageEncoder;
import com.linkedin.camus.coders.MessageEncoderException;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistryException;


public class KafkaAvroMessageEncoder extends MessageEncoder<IndexedRecord, byte[]> {
  public static final String KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS = "kafka.message.coder.schema.registry.class";

  private static final byte MAGIC_BYTE = 0x0;
  private static final Logger logger = Logger.getLogger(KafkaAvroMessageEncoder.class);

  private SchemaRegistry<Schema> client;
  private final Map<Schema, String> cache = Collections.synchronizedMap(new HashMap<Schema, String>());
  private final EncoderFactory encoderFactory = EncoderFactory.get();

  @SuppressWarnings("unchecked")
  public KafkaAvroMessageEncoder(String topicName, Configuration conf) {
    this.topicName = topicName;

  }

  @Override
  public void init(Properties props, String topicName) {
    super.init(props, topicName);
    try {
      client =
          (SchemaRegistry<Schema>) Class.forName(props.getProperty(KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS))
              .newInstance();
      client.init(props);
    } catch (Exception e) {
      throw new MessageEncoderException(e);
    }

  }

  public byte[] toBytes(IndexedRecord record) {
    try {

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(MAGIC_BYTE);

      Schema schema = record.getSchema();

      // auto-register schema if it is not in the cache
      String id;
      if (!cache.containsKey(schema)) {
        try {
          id = client.register(topicName, record.getSchema());
          cache.put(schema, id);
        } catch (SchemaRegistryException e) {
          throw new RuntimeException(e);
        }
      } else {
        id = cache.get(schema);
      }

      out.write(ByteBuffer.allocate(4).putInt(Integer.parseInt(id)).array());

      BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
      DatumWriter<IndexedRecord> writer;

      if (record instanceof SpecificRecord)
        writer = new SpecificDatumWriter<IndexedRecord>(record.getSchema());
      else
        writer = new GenericDatumWriter<IndexedRecord>(record.getSchema());
      writer.write(record, encoder);

      System.err.println(out.toByteArray().length);
      return out.toByteArray();
      //return new Message(out.toByteArray());
    } catch (IOException e) {
      throw new MessageEncoderException(e);
    }
  }
}
