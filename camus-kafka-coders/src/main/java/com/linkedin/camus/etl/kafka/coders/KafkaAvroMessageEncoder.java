package com.linkedin.camus.etl.kafka.coders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.log4j.Logger;

import com.linkedin.camus.coders.MessageEncoder;
import com.linkedin.camus.coders.MessageEncoderException;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaDetails;

public class KafkaAvroMessageEncoder extends MessageEncoder<IndexedRecord, byte[]> {
  public static final String KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS = "kafka.message.coder.schema.registry.class";

  private static final byte MAGIC_BYTE = 0x0;
  private static final Logger logger = Logger.getLogger(KafkaAvroMessageEncoder.class);
  private final EncoderFactory encoderFactory = EncoderFactory.get();
  private SchemaDetails<Schema> schemaDetails;

  @SuppressWarnings("unchecked")
  public KafkaAvroMessageEncoder() {
  }

  @Override
  public void init(Properties props, String topicName) {
    super.init(props, topicName);
    try {
      SchemaRegistry registry =
          (SchemaRegistry) Class.forName(props.getProperty(KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS))
              .newInstance();
      registry.init(props);
      this.schemaDetails = registry.getLatestSchemaByTopic(topicName);
    } catch (Exception e) {
      throw new MessageEncoderException(e);
    }
  }

  public byte[] toBytes(IndexedRecord record) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(MAGIC_BYTE);

      if(this.schemaDetails == null){
        throw new IllegalStateException("Schema not found for this topic: " + this.topicName);
      }
      String id = this.schemaDetails.getId();

      out.write(ByteBuffer.allocate(4).putInt(Integer.parseInt(id)).array());

      BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
      DatumWriter<IndexedRecord> writer;

      if (record instanceof SpecificRecord)
        writer = new SpecificDatumWriter<IndexedRecord>(record.getSchema());
      else
        writer = new GenericDatumWriter<IndexedRecord>(record.getSchema());

      writer.write(record, encoder);

      //System.err.println(out.toByteArray().length);
      return out.toByteArray();
    } catch (IOException e) {
      throw new MessageEncoderException(e);
    }
  }
}
