package com.linkedin.camus.etl.kafka.coders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.schemaregistry.CachedSchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import org.apache.hadoop.io.Text;


public class KafkaAvroMessageDecoder extends MessageDecoder<byte[], Record> {
  protected DecoderFactory decoderFactory;
  protected SchemaRegistry<Schema> registry;
  private Schema latestSchema;

  @Override
  public void init(Properties props, String topicName) {
    super.init(props, topicName);
    try {
      SchemaRegistry<Schema> registry =
          (SchemaRegistry<Schema>) Class.forName(
              props.getProperty(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS)).newInstance();

      registry.init(props);
      CamusAvroWrapper.SetProperties(props);

      this.registry = new CachedSchemaRegistry<Schema>(registry);
      this.latestSchema = registry.getLatestSchemaByTopic(topicName).getSchema();
    } catch (Exception e) {
      throw new MessageDecoderException(e);
    }

    decoderFactory = DecoderFactory.get();
  }

  public class MessageDecoderHelper {
    //private Message message;
    private ByteBuffer buffer;
    private Schema schema;
    private int start;
    private int length;
    private Schema targetSchema;
    private static final byte MAGIC_BYTE = 0x0;
    private final SchemaRegistry<Schema> registry;
    private final String topicName;
    private byte[] payload;

    public MessageDecoderHelper(SchemaRegistry<Schema> registry, String topicName, byte[] payload) {
      this.registry = registry;
      this.topicName = topicName;
      //this.message = message;
      this.payload = payload;
    }

    public ByteBuffer getBuffer() {
      return buffer;
    }

    public Schema getSchema() {
      return schema;
    }

    public int getStart() {
      return start;
    }

    public int getLength() {
      return length;
    }

    public Schema getTargetSchema() {
      return targetSchema;
    }

    private ByteBuffer getByteBuffer(byte[] payload) {
      ByteBuffer buffer = ByteBuffer.wrap(payload);
			if (buffer.get() != MAGIC_BYTE) {
                String dump = "";
                for(int i=0; i<10 && i<payload.length; i++)
                    dump += String.format("%02X ", payload[i]);
                throw new IllegalArgumentException("Unknown magic byte! " + dump);
            }
      return buffer;
    }

    public MessageDecoderHelper invoke() {
      buffer = getByteBuffer(payload);
      String id = Integer.toString(buffer.getInt());
      schema = registry.getSchemaByID(topicName, id);
      if (schema == null)
        throw new IllegalStateException("Unknown schema id: " + id);

      start = buffer.position() + buffer.arrayOffset();
      length = buffer.limit() - 5;

      // try to get a target schema, if any
      targetSchema = latestSchema;
      return this;
    }
  }

  public CamusWrapper<Record> decode(byte[] payload) {
    try {
      MessageDecoderHelper helper = new MessageDecoderHelper(registry, topicName, payload).invoke();
      DatumReader<Record> reader =
          (helper.getTargetSchema() == null) ? new GenericDatumReader<Record>(helper.getSchema())
              : new GenericDatumReader<Record>(helper.getSchema(), helper.getTargetSchema());

      return new CamusAvroWrapper(reader.read(null,
          decoderFactory.binaryDecoder(helper.getBuffer().array(), helper.getStart(), helper.getLength(), null)));

    } catch (IOException e) {
      throw new MessageDecoderException(e);
    }
  }

  public static class CamusAvroWrapper extends CamusWrapper<Record> {
      public static final String CAMUS_REQUEST_TIMESTAMP_FIELD = "camus.request.timestamp.field";
      public static final String CAMUS_MESSAGE_TIMESTAMP_FIELD = "camus.message.timestamp.field";
      public static final String CAMUS_MESSAGE_TIMESTAMP_SUBFIELD = "camus.message.timestamp.subfield";
      public static final String DEFAULT_HEADER_FIELD = "default.header.field";
      public static final String DEFAULT_TIMESTAMP_FIELD = "default.timestamp.field";
      public static final String CUSTOM_TIMESTAMP_FIELD = "custom.timestamp.field";

      private static String camusRequestTimeStamp = "RequestDateUtc";
      private static String camusTimeStamp = "LogDate";
      private static String camusTimeStampSubField =  "msSinceEpoch";
      private static String defaultHeader = "header";
      private static String defaultTimeStamp = "time";
      private static String customTimeStampField = "timestamp";

      public static void SetProperties(Properties props){
          camusRequestTimeStamp = props.getProperty(CAMUS_REQUEST_TIMESTAMP_FIELD, camusRequestTimeStamp);
          camusTimeStamp = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FIELD, camusTimeStamp);
          camusTimeStampSubField = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_SUBFIELD, camusTimeStampSubField);
          defaultHeader = props.getProperty(DEFAULT_HEADER_FIELD, defaultHeader);
          defaultTimeStamp = props.getProperty(DEFAULT_TIMESTAMP_FIELD, defaultTimeStamp);
          customTimeStampField = props.getProperty(CUSTOM_TIMESTAMP_FIELD, customTimeStampField);}

    public CamusAvroWrapper(Record record) {
      super(record);
      Record header = (Record) super.getRecord().get("header");
      if (header != null) {
        if (header.get("server") != null) {
          put(new Text("server"), new Text(header.get("server").toString()));
        }
        if (header.get("service") != null) {
          put(new Text("service"), new Text(header.get("service").toString()));
        }
      }
    }

    @Override
    public long getTimestamp() {
        //*CAMUS_REQUEST_TIMESTAMP_FIELD*//
        Record customLogDate = (Record) super.getRecord().get(camusRequestTimeStamp);
        if (customLogDate != null && customLogDate.get(camusTimeStampSubField) != null) {
            return Long.parseLong(customLogDate.get(camusTimeStampSubField).toString());
        } else{
            //*CAMUS_MESSAGE_TIMESTAMP_FIELD*//
            Record logDate = (Record) super.getRecord().get(camusTimeStamp);
            if (logDate != null && logDate.get(camusTimeStampSubField) != null) {
                return Long.parseLong(logDate.get(camusTimeStampSubField).toString());
            }
            //*DEFAULT_HEADER_FIELD*//
            Record header = (Record) super.getRecord().get(defaultHeader);
            //*DEFAULT_TIMESTAMP_FIELD*//
            if (header != null && header.get(defaultTimeStamp) != null) {
                return (Long) header.get(defaultTimeStamp);
            }
            //*CUSTOM_TIMESTAMP_FIELD*//
            else if (super.getRecord().get(customTimeStampField) != null) {
                return (Long) super.getRecord().get(customTimeStampField);
            } else {
                return System.currentTimeMillis();
            }
        }
    }
  }
}
