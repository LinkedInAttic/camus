package com.linkedin.camus.etl.kafka.coders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import static com.linkedin.camus.etl.kafka.coders.JsonStringMessageDecoder.ISO_8601;
import static com.linkedin.camus.etl.kafka.coders.JsonStringMessageDecoder.UNIX;
import static com.linkedin.camus.etl.kafka.coders.JsonStringMessageDecoder.UNIX_MILLISECONDS;
import static com.linkedin.camus.etl.kafka.coders.JsonStringMessageDecoder.UNIX_SECONDS;
import static com.linkedin.camus.etl.kafka.coders.JsonStringMessageDecoder.CAMUS_MESSAGE_TIMESTAMP_FIELD;
import static com.linkedin.camus.etl.kafka.coders.JsonStringMessageDecoder.DEFAULT_TIMESTAMP_FIELD;
import static com.linkedin.camus.etl.kafka.coders.JsonStringMessageDecoder.CAMUS_MESSAGE_TIMESTAMP_FORMAT;

public class KafkaAvroMessageDecoder extends MessageDecoder<byte[], Record> {

  protected DecoderFactory decoderFactory;
  protected SchemaRegistry<Schema> registry;
  private Schema latestSchema;
  private String timestampFormat;
  private String timestampField;

  @Override
  public void init(Properties props, String topicName) {
    super.init(props, topicName);
    try {
      SchemaRegistry<Schema> registry =
          (SchemaRegistry<Schema>) Class.forName(
              props.getProperty(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS)).newInstance();

      registry.init(props);

      this.registry = new CachedSchemaRegistry<Schema>(registry);
      this.latestSchema = registry.getLatestSchemaByTopic(topicName).getSchema();

      timestampField = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FIELD, DEFAULT_TIMESTAMP_FIELD);
      timestampFormat = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FORMAT, UNIX_MILLISECONDS);
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
      if (buffer.get() != MAGIC_BYTE)
        throw new IllegalArgumentException("Unknown magic byte!");
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
              decoderFactory.binaryDecoder(helper.getBuffer().array(), helper.getStart(), helper.getLength(), null)),
              timestampField, timestampFormat);

    } catch (IOException e) {
      throw new MessageDecoderException(e);
    }
  }

  public static class CamusAvroWrapper extends CamusWrapper<Record> {

    private final String timestampField;
    private final String timestampFormat;
    private final DateTimeFormatter isoDateTimeParser = ISODateTimeFormat.dateTimeParser();

    public CamusAvroWrapper(Record record) {
      this(record, DEFAULT_TIMESTAMP_FIELD, UNIX_MILLISECONDS);
    }

    public CamusAvroWrapper(Record read, String timestampField, String timestampFormat) {
      super(read);

      this.timestampField = timestampField;
      this.timestampFormat = timestampFormat;

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
    public long getTimestamp() throws ParseException {
      Record header = (Record) super.getRecord().get("header");

      if (header != null && header.get("time") != null) {
        return (Long) header.get("time");
      } else if (super.getRecord().get(timestampField) != null) {//timestampField defaults to 'timestamp', can be set to null
        Object eventTimestamp = super.getRecord().get(timestampField);
        if (UNIX_MILLISECONDS.equals(timestampFormat)){ // timestampFormat default to UNIX_MILLISECONDS
          return getAsLong(eventTimestamp);
        }else if (UNIX_SECONDS.equals(timestampFormat) || UNIX.equals(timestampFormat)){
          return getAsLong(eventTimestamp) * 1000;
        }else if(ISO_8601.equals(timestampFormat)) {
          return isoDateTimeParser.parseDateTime(eventTimestamp.toString()).getMillis();
        }else {
          return new SimpleDateFormat(timestampFormat).parse(eventTimestamp.toString()).getTime();
        }
      } else {
        return System.currentTimeMillis();
      }
    }

    private long getAsLong(Object eventTimestamp) {
      if (eventTimestamp instanceof Long) {
        return (Long) eventTimestamp;
      } else {
        return Long.parseLong(eventTimestamp.toString());
      }
    }
  }
}
