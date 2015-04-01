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
        for (int i = 0; i < 10 && i < payload.length; i++)
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
    public static final String TIMESTAMP_NODE = "timestamp.node";
    public static final String SERVER_NODE = "server.node";
    public static final String SERVICE_NODE = "service.node";

    private static String timestampNode = "header.timestamp";
    private static String serverNode = "header.server";
    private static String serviceNode = "header.service";

    public static void SetProperties(Properties props) {
      timestampNode = props.getProperty(TIMESTAMP_NODE, timestampNode);
      serverNode = props.getProperty(SERVER_NODE, serverNode);
      serviceNode = props.getProperty(SERVICE_NODE, serviceNode);
    }

    public CamusAvroWrapper(Record record) {
      super(record);

      Text serverValue = getTextFromNode(serverNode, null);
      if(serverValue != null){
        put(new Text("server"), serverValue);
      }

      Text serviceValue = getTextFromNode(serviceNode, null);
      if(serviceValue != null){
        put(new Text("service"), serviceValue);
      }
    }

    @Override
    public long getTimestamp() {
      String[] nodes = timestampNode.split("\\s*,\\s*");  //split on comma, trim spaces
      Long timeStamp = null;
      int countNodes = 0;

      while (timeStamp == null && countNodes < nodes.length)
        if (nodes[countNodes].length() > 0) {
          timeStamp = getLongFromNode(nodes[countNodes], null);
          countNodes++;
        }
      return timeStamp == null ? System.currentTimeMillis() : timeStamp;
    }

    //due to conversion issues with Text (a hadoop class) this was not wrapped into a generic method
    private Text getTextFromNode(String nodes, Record newRecord) {
      Text value = null;
      int dotIndex = nodes.indexOf(".");

      if (dotIndex != -1) {
        Record tempRecord = null;
        if(newRecord == null){
          tempRecord = (Record) super.getRecord().get(nodes.split("\\.")[0]);
        } else {
          tempRecord = (Record) newRecord.get(nodes.split("\\.")[0]);
        }
        if (tempRecord != null) {
          value = getTextFromNode(nodes.substring(dotIndex + 1), tempRecord);
        }
      }

      if (newRecord != null && newRecord.get(nodes) != null) {
        return value == null ? new Text(newRecord.get(nodes).toString()) : value;
      } else {
        return value;
      }
    }

    private Long getLongFromNode(String nodes, Record newRecord) {
      Long value = null;

      int dotIndex = nodes.indexOf(".");
      if (dotIndex != -1) {
        Record tempRecord = null;
        if(newRecord == null){
          tempRecord = (Record) super.getRecord().get(nodes.split("\\.")[0]);
        } else {
          tempRecord = (Record) newRecord.get(nodes.split("\\.")[0]);
        }
        if (tempRecord != null) {
          value = getLongFromNode(nodes.substring(dotIndex + 1), tempRecord);
        }
      }

      if (newRecord != null && newRecord.get(nodes) != null) {
        return (value == null) ? (Long) newRecord.get(nodes) : value;
      } else{
        return value;
      }
    }
  }
}
