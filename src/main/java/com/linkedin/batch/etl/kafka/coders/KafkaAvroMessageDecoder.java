package com.linkedin.batch.etl.kafka.coders;

import java.io.IOException;
import java.nio.ByteBuffer;

import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import com.linkedin.batch.etl.kafka.schemaregistry.CachedSchemaResolver;
import com.linkedin.batch.etl.kafka.schemaregistry.SchemaRegistry;

public class KafkaAvroMessageDecoder
{

  private static final byte      MAGIC_BYTE = 0x0;
  private final CachedSchemaResolver resolver;
  private final DecoderFactory   decoderFactory;

  public KafkaAvroMessageDecoder(CachedSchemaResolver resolver)
  {
    this.resolver = resolver;
    this.decoderFactory = new DecoderFactory();
  }

  public Record toRecord(Message message)
  {
    return toRecord(message, null);
  }

  public <T extends SpecificRecord> T toSpecificRecord(Message message)
  {
    T specificRecord = this.<T> toSpecificRecord(message, null);
    return specificRecord;
  }

  public Record toRecord(Message message, BinaryDecoder decoderReuse)
  {
    MessageDecoderHelper helper = new MessageDecoderHelper(message).invoke();
    try
    {
      DatumReader<Record> reader =
          (helper.getTargetSchema() == null)
              ? new GenericDatumReader<Record>(helper.getSchema())
              : new GenericDatumReader<Record>(helper.getSchema(),
                                               helper.getTargetSchema());

      return reader.read(null, decoderFactory.createBinaryDecoder(helper.getBuffer()
                                                                        .array(),
                                                                  helper.getStart(),
                                                                  helper.getLength(),
                                                                  decoderReuse));
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }
  }

  public <T extends SpecificRecord> T toSpecificRecord(Message message,
                                                       BinaryDecoder decoderReuse)
  {
    MessageDecoderHelper helper = new MessageDecoderHelper(message).invoke();
    try
    {
      SpecificDatumReader<T> reader =
          (helper.getTargetSchema() == null)
              ? new SpecificDatumReader<T>(helper.getSchema())
              : new SpecificDatumReader<T>(helper.getSchema(), helper.getTargetSchema());

      return reader.read(null, decoderFactory.createBinaryDecoder(helper.getBuffer()
                                                                        .array(),
                                                                  helper.getStart(),
                                                                  helper.getLength(),
                                                                  decoderReuse));
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }
  }

  private class MessageDecoderHelper
  {
    private final Message _message;
    private ByteBuffer    _buffer;
    private Schema        _schema;
    private int           _start;
    private int           _length;
    private Schema        _targetSchema;

    public MessageDecoderHelper(Message message)
    {
      _message = message;
    }

    public ByteBuffer getBuffer()
    {
      return _buffer;
    }

    public Schema getSchema()
    {
      return _schema;
    }

    public int getStart()
    {
      return _start;
    }

    public int getLength()
    {
      return _length;
    }

    public Schema getTargetSchema()
    {
      return _targetSchema;
    }

    private ByteBuffer getByteBuffer(Message message)
    {
      ByteBuffer buffer = message.payload();
      if (buffer.get() != MAGIC_BYTE)
      {
        throw new IllegalArgumentException("Unknown magic byte!");
      }
      return buffer;
    }

    public MessageDecoderHelper invoke()
    {
      _buffer = getByteBuffer(_message);
      byte[] md5 = new byte[16];
      _buffer.get(md5);

      _schema = Schema.parse(resolver.resolve(md5.toString()));
      if (_schema == null)
      {
        throw new IllegalStateException("Unknown schema id: " + Utils.hex(md5));
      }

      _start = _buffer.position() + _buffer.arrayOffset();
      _length = _buffer.limit() - md5.length - 1;

      // try to get a target schema, if any
      // The default implementation always passes null
      _targetSchema = null;
      return this;
    }
  }
}
