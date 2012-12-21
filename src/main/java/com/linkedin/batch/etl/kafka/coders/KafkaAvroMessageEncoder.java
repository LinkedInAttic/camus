package com.linkedin.batch.etl.kafka.coders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

import kafka.message.Message;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.repository.SchemaRepository;
import org.apache.avro.repository.SchemaRepositoryException;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.log4j.Logger;

public class KafkaAvroMessageEncoder
{

  private static final byte             MAGIC_BYTE = 0x0;
  private static final Logger           logger     =
                                                       Logger.getLogger(KafkaAvroMessageDecoder.class);

  private static SchemaRepository       registry   = null;
  // private final SchemaRegistryClient client;
  private static String                 source;
  private final HashMap<String, String> cache;

  public KafkaAvroMessageEncoder()
  {
    this(null, null);
  }

  public KafkaAvroMessageEncoder(SchemaRepository registry, String source)
  {

    // Get the source information get sorted here
    this.source = source;
    this.registry = registry;
    this.cache =
        (HashMap<String, String>) Collections.synchronizedMap(new HashMap<String, String>());
  }

  public Message toMessage(IndexedRecord record)
  {
    try
    {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(MAGIC_BYTE);
      /* TODO: this is expensive and should be cached somehow */
      byte[] schemaBytes = Utils.getBytes(record.getSchema().toString(false), "UTF-8");
      logger.debug("Schema string UTF-8 bytes in hex: " + Utils.hex(schemaBytes));
      byte[] md5Bytes = Utils.md5(schemaBytes);
      logger.debug("MD5 bytes in hex: " + Utils.hex(md5Bytes));
      // SchemaId id = new SchemaId(md5Bytes);
      String id = md5Bytes.toString();
      String schema = schemaBytes.toString();

      // Concatenation of the source and the string is the primary unique string
      String key = source + id;
      if (registry != null && !cache.containsKey(key))
      {
        try
        {
          schema = registry.lookup(id, source);
        }
        catch (SchemaRepositoryException e)
        {
          try
          {
            id = registry.register(source, schema);
          }
          catch (Exception ex)
          {
            throw new AvroIncompatibleSchemaException("Unable to register new schema, so aboring message conversion.");
          }
          cache.put(source + id, schema);
        }
      }

      // This is the id that is used to fetch the schema
      out.write(md5Bytes);
      BinaryEncoder encoder = new BinaryEncoder(out);
      DatumWriter<IndexedRecord> writer;
      if (record instanceof SpecificRecord)
      {
        writer = new SpecificDatumWriter<IndexedRecord>(record.getSchema());
      }
      else
      {
        writer = new GenericDatumWriter<IndexedRecord>(record.getSchema());
      }
      writer.write(record, encoder);
      return new Message(out.toByteArray());
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }
  }
}
