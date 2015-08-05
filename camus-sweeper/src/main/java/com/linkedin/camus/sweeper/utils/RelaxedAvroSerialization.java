package com.linkedin.camus.sweeper.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroKeyDeserializer;
import org.apache.avro.hadoop.io.AvroSerializer;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.hadoop.io.AvroValueDeserializer;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;


/**
 * The AvroSerialization class to relax the name validation
 * 
 * @author hcai
 *
 * @param <T> The Java type of the Avro data to serialize.
 */
public class RelaxedAvroSerialization<T> extends AvroSerialization<T> {
  private static final Log LOG = LogFactory.getLog(RelaxedAvroSerialization.class.getName());

  private static final String CONF_KEY_WRITER_SCHEMA = "avro.serialization.key.writer.schema";
  private static final String CONF_KEY_READER_SCHEMA = "avro.serialization.key.reader.schema";
  private static final String CONF_VALUE_WRITER_SCHEMA = "avro.serialization.value.writer.schema";
  private static final String CONF_VALUE_READER_SCHEMA = "avro.serialization.value.reader.schema";

  public static void addToConfiguration(Configuration conf) {
    Collection<String> serializations = conf.getStringCollection("io.serializations");
    if (!serializations.contains(RelaxedAvroSerialization.class.getName())) {
      LOG.info("Prepending RelaxedAvroSerialization.class");
      // prepend RelaxedAvroSerialization
      List<String> newList = new ArrayList<String>();
      newList.add(RelaxedAvroSerialization.class.getName());
      newList.addAll(serializations);
      conf.setStrings("io.serializations", newList.toArray(new String[newList.size()]));
    }
  }

  @Override
  public Serializer<AvroWrapper<T>> getSerializer(Class<AvroWrapper<T>> c) {
    LOG.info("getSerializer for " + c);
    Schema schema;
    if (AvroKey.class.isAssignableFrom(c)) {
      schema = getKeyWriterSchema(getConf());
    } else if (AvroValue.class.isAssignableFrom(c)) {
      schema = getValueWriterSchema(getConf());
    } else {
      throw new IllegalStateException("Only AvroKey and AvroValue are supported.");
    }
    return new AvroSerializer<T>(schema);
  }

  @Override
  public Deserializer<AvroWrapper<T>> getDeserializer(Class<AvroWrapper<T>> c) {
    LOG.info("getDeerializer for " + c);
    Configuration conf = getConf();
    if (AvroKey.class.isAssignableFrom(c)) {
      return new AvroKeyDeserializer<T>(getKeyWriterSchema(conf), getKeyReaderSchema(conf), conf.getClassLoader());
    } else if (AvroValue.class.isAssignableFrom(c)) {
      return new AvroValueDeserializer<T>(getValueWriterSchema(conf), getValueReaderSchema(conf), conf.getClassLoader());
    } else {
      throw new IllegalStateException("Only AvroKey and AvroValue are supported.");
    }
  }

  public static Schema getKeyReaderSchema(Configuration conf) {
    String json = conf.get(CONF_KEY_READER_SCHEMA);
    return null == json ? null : RelaxedSchemaUtils.parseSchema(json, conf);
  }

  public static Schema getValueReaderSchema(Configuration conf) {
    String json = conf.get(CONF_VALUE_READER_SCHEMA);
    return null == json ? null : RelaxedSchemaUtils.parseSchema(json, conf);
  }

  public static Schema getKeyWriterSchema(Configuration conf) {
    String json = conf.get(CONF_KEY_WRITER_SCHEMA);
    return null == json ? null : RelaxedSchemaUtils.parseSchema(json, conf);
  }

  public static Schema getValueWriterSchema(Configuration conf) {
    String json = conf.get(CONF_VALUE_WRITER_SCHEMA);
    return null == json ? null : RelaxedSchemaUtils.parseSchema(json, conf);
  }

}
