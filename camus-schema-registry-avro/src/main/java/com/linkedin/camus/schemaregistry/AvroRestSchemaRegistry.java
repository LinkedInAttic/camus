package com.linkedin.camus.schemaregistry;

import java.util.Properties;

import org.apache.avro.Schema;
import org.schemarepo.SchemaEntry;
import org.schemarepo.SchemaValidationException;
import org.schemarepo.Subject;
import org.schemarepo.SubjectConfig;
import org.schemarepo.client.RESTRepositoryClient;
import org.apache.hadoop.conf.Configuration;


/**
 * An implementation of SchemaRegistry that uses Avro's schema registry to
 * manage Avro schemas.
 */
public class AvroRestSchemaRegistry implements SchemaRegistry<Schema> {
  private RESTRepositoryClient client;
  public static final String ETL_SCHEMA_REGISTRY_URL = "etl.schema.registry.url";

  @Override
  public void init(Properties props) {
    client = new RESTRepositoryClient(props.getProperty(ETL_SCHEMA_REGISTRY_URL),new org.schemarepo.json.GsonJsonUtil(),false);
  }

  @Override
  public String register(String topic, Schema schema) {
    Subject subject = client.lookup(topic);

    if (subject == null) {
      subject = client.register(topic, new SubjectConfig.Builder().build());
    }

    try {
      return subject.register(schema.toString()).getId();
    } catch (SchemaValidationException e) {
      throw new SchemaRegistryException(e);
    }
  }

  @Override
  public Schema getSchemaByID(String topic, String id) {
    Subject subject = client.lookup(topic);

    if (subject == null) {
      throw new SchemaNotFoundException("Schema not found for " + topic);
    }

    SchemaEntry entry = subject.lookupById(id);

    if (entry == null)
      throw new SchemaNotFoundException("Schema not found for " + topic + " " + id);

    return Schema.parse(entry.getSchema());
  }

  @Override
  public SchemaDetails<Schema> getLatestSchemaByTopic(String topicName) {
    Subject subject = client.lookup(topicName);

    if (subject == null) {
      throw new SchemaNotFoundException("Schema not found for " + topicName);
    }

    SchemaEntry entry = subject.latest();

    if (entry == null)
      throw new SchemaNotFoundException("Schema not found for " + topicName);

    return new SchemaDetails<Schema>(topicName, entry.getId(), Schema.parse(entry.getSchema()));
  }
}
