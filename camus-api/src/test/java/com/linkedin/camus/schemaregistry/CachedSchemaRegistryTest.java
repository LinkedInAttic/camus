package com.linkedin.camus.schemaregistry;

import java.util.Properties;

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.easymock.EasyMock;


public class CachedSchemaRegistryTest {
  private static final String GET_SCHEMA_BY_ID_MAX_RETIRES = "get.schema.by.id.max.retries";
  private static final String GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS = "get.schema.by.id.min.interval.seconds";
  private CachedSchemaRegistry<Schema> cachedRegistry;
  private SchemaRegistry<Schema> registry;
  private Properties props;

  @SuppressWarnings("unchecked")
  @Before
  public void setupRegistryMock() {
    props = new Properties();
    registry = EasyMock.createNiceMock(SchemaRegistry.class);
  }

  @Test
  public void testMaxRetries() {
    EasyMock.expect(registry.getSchemaByID(EasyMock.anyString(), EasyMock.anyString())).andThrow(
        new SchemaNotFoundException());
    EasyMock.expectLastCall().times(20);
    EasyMock.replay(registry);
    props.setProperty(GET_SCHEMA_BY_ID_MAX_RETIRES, "10");
    props.setProperty(GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS, "0");
    cachedRegistry = new CachedSchemaRegistry<Schema>(registry, props);
    for (int i = 0; i < 100; i++) {
      try {
        cachedRegistry.getSchemaByID("dummyTopic", "dummyID");
      } catch (SchemaNotFoundException e) {
      }
      try {
        cachedRegistry.getSchemaByID("dummyTopic", "dummyID2");
      } catch (SchemaNotFoundException e) {
      }
    }
    EasyMock.verify(registry);
  }

  @Test
  public void testMinInterval() throws InterruptedException {
    EasyMock.expect(registry.getSchemaByID(EasyMock.anyString(), EasyMock.anyString())).andThrow(
        new SchemaNotFoundException());
    EasyMock.expectLastCall().times(4);
    EasyMock.replay(registry);
    props.setProperty(GET_SCHEMA_BY_ID_MAX_RETIRES, String.valueOf(Integer.MAX_VALUE));
    props.setProperty(GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS, "2");
    cachedRegistry = new CachedSchemaRegistry<Schema>(registry, props);
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 5; j++) {
        try {
          cachedRegistry.getSchemaByID("dummyTopic", "dummyID");
        } catch (SchemaNotFoundException e) {
        }
        try {
          cachedRegistry.getSchemaByID("dummyTopic", "dummyID2");
        } catch (SchemaNotFoundException e) {
        }
      }
      Thread.sleep(2500);
    }
    EasyMock.verify(registry);
  }
}
