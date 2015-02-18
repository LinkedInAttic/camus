package com.linkedin.camus.schemaregistry;

import java.util.Properties;

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class CachedSchemaRegistryTest {
  private static final String GETSCHEMA_BYID_MAX_RETIRES = "getschema.byid.max.retries";
  private static final String GETSCHEMA_BYID_MIN_INTERVAL_SECONDS = "getschema.byid.min.interval.seconds";
  private CachedSchemaRegistry<Schema> cachedRegistry;
  private SchemaRegistry<Schema> registry;
  private Properties props;

  @SuppressWarnings("unchecked")
  @Before
  public void setupRegistryMock() {
    props = new Properties();
    registry = Mockito.mock(SchemaRegistry.class);
    Mockito.when(
        registry.getSchemaByID(Mockito.anyString(), Mockito.anyString()))
        .thenThrow(new SchemaNotFoundException());
    cachedRegistry = new CachedSchemaRegistry<Schema>(registry);
  }

  @Test
  public void testMaxRetries() {
    props.setProperty(GETSCHEMA_BYID_MAX_RETIRES, "10");
    cachedRegistry.init(props);
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
    Mockito.verify(registry, Mockito.times(20)).getSchemaByID(
        Mockito.anyString(), Mockito.anyString());
  }

  @Test
  public void testMinInterval() throws InterruptedException {
    props.setProperty(GETSCHEMA_BYID_MIN_INTERVAL_SECONDS, "2");
    cachedRegistry.init(props);
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
    Mockito.verify(registry, Mockito.times(4)).getSchemaByID(
        Mockito.anyString(), Mockito.anyString());
  }
}
