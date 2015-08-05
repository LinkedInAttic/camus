package com.linkedin.camus.schemaregistry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(value = Parameterized.class)
public class TestSchemaRegistries {
  protected final SchemaRegistry registry;

  public TestSchemaRegistries(SchemaRegistry registry) {
    this.registry = registry;
  }

  public Object getSchema1() {
    return "my-schema";
  }

  public Object getSchema2() {
    return "my-schema-2";
  }

  @Test
  public void testSchemaRegistry() {
    try {
      registry.getLatestSchemaByTopic("test");
      fail("Should have failed with a SchemaNotFoundException.");
    } catch (SchemaNotFoundException e) {
      // expected
    }

    try {
      registry.getSchemaByID("test", "abc");
      fail("Should have failed with a SchemaNotFoundException.");
    } catch (SchemaNotFoundException e) {
      // expected
    }

    String id = registry.register("test", getSchema1());

    try {
      registry.getSchemaByID("test", "abc");
      fail("Should have failed with a SchemaNotFoundException.");
    } catch (SchemaNotFoundException e) {
      // expected
    }

    assertEquals(getSchema1(), registry.getSchemaByID("test", id));
    assertEquals(new SchemaDetails("test", id, getSchema1()), registry.getLatestSchemaByTopic("test"));

    String secondId = registry.register("test", getSchema2());

    assertEquals(getSchema1(), registry.getSchemaByID("test", id));
    assertEquals(getSchema2(), registry.getSchemaByID("test", secondId));
    assertEquals(new SchemaDetails("test", secondId, getSchema2()), registry.getLatestSchemaByTopic("test"));

    try {
      registry.getLatestSchemaByTopic("test-2");
      fail("Should have failed with a SchemaNotFoundException.");
    } catch (SchemaNotFoundException e) {
      // expected
    }

    try {
      registry.getSchemaByID("test-2", "");
      fail("Should have failed with a SchemaNotFoundException.");
    } catch (SchemaNotFoundException e) {
      // expected
    }

    secondId = registry.register("test", getSchema2());

    assertEquals(getSchema1(), registry.getSchemaByID("test", id));
    assertEquals(getSchema2(), registry.getSchemaByID("test", secondId));
    assertEquals(new SchemaDetails("test", secondId, getSchema2()), registry.getLatestSchemaByTopic("test"));
  }

  @Parameters
  public static Collection data() {
    File tmpDir = new File("/tmp/test-" + System.currentTimeMillis());
    SchemaRegistry<String> fileRegistry = new FileSchemaRegistry<String>(tmpDir, new StringSerde());
    SchemaRegistry<String> memoryRegistry = new MemorySchemaRegistry<String>();
    Object[][] data = new Object[][] { { fileRegistry }, { memoryRegistry } };
    return Arrays.asList(data);
  }

  public static class StringSerde implements Serde<String> {
    @Override
    public byte[] toBytes(String obj) {
      try {
        return obj.getBytes("utf-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String fromBytes(byte[] bytes) {
      try {
        return new String(bytes, "utf-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
