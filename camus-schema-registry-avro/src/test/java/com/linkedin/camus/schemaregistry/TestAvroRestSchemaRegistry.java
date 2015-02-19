package com.linkedin.camus.schemaregistry;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.repo.InMemoryRepository;
import org.apache.avro.repo.server.RepositoryServer;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(value = Parameterized.class)
public class TestAvroRestSchemaRegistry extends TestSchemaRegistries {
  private RepositoryServer server;

  public static final Schema SCHEMA1 =
      new Schema.Parser()
          .parse("{\"type\":\"record\",\"name\":\"DummyLog2\",\"namespace\":\"com.linkedin.camus.example.records\",\"doc\":\"Logs for really important stuff.\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"muchoStuff\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}");
  public static final Schema SCHEMA2 =
      new Schema.Parser()
          .parse("{\"type\":\"record\",\"name\":\"DummyLog2\",\"namespace\":\"com.linkedin.camus.example.records\",\"doc\":\"Logs for really important stuff.\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"muchoStuff\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}");

  public TestAvroRestSchemaRegistry(SchemaRegistry<String> registry) {
    super(registry);
  }

  @Override
  public Object getSchema1() {
    return SCHEMA1;
  }

  @Override
  public Object getSchema2() {
    return SCHEMA2;
  }

  @Before
  public void doSetup() throws Exception {
    Properties props = new Properties();
    props.put("avro.repo.class", InMemoryRepository.class.getName());
    props.put("jetty.host", "localhost");
    props.put("jetty.port", "8123");
    server = new RepositoryServer(props);
    server.start();
  }

  @After
  public void doTearDown() throws Exception {
    server.stop();
  }

  @Parameters
  public static Collection data() {
    Properties props = new Properties();
    props.put(AvroRestSchemaRegistry.ETL_SCHEMA_REGISTRY_URL, "http://localhost:8123/schema-repo/");
    SchemaRegistry<Schema> avroSchemaRegistry = new AvroRestSchemaRegistry();

    avroSchemaRegistry.init(props);
    Object[][] data = new Object[][] { { avroSchemaRegistry } };
    return Arrays.asList(data);
  }
}
