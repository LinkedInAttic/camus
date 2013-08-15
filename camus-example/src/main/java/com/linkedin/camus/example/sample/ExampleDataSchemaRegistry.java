package com.linkedin.camus.example.sample;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import com.linkedin.camus.example.sample.ExampleData;
import com.linkedin.camus.schemaregistry.MemorySchemaRegistry;

/**
 * This is a little dummy registry that just uses a memory-backed schema
 * registry to store two dummy Avro schemas. You can use this with
 * camus.properties
 */
public class ExampleDataSchemaRegistry extends MemorySchemaRegistry<Schema> {
    public ExampleDataSchemaRegistry(Configuration conf) {
	super();

	// EXAMPLE_LOG is the topic we're registering this schema against
	super.register("EXAMPLE_LOG", ExampleData.newBuilder().build().getSchema());
    }
}
