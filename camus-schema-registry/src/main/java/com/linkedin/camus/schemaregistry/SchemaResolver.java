package com.linkedin.camus.schemaregistry;

import org.apache.avro.Schema;

public interface SchemaResolver {

	public Schema resolve(String id);
	
	public Schema getTargetSchema();
}
