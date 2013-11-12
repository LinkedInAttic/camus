package com.linkedin.camus.schemaregistry;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

/**
 * This is an in-memory implementation of a SchemaRegistry that stores
 * Avro schemas and the Avro fingerprint of the schema as it's id
 */
public class AvroMemorySchemaRegistry extends MemorySchemaRegistry<Schema> {
	
	@Override
    protected long generateSchemaId(Schema schema) {
	    return SchemaNormalization.parsingFingerprint64(schema);
    }
}