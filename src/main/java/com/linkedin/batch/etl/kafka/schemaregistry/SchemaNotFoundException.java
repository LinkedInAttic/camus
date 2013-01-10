package com.linkedin.batch.etl.kafka.schemaregistry;

public class SchemaNotFoundException extends Exception {
	
	public SchemaNotFoundException(String message) {
		super(message);
	}

	public SchemaNotFoundException(String message, Exception e) {
		super(message, e);
	}
}
