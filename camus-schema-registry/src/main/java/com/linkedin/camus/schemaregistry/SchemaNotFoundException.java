package com.linkedin.camus.schemaregistry;

public class SchemaNotFoundException extends Exception {

	public SchemaNotFoundException(String message) {
		super(message);
	}

	public SchemaNotFoundException(String message, Exception e) {
		super(message, e);
	}
}