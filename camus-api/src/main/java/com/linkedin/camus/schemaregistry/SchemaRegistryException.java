package com.linkedin.camus.schemaregistry;

public class SchemaRegistryException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public SchemaRegistryException() {
		super();
	}

	public SchemaRegistryException(String s, Throwable t) {
		super(s, t);
	}

	public SchemaRegistryException(String s) {
		super(s);
	}

	public SchemaRegistryException(Throwable t) {
		super(t);
	}
}