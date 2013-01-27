package com.linkedin.camus.schemaregistry;

public class SchemaNotFoundException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public SchemaNotFoundException() {
		super();
	}

	public SchemaNotFoundException(String s, Throwable t) {
		super(s, t);
	}

	public SchemaNotFoundException(String s) {
		super(s);
	}

	public SchemaNotFoundException(Throwable t) {
		super(t);
	}
}