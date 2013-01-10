package com.linkedin.batch.etl.kafka.schemaregistry;

public class SchemaRegistryException extends Exception
{
	public SchemaRegistryException(Exception e) {
		super(e);
	}
}
