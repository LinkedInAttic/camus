package com.linkedin.batch.etl.kafka.schemaregistry;

public abstract class SchemaDetails {
	
	public String schema;
	public String topic;
	
	public SchemaDetails(String schema, String topic) {
	}
	/**
	 * Get the schema
	 */
	
	
	public abstract String getSchema();
	
	/**
	 * Get the topic
	 */
	
	public abstract String getTopic();

}
