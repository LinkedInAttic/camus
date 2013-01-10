package com.linkedin.batch.etl.kafka.schemaregistry;

public class SchemaDetails {
	
	public String id;
	public String schema;
	public String topic;
	
	public SchemaDetails(String id, String schema, String topic) {
	}
	/**
	 * Get the schema
	 */
	
	
	public String getSchema(){
		return schema;
	}
	
	/**
	 * Get the topic
	 */
	
	public String getTopic(){
		return topic;
	}
	
	public String getId(){
		return id;
	}

	@Override
	public int hashCode() {
		return (topic + id).hashCode();
	}
}
