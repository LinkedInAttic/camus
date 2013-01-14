package com.linkedin.batch.etl.kafka.schemaregistry;

public class SchemaDetails {
	
	private String id;
	private String schema;
	private String topic;
	
	public SchemaDetails(String id, String schema, String topic) {
		this.id = id;
		this.schema = schema;
		this.topic = topic;
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
