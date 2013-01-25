package com.linkedin.camus.schemaregistry;

import org.apache.avro.Schema;

public class SchemaDetails
{
	private String topic;
	private String id;
	private Schema schema;

	public SchemaDetails(String topic, String id, Schema schema) {
		this.topic = topic;
		this.id = id;
		this.schema = schema;
	}
	
	public SchemaDetails(String topic, String id) {
		this.topic = topic;
		this.id = id;
	}
	
	/**
	 * Get the schema
	 */
	public Schema getSchema(){
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