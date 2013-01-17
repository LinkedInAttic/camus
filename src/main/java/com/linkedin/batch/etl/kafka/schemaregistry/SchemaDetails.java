package com.linkedin.batch.etl.kafka.schemaregistry;

import org.apache.avro.Schema;

public class SchemaDetails
{
	private String id;
	private Schema schema;
	private String topic;

	public SchemaDetails(String id, Schema schema, String topic) {
		this.id = id;
		this.schema = schema;
		this.topic = topic;
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