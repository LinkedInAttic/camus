package com.linkedin.batch.etl.kafka.schemaregistry;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

public abstract class SchemaRegistry {
	
	protected Configuration conf;
	
	public SchemaRegistry(Configuration conf)
	{
		this.conf = conf;
	}
	
	public abstract String register(String topic, String schema) throws SchemaRegistryException;

	public abstract Schema getSchemaByID(String topic, String id) throws SchemaRegistryException, SchemaNotFoundException;

	public abstract SchemaDetails getLatestSchemaByTopic(String topicName) throws SchemaRegistryException, SchemaNotFoundException;

}