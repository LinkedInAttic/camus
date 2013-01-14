package com.linkedin.batch.etl.kafka.schemaregistry;

public interface SchemaRegistry {
    
	public String register(String topic, String schema) throws SchemaRegistryException;
	
	public String getSchemaByID(String topic, String id) throws SchemaRegistryException, SchemaNotFoundException;
	
	public SchemaDetails getLatestSchemaByTopic(String topicName) throws SchemaRegistryException, SchemaNotFoundException;
		

}
