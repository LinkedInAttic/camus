package com.linkedin.batch.etl.kafka.schemaregistry;

public interface SchemaRegistry {
    
	public String register(String topic, String schema) throws SchemaRegistryException;
	
	public String register(String schema) throws SchemaRegistryException, UnsupportedOperationException;
	
	public String getSchemaByID(String topic, String id) throws SchemaNotFoundException;
	
	public String getSchemaByID(String id) throws SchemaNotFoundException;
	
	public SchemaDetails getLatestSchemaByTopic(String topicName) throws SchemaNotFoundException, UnsupportedOperationException;
		

}
