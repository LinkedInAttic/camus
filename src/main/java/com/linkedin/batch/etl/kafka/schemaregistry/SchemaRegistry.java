package com.linkedin.batch.etl.kafka.schemaregistry;

public interface SchemaRegistry {
	
   
    public SchemaRegistry getInstance();
    
	public String register(String schema) throws SchemaRegistryException;
	
	public String lookUp(String schema);
	
	public SchemaDetails lookUpLatest(String topic);
	
	public String getSchemaByID(String Id) throws SchemaNotFoundException;
	
	public String getLatestSchemaByName(String topicName);
		

}
