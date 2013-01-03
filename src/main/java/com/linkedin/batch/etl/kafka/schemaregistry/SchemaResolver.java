package com.linkedin.batch.etl.kafka.schemaregistry;

public interface SchemaResolver {

    public String resolve(String topicName);
    
    
}
