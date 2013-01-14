package com.linkedin.batch.etl.kafka.schemaregistry;

import org.apache.avro.Schema;

public interface SchemaResolver {

    public Schema resolve(String topicName);
    
    
}
