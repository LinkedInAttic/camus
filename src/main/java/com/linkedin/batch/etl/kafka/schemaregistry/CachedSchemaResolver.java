package com.linkedin.batch.etl.kafka.schemaregistry;

/**
 * Mock implementation of a CachedSchemaResolver as used by the KafkaAvroMessageDecoder
 */
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.linkedin.batch.etl.kafka.mapred.EtlInputFormat;

public class CachedSchemaResolver implements SchemaResolver {

    SchemaRegistry registry;
    
    public CachedSchemaResolver(String topicName){
        /**
         * Look into the schema registry and get the schema information
         */
    }
    
    public CachedSchemaResolver(TaskAttemptContext context, String topicName){
        try {
            registry =(SchemaRegistry) Class.forName(EtlInputFormat.getSchemaRegistryType(context)).newInstance();
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

    public String resolve(String topicName) {
        // TODO Auto-generated method stub
        return null;
    }
    
}
