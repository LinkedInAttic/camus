package com.linkedin.batch.etl.kafka.schemaregistry;

import org.apache.avro.repository.IsAvroValidator;
import org.apache.avro.repository.JdbcSchemaRepository;
import org.apache.avro.repository.Md5SchemaIdGenerator;
import org.apache.avro.repository.SchemaAndId;
import org.apache.avro.repository.SchemaChangeValidator;
import org.apache.avro.repository.SchemaIdGenerator;
import org.apache.avro.repository.SchemaRepository;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import com.linkedin.batch.etl.kafka.CamusJob;
import com.linkedin.batch.etl.kafka.mapred.EtlInputFormat;

/**
 * Wrapper around the Schema Repository Interface
 * 
 * @author
 * 
 */
public class AvroJdbcSchemaRegistryClient implements SchemaRegistry
{

	private JdbcSchemaRepository client;
	
  public AvroJdbcSchemaRegistryClient(Configuration conf) throws InstantiationException, IllegalAccessException, ClassNotFoundException
  {
	  SchemaChangeValidator validator = new IsAvroValidator();
      SchemaIdGenerator idGenerator = new Md5SchemaIdGenerator();
    
      client =
          new JdbcSchemaRepository(validator,
                                   idGenerator,
                                   conf.get("schema.registry.user"),
                                   conf.get("schema.registry.password"),
                                   conf.get(CamusJob.ETL_SCHEMA_REGISTRY_URL),
                                   conf.get(CamusJob.JDBC_SCHEMA_REGISTRY_DRIVER),
                                   conf.getInt(CamusJob.JDBC_SCHEMA_REGISTRY_POOL_SIZE, 8));
  }

@Override
public String register(String topic, String schema)
		throws SchemaRegistryException {
	return client.register(topic, schema);
}

@Override
public String getSchemaByID(String topic, String id)
		throws SchemaNotFoundException {
	String schema = client.lookup(topic, id);
	
	if (schema != null)
		return schema;
	else
		throw new SchemaNotFoundException("Schema not found for " + topic + " " + id);
}

@Override
public SchemaDetails getLatestSchemaByTopic(String topicName) throws SchemaNotFoundException {
	SchemaAndId schema = client.lookupLatest(topicName);
	
	if (schema != null)
		return new SchemaDetails(schema.getId(), schema.getSchema(), topicName);
	else
		throw new SchemaNotFoundException("Schema not found for " + topicName);
	
}

}
