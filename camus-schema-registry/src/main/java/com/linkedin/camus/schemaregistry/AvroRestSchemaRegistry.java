package com.linkedin.camus.schemaregistry;

import org.apache.avro.repo.SchemaEntry;
import org.apache.avro.repo.SchemaValidationException;
import org.apache.avro.repo.Subject;
import org.apache.avro.repo.client.RESTRepositoryClient;
import org.apache.hadoop.conf.Configuration;

import org.apache.avro.Schema;

import com.sun.xml.internal.xsom.impl.SchemaImpl;

public class AvroRestSchemaRegistry extends SchemaRegistry {
	private RESTRepositoryClient client;
	public static final String ETL_SCHEMA_REGISTRY_URL = "etl.schema.registry.url";
	
	public AvroRestSchemaRegistry(Configuration conf) {
		super(conf);
		
		client = new RESTRepositoryClient(conf.get(ETL_SCHEMA_REGISTRY_URL));
	}

	@Override
	public String register(String topic, String schema)
			throws SchemaRegistryException {
		Subject subject = client.lookup(topic);
		
		if (subject == null){
			subject = client.register(topic, "org.apache.avro.repo.Validator");
		}
		
		try {
			return subject.register(schema).getId();
		} catch (SchemaValidationException e) {
			throw new SchemaRegistryException(e);
		}
	}

	@Override
	public Schema getSchemaByID(String topic, String id)
			throws SchemaRegistryException, SchemaNotFoundException {
		try {
			SchemaEntry entry = client.lookup(topic).lookupById(id);
			if (entry == null)
				throw new SchemaNotFoundException("Schema not found for " + topic + " " + id);
			
			Schema schema = Schema.parse(entry.getSchema());
			return schema;
			
		} catch (Exception e) {
			throw new SchemaRegistryException(e);
		}
	}

	@Override
	public SchemaDetails getLatestSchemaByTopic(String topicName)
			throws SchemaRegistryException, SchemaNotFoundException {
		try {
			SchemaEntry entry = client.lookup(topicName).latest();
			
			if (entry == null)
				throw new SchemaNotFoundException("Schema not found for " + topicName);
			
			return new SchemaDetails(topicName, entry.getId(), Schema.parse(entry.getSchema()));
			
		} catch (Exception e) {
			throw new SchemaRegistryException(e);
		}
	}
}
