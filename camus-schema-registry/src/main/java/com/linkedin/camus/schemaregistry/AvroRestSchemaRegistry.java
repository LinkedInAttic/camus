package com.linkedin.camus.schemaregistry;

import org.apache.avro.Schema;
import org.apache.avro.repo.SchemaEntry;
import org.apache.avro.repo.SchemaValidationException;
import org.apache.avro.repo.Subject;
import org.apache.avro.repo.client.RESTRepositoryClient;
import org.apache.hadoop.conf.Configuration;

public class AvroRestSchemaRegistry implements SchemaRegistry<Schema> {
	private RESTRepositoryClient client;
	public static final String ETL_SCHEMA_REGISTRY_URL = "etl.schema.registry.url";

	public AvroRestSchemaRegistry(Configuration conf) {
		client = new RESTRepositoryClient(conf.get(ETL_SCHEMA_REGISTRY_URL));
	}

	@Override
	public String register(String topic, String schema) {
		Subject subject = client.lookup(topic);

		if (subject == null) {
			subject = client.register(topic, "org.apache.avro.repo.Validator");
		}

		try {
			return subject.register(schema).getId();
		} catch (SchemaValidationException e) {
			throw new SchemaRegistryException(e);
		}
	}

	@Override
	public Schema getSchemaByID(String topic, String id) {
		try {
			SchemaEntry entry = client.lookup(topic).lookupById(id);
			if (entry == null)
				throw new SchemaNotFoundException("Schema not found for "
						+ topic + " " + id);

			return Schema.parse(entry.getSchema());
		} catch (Exception e) {
			throw new SchemaRegistryException(e);
		}
	}

	@Override
	public SchemaDetails<Schema> getLatestSchemaByTopic(String topicName) {
		try {
			SchemaEntry entry = client.lookup(topicName).latest();

			if (entry == null)
				throw new SchemaNotFoundException("Schema not found for "
						+ topicName);

			return new SchemaDetails<Schema>(topicName, entry.getId(),
					Schema.parse(entry.getSchema()));
		} catch (Exception e) {
			throw new SchemaRegistryException(e);
		}
	}
}
