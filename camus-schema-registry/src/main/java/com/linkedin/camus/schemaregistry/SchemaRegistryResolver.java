package com.linkedin.camus.schemaregistry;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

public class SchemaRegistryResolver implements SchemaResolver {
	private final SchemaRegistry client;
	private static final Logger logger = Logger
			.getLogger(CachedSchemaResolver.class);
	private String topicName;
	private Schema targetSchema;

	public SchemaRegistryResolver(SchemaRegistry client, String topicName) {
		this.client = client;
		this.topicName = topicName;
		try {
			SchemaDetails details = client.getLatestSchemaByTopic(topicName);
			this.targetSchema = details.getSchema();
		} catch (Exception e) {
			RuntimeException e1 = new RuntimeException(e);
			e1.setStackTrace(e.getStackTrace());
			throw e1;
		}
	}

	@Override
	public Schema getTargetSchema() {
		return targetSchema;
	}

	@Override
	public Schema resolve(String id) {
		try {
			logger.debug("Fetching schema using ID=" + id);
			return client.getSchemaByID(topicName, id);
		} catch (Exception e) {
			throw new RuntimeException(
					"Problem in fetching schema for ID = " + id.toString(), e);
		}
	}

}
