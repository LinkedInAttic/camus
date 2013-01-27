package com.linkedin.camus.schemaregistry;

public interface SchemaRegistry<S> {
	public String register(String topic, String schema);

	public S getSchemaByID(String topic, String id);

	public SchemaDetails<S> getLatestSchemaByTopic(String topicName);
}