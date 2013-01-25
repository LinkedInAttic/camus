package com.linkedin.camus.schemaregistry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

public class CachedSchemaResolver implements SchemaResolver {

	private final ConcurrentMap<String, Schema> cache = new ConcurrentHashMap<String, Schema>();
	private final SchemaResolver resolver;
	private final String topicName;
	private static final Logger logger = Logger
			.getLogger(CachedSchemaResolver.class);

	public CachedSchemaResolver(String topicName,
			SchemaResolver resolver) {
		this.topicName = topicName;
		this.resolver = resolver;
	}

	@Override
	public Schema getTargetSchema() {
		return resolver.getTargetSchema();
	}

	@Override
	public Schema resolve(String id) {
		Schema schema = cache.get(id);
		if (schema == null) {
			try {
				logger.debug("Fetching schema using ID=" + topicName + "-" + id);
				schema = resolver.resolve(id);
				cache.putIfAbsent(id, schema);
			} catch (Exception e) {
				throw new RuntimeException(
						"Problem in fetching schema for ID = " + topicName + "-" + id, e);
			}
		}
		return schema;
	}

}
