package com.linkedin.camus.schemaregistry;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class CachedSchemaRegistry<S> implements SchemaRegistry<S> {
	private final SchemaRegistry<S> registry;
	private final ConcurrentHashMap<CachedSchemaTuple, S> cachedById;
	private final ConcurrentHashMap<String, S> cachedLatest;
	
	public void init(Properties props) {}

	public CachedSchemaRegistry(SchemaRegistry<S> registry) {
		this.registry = registry;
		this.cachedById = new ConcurrentHashMap<CachedSchemaTuple, S>();
		this.cachedLatest = new ConcurrentHashMap<String, S>();
	}

	public String register(String topic, S schema) {
		return registry.register(topic, schema);
	}

	public S getSchemaByID(String topic, String id) {
		CachedSchemaTuple cacheKey = new CachedSchemaTuple(topic, id);
		S schema = cachedById.get(cacheKey);
		if (schema == null) {
			schema = registry.getSchemaByID(topic, id);
			cachedById.putIfAbsent(cacheKey, schema);
		}
		return schema;
	}

	public SchemaDetails<S> getLatestSchemaByTopic(String topicName) {
		S schema = cachedLatest.get(topicName);
		if (schema == null) {
			schema = registry.getLatestSchemaByTopic(topicName).getSchema();
			cachedLatest.putIfAbsent(topicName, schema);
		}
		return registry.getLatestSchemaByTopic(topicName);
	}

	public static class CachedSchemaTuple {
		private final String topic;
		private final String id;

		public CachedSchemaTuple(String topic, String id) {
			this.topic = topic;
			this.id = id;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result + ((topic == null) ? 0 : topic.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			CachedSchemaTuple other = (CachedSchemaTuple) obj;
			if (id == null) {
				if (other.id != null)
					return false;
			} else if (!id.equals(other.id))
				return false;
			if (topic == null) {
				if (other.topic != null)
					return false;
			} else if (!topic.equals(other.topic))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "CachedSchemaTuple [topic=" + topic + ", id=" + id + "]";
		}
	}
}
