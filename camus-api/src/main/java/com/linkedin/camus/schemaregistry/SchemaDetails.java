package com.linkedin.camus.schemaregistry;

public class SchemaDetails<S> {
	private String topic;
	private String id;
	private S schema;

	public SchemaDetails(String topic, String id, S schema) {
		this.topic = topic;
		this.id = id;
		this.schema = schema;
	}

	public SchemaDetails(String topic, String id) {
		this.topic = topic;
		this.id = id;
	}

	/**
	 * Get the schema
	 */
	public S getSchema() {
		return schema;
	}

	/**
	 * Get the topic
	 */
	public String getTopic() {
		return topic;
	}

	public String getId() {
		return id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((schema == null) ? 0 : schema.hashCode());
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SchemaDetails other = (SchemaDetails) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (schema == null) {
			if (other.schema != null)
				return false;
		} else if (!schema.equals(other.schema))
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
		return "SchemaDetails [topic=" + topic + ", id=" + id + ", schema="
				+ schema + "]";
	}
}