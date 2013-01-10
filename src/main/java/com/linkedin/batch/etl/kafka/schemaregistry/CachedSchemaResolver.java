package com.linkedin.batch.etl.kafka.schemaregistry;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import com.linkedin.batch.etl.kafka.EtlJob;
import com.linkedin.batch.etl.kafka.coders.Utils;


/**
 * Resolve schema from schema registry service. It maintains a local cache for
 * previously accessed schema.
 * 
 * @author lguo
 * 
 */
public class CachedSchemaResolver {

	// Schema registry client
	private SchemaRegistry client = null;
	// Local cache so we don't constantly query the registry.
	private Map<SchemaDetails, Schema> cache = new HashMap<SchemaDetails, Schema>();
	// target schema for the event
	private Schema readerSchema = null;
	
	private String topic;

	/**
	 * Creates a registry resolver that will go to the schema registry and sets
	 * the target registry to the latest found by the topic Name
	 * 
	 * @param url
	 * @param topicName
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws SchemaNotFoundException 
	 * @throws UnsupportedOperationException 
	 * @throws NoSuchMethodException 
	 * @throws SecurityException 
	 * @throws InvocationTargetException 
	 * @throws IllegalArgumentException 
	 */
	public CachedSchemaResolver(String topicName, Configuration conf) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, UnsupportedOperationException, SchemaNotFoundException, SecurityException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException {
		Constructor<?> constructor = Class.forName(conf.get(EtlJob.SCHEMA_REGISTRY_TYPE)).getConstructor(Configuration.class);
		
		client = (SchemaRegistry) constructor.newInstance(conf);
		SchemaDetails targetSchema = client.getLatestSchemaByTopic(topicName);
		readerSchema = Schema.parse(targetSchema.getSchema());
		topic = topicName;
	}

	public Schema resolve(String id) {
		SchemaDetails details = new SchemaDetails(id, null, topic);
		Schema schema = cache.get(details);
		if (schema != null)
			return schema;
		try {
			String s = client.getSchemaByID(topic, id.toString());
			schema = Schema.parse(s);
			cache.put(details, schema);
		} catch (Exception e) {
			throw new RuntimeException("Error while resolving schema id:" + id + " msg:" + e.getMessage());
		}
		return schema;
	}
}
