package com.linkedin.camus.etl.kafka.coders;

import java.lang.reflect.Constructor;

import kafka.message.Message;

import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.linkedin.camus.schemaregistry.SchemaNotFoundException;
import com.linkedin.camus.schemaregistry.SchemaRegistry;

public abstract class KafkaAvroMessageDecoder implements Configurable {
	
	private static final String KAFKA_MESSAGE_DECODER_SCHEMA_REGISTRY_CLASS = "kafka.message.decoder.schema.registry.class";

	protected Configuration conf;
	protected String topicName;
	protected static SchemaRegistry schemaRegistry = null;

	public KafkaAvroMessageDecoder(Configuration conf, String topicName) throws KafkaMessageDecoderException {
		this.conf = conf;
		this.topicName = topicName;
		
		try
		{
			if (schemaRegistry == null)
			{
				Constructor<?> constructor = Class.forName(conf.get(KAFKA_MESSAGE_DECODER_SCHEMA_REGISTRY_CLASS)).getConstructor(Configuration.class);
		  		schemaRegistry = (SchemaRegistry) constructor.newInstance(conf);
			}
			
			// This call ensures that the topicName requested has a Schema associated with it in the SchemaRegistry (and will throw an exception otherwise)
			schemaRegistry.getLatestSchemaByTopic(topicName);
		}
		catch (SchemaNotFoundException e)
		{
			throw new KafkaMessageDecoderException(e);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public abstract Record toRecord(Message message);

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

}
