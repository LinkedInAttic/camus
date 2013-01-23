package com.example;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.linkedin.batch.etl.kafka.schemaregistry.SchemaDetails;
import com.linkedin.batch.etl.kafka.schemaregistry.SchemaNotFoundException;
import com.linkedin.batch.etl.kafka.schemaregistry.SchemaRegistry;
import com.linkedin.batch.etl.kafka.schemaregistry.SchemaRegistryException;

public class ExampleEnumBasedAvroSchemaRegistry extends SchemaRegistry
{
	public ExampleEnumBasedAvroSchemaRegistry(Configuration conf)
	{
		super(conf);
	}

	@Override
	public String register(String topic, String schema)
			throws SchemaRegistryException
	{
		throw new SchemaRegistryException(new NotImplementedException());
	}

	@Override
	public Schema getSchemaByID(String topicName, String id)
			throws SchemaRegistryException, SchemaNotFoundException
	{
		return getSchema(topicName);
	}

	@Override
	public SchemaDetails getLatestSchemaByTopic(String topicName)
			throws SchemaRegistryException, SchemaNotFoundException
	{
			return new SchemaDetails(null, getSchema(topicName), topicName);
	}
	
	private Schema getSchema(String topicName) throws SchemaRegistryException, SchemaNotFoundException
	{
		try
		{
			return DummyEventDataAvro.EventTypeAvro.valueOf(topicName).getSchema();
		}
		catch (IllegalArgumentException e) // If the enum is not found
		{
			throw new SchemaNotFoundException("The topic '" + topicName + "' cannot be found!", e);
		}
		catch (Exception e)
		{
			throw new SchemaRegistryException(e);
		}
	}

}
