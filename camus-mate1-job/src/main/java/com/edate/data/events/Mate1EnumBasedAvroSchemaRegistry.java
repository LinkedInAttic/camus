package com.edate.data.events;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.linkedin.camus.schemaregistry.SchemaDetails;
import com.linkedin.camus.schemaregistry.SchemaNotFoundException;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistryException;

public class Mate1EnumBasedAvroSchemaRegistry implements SchemaRegistry<Schema>
{
	public Mate1EnumBasedAvroSchemaRegistry(Configuration conf)
	{
		super();
	}

	@Override
	public String register(String topic, Schema schema)
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
	public SchemaDetails<Schema> getLatestSchemaByTopic(String topicName)
			throws SchemaRegistryException, SchemaNotFoundException
	{
			return new SchemaDetails<Schema>(topicName, null, getSchema(topicName));
	}
	
	private Schema getSchema(String topicName) throws SchemaRegistryException, SchemaNotFoundException
	{
		try
		{
			return EventDataAvro.EventTypeAvro.valueOf(topicName).getSchema();
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
