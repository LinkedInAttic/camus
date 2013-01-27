package com.linkedin.camus.example;

import org.apache.avro.Schema;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.linkedin.camus.schemaregistry.SchemaDetails;
import com.linkedin.camus.schemaregistry.SchemaNotFoundException;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistryException;

public class ExampleEnumBasedAvroSchemaRegistry implements SchemaRegistry<Schema>
{
	@Override
	public String register(String topic, String schema)
	{
		throw new SchemaRegistryException(new NotImplementedException());
	}

	@Override
	public Schema getSchemaByID(String topicName, String id)
	{
		return getSchema(topicName);
	}

	@Override
	public SchemaDetails<Schema> getLatestSchemaByTopic(String topicName)
	{
			return new SchemaDetails<Schema>(topicName, null, getSchema(topicName));
	}
	
	private Schema getSchema(String topicName)
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
