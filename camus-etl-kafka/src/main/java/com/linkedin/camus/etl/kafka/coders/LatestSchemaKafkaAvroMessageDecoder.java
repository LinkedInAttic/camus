package com.linkedin.camus.etl.kafka.coders;

import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;

public class LatestSchemaKafkaAvroMessageDecoder extends KafkaAvroMessageDecoder
{

	public LatestSchemaKafkaAvroMessageDecoder(Configuration conf, String topic)
	{
		super(conf, topic);
	}

	@Override
	public CamusWrapper decode(Message message)
	{
		try
		{
			GenericDatumReader<Record> reader = new GenericDatumReader<Record>();
			
			Schema schema = super.registry.getLatestSchemaByTopic(super.topicName).getSchema();
			
			reader.setSchema(schema);
			
			return new CamusWrapper(reader.read(
					null, 
					decoderFactory.jsonDecoder(
							schema, 
							new String(
									message.payload().array(), 
									Message.payloadOffset(message.magic()),
									message.payloadSize()
							)
					)
			));
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
}