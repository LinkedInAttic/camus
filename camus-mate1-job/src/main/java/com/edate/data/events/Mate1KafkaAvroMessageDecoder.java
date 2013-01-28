package com.edate.data.events;

import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;

import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageDecoder;

public class Mate1KafkaAvroMessageDecoder extends KafkaAvroMessageDecoder
{

	public Mate1KafkaAvroMessageDecoder(Configuration conf, String topic)
	{
		super(conf, topic);
	}

	@Override
	public Mate1CamusWrapper decode(Message message)
	{
		try
		{
			GenericDatumReader<Record> reader = new GenericDatumReader<Record>();
			
			Schema schema = super.registry.getLatestSchemaByTopic(super.topicName).getSchema();
			
			reader.setSchema(schema);
			
			return new Mate1CamusWrapper(reader.read(
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