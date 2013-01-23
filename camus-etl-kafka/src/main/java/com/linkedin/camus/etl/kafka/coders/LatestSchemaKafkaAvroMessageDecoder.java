package com.linkedin.camus.etl.kafka.coders;

import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;

public class LatestSchemaKafkaAvroMessageDecoder extends KafkaAvroMessageDecoder
{

	private final DecoderFactory decoderFactory;

	public LatestSchemaKafkaAvroMessageDecoder(Configuration conf, String topic) throws KafkaMessageDecoderException
	{
		super(conf, topic);
		this.decoderFactory = DecoderFactory.get();
	}

	@Override
	public Record toRecord(Message message)
	{
		try
		{
			GenericDatumReader<Record> reader = new GenericDatumReader<Record>();
			
			Schema schema = schemaRegistry.getLatestSchemaByTopic(topicName).getSchema();
			
			reader.setSchema(schema);
			
			return reader.read(
					null, 
					decoderFactory.jsonDecoder(
							schema, 
							new String(
									message.payload().array(), 
									Message.payloadOffset(message.magic()),
									message.payloadSize()
							)
					)
			);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
}