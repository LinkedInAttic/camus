package com.linkedin.camus.etl.kafka.coders;

import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;

public class LatestSchemaKafkaAvroMessageDecoder extends KafkaAvroMessageDecoder
{

	public LatestSchemaKafkaAvroMessageDecoder(Configuration conf, String topic) throws KafkaMessageDecoderException
	{
		super(conf, topic);
	}

	@Override
	public Record toRecord(Message message)
	{
		try
		{
			GenericDatumReader<Record> reader = new GenericDatumReader<Record>();
			
			Schema schema = resolver.getTargetSchema();
			
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