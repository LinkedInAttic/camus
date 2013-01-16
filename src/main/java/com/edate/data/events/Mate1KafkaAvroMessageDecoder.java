package com.edate.data.events;

import java.io.IOException;

import kafka.message.Message;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configuration;

import com.linkedin.batch.etl.kafka.coders.KafkaMessageDecoder;

import com.edate.data.events.EventDataAvro.EventTypeAvro;

public class Mate1KafkaAvroMessageDecoder extends KafkaMessageDecoder
{

	private final DecoderFactory decoderFactory;

	public Mate1KafkaAvroMessageDecoder(Configuration conf)
	{
		super(conf);
		this.decoderFactory = DecoderFactory.get();
	}

	@Override
	public Record toRecord(String topicName, Message message, BinaryDecoder decoderReuse)
	{
		return getRecord(topicName, message, decoderReuse, new GenericDatumReader<Record>());
	}

	@Override
	public <T extends SpecificRecord> T toSpecificRecord(String topicName, Message message, BinaryDecoder decoderReuse)
	{
		return getRecord(topicName, message, decoderReuse, new GenericDatumReader<T>());
	}
	
	private <T extends IndexedRecord> T getRecord(String topicName, Message message, BinaryDecoder decoderReuse, DatumReader<T> reader)
	{
		try
		{
			EventTypeAvro type = EventDataAvro.EventTypeAvro.valueOf(topicName);
			
			reader.setSchema(type.getSchema());
			
			return reader.read(
					null, 
					decoderFactory.jsonDecoder(
							type.getSchema(), 
							new String(
									message.payload().array(), 
									Message.payloadOffset(message.magic()),
									message.payloadSize()
							)
					)
			);
		}
		catch (IllegalArgumentException e) // If the enum is not found
		{
			return null;
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

}