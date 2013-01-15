package com.edate.data.events;

import java.io.IOException;

import kafka.message.Message;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData.Record;
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
		this.decoderFactory = new DecoderFactory();
	}

	@Override
	public Record toRecord(String topicName, Message message, BinaryDecoder decoderReuse)
	{
		EventTypeAvro type = EventDataAvro.EventTypeAvro.valueOf(topicName);

		try
		{
			DatumReader<Record> reader = new GenericDatumReader<Record>(type.getSchema());

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
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public <T extends SpecificRecord> T toSpecificRecord(String topicName, Message message, BinaryDecoder decoderReuse)
	{
		EventTypeAvro type = EventDataAvro.EventTypeAvro.valueOf(topicName);

		try
		{
			DatumReader<T> reader = new GenericDatumReader<T>(type.getSchema());

			return reader.read(null, decoderFactory.jsonDecoder(type.getSchema(), new String(message.payload().array())));
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}
}