package com.linkedin.batch.etl.kafka.coders;

import kafka.message.Message;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public abstract class KafkaMessageDecoder implements Configurable {
	private Configuration conf;

	public KafkaMessageDecoder(Configuration conf) {
		this.conf = conf;
	}

	public Record toRecord(String topicName, Message message) {
		return toRecord(topicName, message, null);
	}

	public <T extends SpecificRecord> T toSpecificRecord(String topicName, Message message) {
		T specificRecord = this.<T> toSpecificRecord(topicName, message, null);
		return specificRecord;
	}

	public abstract Record toRecord(String topicName, Message message, BinaryDecoder decoderReuse);

	public abstract <T extends SpecificRecord> T toSpecificRecord(String topicName, 
			Message message, BinaryDecoder decoderReuse);

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

}
