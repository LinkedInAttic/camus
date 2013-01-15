package com.linkedin.batch.etl.kafka.coders;

import kafka.message.Message;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public abstract class KafkaMessageEncoder implements Configurable {
	Configuration conf;
	
	public KafkaMessageEncoder(Configuration conf) {
		this.conf = conf;
	}
	
	public abstract Message toMessage(IndexedRecord record);
	
	@Override
	public Configuration getConf() {
		return conf;
	}
	
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

}
