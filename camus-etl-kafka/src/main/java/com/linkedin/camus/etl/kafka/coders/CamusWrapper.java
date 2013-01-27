package com.linkedin.camus.etl.kafka.coders;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.IndexedRecord;

public class CamusWrapper {
	protected Record record;
	protected long timestamp;
	protected String server;
	protected String service;

	public CamusWrapper(Record record) {
		this.record = record;
	}

	public IndexedRecord getRecord() {
		return record;
	}

	public long getTimeStamp() {
		Record header = (Record) record.get("header");

		if (header != null && header.get("time") != null) {
			return (Long) header.get("time");
		} else if (record.get("timestamp") != null) {
			return (Long) record.get("timestamp");
		} else {
			return System.currentTimeMillis();
		}
	}

	public String getServer() {
		return ((Record) record.get("header")).get("server").toString();
	}

	public String getService() {
		return ((Record) record.get("header")).get("service").toString();
	}
}
