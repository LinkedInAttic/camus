package com.edate.data.events;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.IndexedRecord;

import com.linkedin.camus.etl.kafka.coders.CamusWrapper;

public class Mate1CamusWrapper extends CamusWrapper {
	public Mate1CamusWrapper(Record record) {
		super(record);
	}

	public IndexedRecord getRecord() {
		return record;
	}

	public long getTimeStamp() {
		if (record.get("logged_time") != null) {
			// Chat events (in microseconds)
			return (Long) record.get("logged_time") / 1000;
		} else {
			return System.currentTimeMillis();
		}
	}

	
	public String getServer() {
		return "Server unavailable";
	}

	public String getService() {
		return "Service unavailable";
	}

}
