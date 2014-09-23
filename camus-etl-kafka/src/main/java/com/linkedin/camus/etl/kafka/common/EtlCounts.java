package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Map.Entry;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import com.linkedin.camus.coders.MessageEncoder;
import com.linkedin.camus.etl.kafka.CamusJob;

@JsonIgnoreProperties({"trackingCount", "lastKey", "eventCount", "RANDOM"})
public class EtlCounts {

	private static Logger log = Logger.getLogger(EtlCounts.class);
	private static final String TOPIC = "topic";
	private static final String GRANULARITY = "granularity";
	private static final String COUNTS = "counts";
	private static final String START_TIME = "startTime";
	private static final String END_TIME = "endTime";
	private static final String FIRST_TIMESTAMP = "firstTimestamp";
	private static final String LAST_TIMESTAMP = "lastTimestamp";
	private static final String ERROR_COUNT = "errorCount";
	private static final String MONITORING_EVENT_CLASS = "monitoring.event.class";
	
	private String topic;
	private long startTime;
	private long granularity;
	private long errorCount;
	private long endTime;
	private long lastTimestamp;
	private long firstTimestamp;
	private HashMap<String, Source> counts;
		
	private transient EtlKey lastKey;
	private transient int eventCount = 0;
	private transient static final Random RANDOM = new Random();
	
	public EtlCounts()
	{}
	
	public EtlCounts(String topic, long granularity, long currentTime)
	{
		this.topic = topic;
		this.granularity = granularity;
		this.startTime = currentTime;
		this.counts = new HashMap<String, Source>();
	}
	
	public EtlCounts(String topic, long granularity)
	{
		this(topic, granularity, System.currentTimeMillis());
	}
	
	
	public HashMap<String, Source> getCounts() {
		return counts;
	}
	
	public long getEndTime() {
		return endTime;
	}
	
	public long getErrorCount() {
		return errorCount;
	}
	
	public long getFirstTimestamp() {
		return firstTimestamp;
	}
	
	public long getGranularity() {
		return granularity;
	}
	
	public long getLastTimestamp() {
		return lastTimestamp;
	}
	
	public long getStartTime() {
		return startTime;
	}
	
	public String getTopic() {
		return topic;
	}
	

	public void setCounts(HashMap<String, Source> counts) {
		this.counts = counts;
	}
	
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
	
	public void setErrorCount(long errorCount) {
		this.errorCount = errorCount;
	}
	
	public void setFirstTimestamp(long firstTimestamp) {
		this.firstTimestamp = firstTimestamp;
	}
	
	public void setGranularity(long granularity) {
		this.granularity = granularity;
	}
	
	public void setLastTimestamp(long lastTimestamp) {
		this.lastTimestamp = lastTimestamp;
	}
	
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	
	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	public int getEventCount() {
		return eventCount;
	}
	
	public EtlKey getLastKey() {
		return lastKey;
	}
	
	public void setEventCount(int eventCount) {
		this.eventCount = eventCount;
	}
	
	public void setLastKey(EtlKey lastKey) {
		this.lastKey = lastKey;
	}
	
	public void incrementMonitorCount(EtlKey key) {
		long monitorPartition = DateUtils.getPartition(granularity,
				key.getTime());
		Source source = new Source(key.getServer(), key.getService(),
				monitorPartition);
		if(counts.containsKey(source.toString()))
		{
			Source countSource = counts.get(source.toString());
			countSource.setCount(countSource.getCount() + 1);
			counts.put(countSource.toString(), countSource);
		}
		else
		{
			source.setCount(1);
			counts.put(source.toString(), source);
		}

		if (key.getTime() > lastTimestamp) {
			lastTimestamp = key.getTime();
		}

		if (key.getTime() < firstTimestamp) {
			firstTimestamp = key.getTime();
		}

		lastKey = new EtlKey(key);
		eventCount++;
	}
	
	
	
	public void writeCountsToMap(ArrayList<Map<String,Object>> allCountObject, FileSystem fs, Path path) throws IOException {
		Map<String, Object> countFile = new HashMap<String, Object>();
		countFile.put(TOPIC, topic);
		countFile.put(GRANULARITY, granularity);
		countFile.put(COUNTS, counts);
		countFile.put(START_TIME, startTime);
		countFile.put(END_TIME, endTime);
		countFile.put(FIRST_TIMESTAMP, firstTimestamp);
		countFile.put(LAST_TIMESTAMP, lastTimestamp);
		countFile.put(ERROR_COUNT, errorCount);	
		allCountObject.add(countFile);
	}
}
