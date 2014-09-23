package com.linkedin.camus.etl.kafka.common;

import java.util.HashMap;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties({ "trackingCount", "lastKey", "eventCount", "RANDOM" })
public class EtlCounts {
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

	public EtlCounts() {
	}

	public EtlCounts(String topic, long granularity, long currentTime) {
		this.topic = topic;
		this.granularity = granularity;
		this.startTime = currentTime;
		this.counts = new HashMap<String, Source>();
	}

	public EtlCounts(String topic, long granularity) {
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
		if (counts.containsKey(source.toString())) {
			Source countSource = counts.get(source.toString());
			countSource.setCount(countSource.getCount() + 1);
			counts.put(countSource.toString(), countSource);
		} else {
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
}
