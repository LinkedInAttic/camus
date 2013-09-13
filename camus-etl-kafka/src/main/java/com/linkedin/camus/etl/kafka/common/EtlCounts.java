package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Map.Entry;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.joda.time.DateTime;

import com.linkedin.camus.coders.MessageEncoder;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.events.records.EventHeader;
import com.linkedin.camus.events.records.Guid;
import com.linkedin.camus.events.records.TrackingMonitoringEvent;

@JsonIgnoreProperties({"trackingCount", "lastKey", "eventCount", "RANDOM"})
public class EtlCounts {

	private static Logger log = Logger.getLogger(EtlCounts.class);
	private static final String TOPIC = "topic";
	private static final String SERVER = "server";
	private static final String SERVICE = "service";
	private static final String GRANULARITY = "granularity";
	private static final String COUNTS = "counts";
	private static final String START_TIME = "startTime";
	private static final String END_TIME = "endTime";
	private static final String FIRST_TIMESTAMP = "firstTimestamp";
	private static final String LAST_TIMESTAMP = "lastTimestamp";
	private static final String ERROR_COUNT = "errorCount";
	private static final String START = "start";
	private static final String COUNT = "count";
	
	private String topic;
	private long startTime;
	private long granularity;
	private long errorCount;
	private long endTime;
	private long lastTimestamp;
	private long firstTimestamp;
	private HashMap<String, Source> counts;
		
	//private final transient HashMap<NewSource, Long> trackingCount = new HashMap<NewSource, Long>();
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
	
	
	
	public void writeCountsToHDFS(ArrayList<Map<String,Object>> allCountObject, FileSystem fs, Path path) throws IOException {
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
	
	
	public void postTrackingCountToKafka(Configuration conf , String tier, String brokerList) {
		MessageEncoder<IndexedRecord, byte[]> encoder;
		try {
			encoder = (MessageEncoder<IndexedRecord, byte[]>) Class.forName(
					conf.get(CamusJob.CAMUS_MESSAGE_ENCODER_CLASS))
					.newInstance();

			Properties props = new Properties();
			for (Entry<String, String> entry : conf) {
				props.put(entry.getKey(), entry.getValue());
			}

			encoder.init(props, "TrackingMonitoringEvent");
		} catch (Exception e1) {
			throw new RuntimeException(e1);
		}

		ArrayList<byte[]> monitorSet = new ArrayList<byte[]>();
		long timestamp = new DateTime().getMillis();
		int counts = 0;
		for (Map.Entry<String, Source> singleCount : this.getCounts().entrySet()) {
			Source countEntry = singleCount.getValue();
			long partition = countEntry.getStart();
			String serverName = countEntry.getServer();
			String serviceName = countEntry.getService();
			long count = countEntry.getCount();
			EventHeader header = new EventHeader();
			
			Guid guid = new Guid();
			byte[] bytes = new byte[16];
			RANDOM.nextBytes(bytes);
			guid.bytes(bytes);

			header.memberId = -1;
			header.server = serverName;
			header.service = serviceName;
			header.time = timestamp;
			header.guid = guid;
			
			TrackingMonitoringEvent trackingRecord = new TrackingMonitoringEvent();
			trackingRecord.header = header;
			trackingRecord.beginTimestamp = partition;
			trackingRecord.endTimestamp = partition + granularity;
			trackingRecord.count = count;
			trackingRecord.tier = tier;
			trackingRecord.eventType = topic;

			byte[] message = encoder.toBytes(trackingRecord);
			monitorSet.add(message);

			if (monitorSet.size() >= 2000) {
				counts += monitorSet.size();
				produceCount(brokerList, monitorSet);
				monitorSet.clear();
			}
		}

		if (monitorSet.size() > 0) {
			counts += monitorSet.size();
			produceCount(brokerList, monitorSet);
		}

		log.info(topic + " sent " + counts + " counts");
	}

	private void produceCount(String brokerList, ArrayList<byte[]> monitorSet) {
		// Shuffle the broker

		Properties props = new Properties();
		props.put("metadata.broker.list", brokerList);
		props.put("producer.type", "async");
		props.put("request.required.acks", "1");
		props.put("request.timeout.ms", "30000");
		log.debug("Broker list: " + brokerList);
		Producer producer = new Producer(new ProducerConfig(props));
		try {
			for (byte[] message : monitorSet) {
				KeyedMessage keyedMessage = new KeyedMessage(
						"TrackingMonitoringEvent", message);
				producer.send(keyedMessage);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(topic + " issue sending tracking to "
                    + brokerList.toString());
		} finally {
			if (producer != null) {
				producer.close();
			}
		}

	}
	
}
