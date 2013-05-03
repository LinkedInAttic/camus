package com.linkedin.camus.etl.kafka.common;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.producer.SyncProducerConfig;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;

import com.linkedin.camus.coders.MessageEncoder;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.events.records.EventHeader;
import com.linkedin.camus.events.records.Guid;
import com.linkedin.camus.events.records.TrackingMonitoringEvent;

public class EtlCounts {
	private static final Random RANDOM = new Random();
	private static final String TOPIC = "topic";
	private static final String SERVER = "server";
	private static final String SERVICE = "service";
	private static final String MONITOR_GRANULARITY = "granularity";
	private static final String COUNTS = "counts";
	private static final String START_TIME = "startTime";
	private static final String END_TIME = "endTime";
	private static final String FIRST_TIMESTAMP = "firstTimestamp";
	private static final String LAST_TIMESTAMP = "lastTimestamp";
	private static final String ERROR_COUNT = "errorCount";

	private static final String START = "start";
	private static final String END = "end";
	private static final String COUNT = "count";

	private String topic;
	private long startTime = -1;
	private long endTime = -1;
	private long firstTimestamp = Long.MAX_VALUE;
	private long lastTimestamp = 0;
	private long errorCount = 0;
	private long monitorGranularity;
	private EtlKey lastKey;
	private int eventCount = 0;

	private final Logger log = Logger.getLogger(getClass());
	
	private final HashMap<Source, Long> trackingCount = new HashMap<Source, Long>();

	// Used to decrease memory usage, we intern the string so it'll use the same
	// String instance
	private final HashMap<String, String> stringIntern = new HashMap<String, String>();
	private Configuration conf;

	public EtlCounts(Configuration conf, String topic, long monitorGranularity) {
		this.topic = topic;
		this.monitorGranularity = monitorGranularity;
		this.startTime = System.currentTimeMillis();
		this.conf = conf;
	}

	public void incrementMonitorCount(EtlKey key) {
		long monitorPartition = DateUtils.getPartition(monitorGranularity,
				key.getTime());
		Source source = new Source(key.getServer(), key.getService(),
				monitorPartition);

		Long count = trackingCount.get(source);
		if (count == null) {
			count = 0l;
		}

		count++;
		trackingCount.put(source, count);

		if (key.getTime() > lastTimestamp) {
			lastTimestamp = key.getTime();
		}

		if (key.getTime() < firstTimestamp) {
			firstTimestamp = key.getTime();
		}

		lastKey = new EtlKey(key);
		eventCount++;
	}

	public int loadStreamingCountsFromDir(Configuration conf, Path path)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		return loadStreamingCountsFromDir(fs, path, true);
	}

	public int loadStreamingTimestampsFromDir(Configuration conf, Path path)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		return loadStreamingCountsFromDir(fs, path, false);
	}

	/**
	 * 
	 * @param fs
	 * @param path
	 * @param loadCounts
	 *            Setting this to false will ignore reconstructing counts which
	 *            will save on memory when we only need timestamps
	 * @return
	 * @throws IOException
	 */
	public int loadStreamingCountsFromDir(FileSystem fs, Path path,
			boolean loadCounts) throws IOException {
		FileStatus[] statuses = fs.listStatus(path, new PrefixFilter(COUNTS));

		if (statuses.length == 0) {
			System.out.println("No old counts found!");
		}

		for (FileStatus status : statuses) {
			InputStream stream = new BufferedInputStream(fs.open(status
					.getPath()));
			loadStreamingCountsFromStream(stream, loadCounts);
		}

		return statuses.length;
	}

	public void loadStreamingCountsFromStream(InputStream stream,
			boolean loadCounts) throws IOException {
		JsonFactory jsonF = new JsonFactory();
		JsonParser jp = jsonF.createJsonParser(stream);

		if (jp.nextToken() != JsonToken.START_OBJECT) {
			throw new IOException("Expected data to start with an Object");
		}

		while (jp.nextToken() != JsonToken.END_OBJECT) {
			String fieldName = jp.getCurrentName();
			// Let's move to value
			jp.nextToken();
			// Skip all counts
			if (COUNTS.equals(fieldName)) {
				extractCounts(jp, loadCounts);
			} else if (LAST_TIMESTAMP.equals(fieldName)) {
				lastTimestamp = Math.max(lastTimestamp, jp.getLongValue());
			} else if (START_TIME.equals(fieldName)) {
				if (startTime == -1) {
					startTime = jp.getLongValue();
				} else {
					startTime = Math.min(jp.getLongValue(), startTime);
				}
			} else if (END_TIME.equals(fieldName)) {
				endTime = Math.max(jp.getLongValue(), endTime);
			} else if (TOPIC.equals(fieldName)) {
				topic = jp.getText();
			} else if (MONITOR_GRANULARITY.equals(fieldName)) {
				monitorGranularity = jp.getLongValue();
			} else if (FIRST_TIMESTAMP.equals(fieldName)) {
				if (firstTimestamp == -1) {
					firstTimestamp = jp.getLongValue();
				}
				firstTimestamp = Math.min(jp.getLongValue(), firstTimestamp);
			} else if (ERROR_COUNT.equals(fieldName)) {
				errorCount += jp.getLongValue();
			}
		}
	}

	private void extractCounts(JsonParser jp, boolean loadCounts)
			throws IOException {
		while (jp.nextToken() != JsonToken.END_ARRAY) {
			if (loadCounts) {
				long count = 0;
				long start = 0;
				String server = "esv4-tarotaz01";
				String service = "azkaban";

				while (jp.nextToken() != JsonToken.END_OBJECT) {
					jp.nextToken();
					String fieldName = jp.getCurrentName();
					// JsonToken token = jp.nextToken();
					// System.out.println("token: " + token);
					if (COUNT.equals(fieldName)) {
						count = jp.getLongValue();
					} else if (START.equals(fieldName)) {
						start = jp.getLongValue();
					} else if (SERVICE.equals(fieldName)) {
						service = jp.getText();
					} else if (SERVER.equals(fieldName)) {
						server = jp.getText();
					}
				}

				Source source = new Source(server, service, start);
				Long totalCount = trackingCount.get(source);
				if (totalCount == null) {
					totalCount = 0l;
				}
				totalCount += count;
				trackingCount.put(source, totalCount);
			}
		}
	}

	public long getStartTime() {
		return startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime() {
		endTime = System.currentTimeMillis();
	}

	public EtlKey getLastOffsetKey() {
		return lastKey;
	}

	public int getEventCount() {
		return eventCount;
	}

	public long getErrorCount() {
		return errorCount;
	}

	public long getFirstTimestamp() {
		return firstTimestamp;
	}

	public long getLastTimestamp() {
		return lastTimestamp;
	}

	public String getTopic() {
		return topic;
	}

	public long getMonitorGranularity() {
		return monitorGranularity;
	}

	public void writeCountsToHDFS(FileSystem fs, Path path) throws IOException {
		Map<String, Object> countFile = new HashMap<String, Object>();

		ArrayList<Map<String, Object>> countList = new ArrayList<Map<String, Object>>();
		for (Map.Entry<Source, Long> countEntry : trackingCount.entrySet()) {
			HashMap<String, Object> map = new HashMap<String, Object>();

			Source source = countEntry.getKey();

			map.put(START, source.getPartition());
			map.put(SERVER, source.getServer());
			map.put(SERVICE, source.getService());
			map.put(COUNT, countEntry.getValue());

			countList.add(map);
		}

		countFile.put(TOPIC, topic);
		countFile.put(MONITOR_GRANULARITY, monitorGranularity);
		countFile.put(COUNTS, countList);
		countFile.put(START_TIME, startTime);
		countFile.put(END_TIME, endTime);
		countFile.put(FIRST_TIMESTAMP, firstTimestamp);
		countFile.put(LAST_TIMESTAMP, lastTimestamp);
		countFile.put(ERROR_COUNT, errorCount);

		Path countOutput = path;
		System.out.println("Writing count to file " + path);
		OutputStream outputStream = new BufferedOutputStream(fs.create(path));

		ObjectMapper m = new ObjectMapper();
		m.writeValue(outputStream, countFile);

		outputStream.close();
		System.out.println("Finished writing to file " + countOutput);
	}

	/**
	 * Path filter that filters based on prefix
	 */
	private class PrefixFilter implements PathFilter {
		private final String prefix;

		public PrefixFilter(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public boolean accept(Path path) {
			// TODO Auto-generated method stub
			return path.getName().startsWith(prefix);
		}
	}

	public void postTrackingCountToKafka(String tier, URI brokerURI) {
		MessageEncoder<IndexedRecord, byte[]> encoder;
		try {
			log.setLevel(Level.DEBUG);
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
		for (Map.Entry<Source, Long> countEntry : trackingCount.entrySet()) {
			Source source = countEntry.getKey();
			long partition = source.getPartition();
			String serverName = source.getServer();
			String serviceName = source.getService();
			long count = countEntry.getValue();
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
			trackingRecord.endTimestamp = partition + monitorGranularity;
			trackingRecord.count = count;
			trackingRecord.tier = tier;
			trackingRecord.eventType = topic;

			byte[] message = encoder.toBytes(trackingRecord);
			monitorSet.add(message);

			if (monitorSet.size() >= 2000) {
				counts += monitorSet.size();
				produceCount(brokerURI, monitorSet);
				monitorSet.clear();
			}
		}

		if (monitorSet.size() > 0) {
			counts += monitorSet.size();
			produceCount(brokerURI, monitorSet);
		}

		System.out.println(topic + " sent " + counts + " counts");
	}

	private void produceCount(URI brokerURI, ArrayList<byte[]> monitorSet) {
		// Shuffle the broker

		Properties props = new Properties();
		props.put("broker.list",
				brokerURI.getHost() + ":" + brokerURI.getPort());
		props.put("producer.type", "async");
		props.put("request.required.acks", "-1");
		System.out.println("Host " + brokerURI.getHost() + " port "
				+ brokerURI.getPort());
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
					+ brokerURI.toString());
		} finally {
			if (producer != null) {
				producer.close();
			}
		}

	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("Topic:" + getTopic());
		buffer.append(", First:" + getFirstTimestamp());
		buffer.append(", Last:" + getLastTimestamp());
		buffer.append(", Start:" + getStartTime());
		buffer.append(", End:" + getEndTime());
		buffer.append(", Granularity:" + getMonitorGranularity());
		buffer.append(", ErrorCount:" + getErrorCount());
		buffer.append(", NumEntries:" + trackingCount.size());
		buffer.append("\n");

		// HashMap<Source, Long> trackingCount
		for (Map.Entry<Source, Long> entry : trackingCount.entrySet()) {
			buffer.append(entry.getKey().toString());
			buffer.append("->");
			buffer.append(entry.getValue());
			buffer.append("\n");
		}

		return buffer.toString();
	}

	private synchronized String getIntern(String value) {
		String intern = stringIntern.get(value);

		if (intern == null) {
			intern = value;
			stringIntern.put(value, intern);
		}

		return intern;
	}

	private class Source {
		private final String server;
		private final String service;
		private final Long partition;

		public Source(String server, String service, Long partition) {
			this.server = getIntern(server);
			this.service = getIntern(service);
			this.partition = partition;
		}

		public String getServer() {
			return server;
		}

		public String getService() {
			return service;
		}

		public Long getPartition() {
			return partition;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((partition == null) ? 0 : partition.hashCode());
			result = prime * result
					+ ((server == null) ? 0 : server.hashCode());
			result = prime * result
					+ ((service == null) ? 0 : service.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}

			Source other = (Source) obj;
			if (partition == null) {
				if (other.partition != null) {
					return false;
				}
			} else if (!partition.equals(other.partition)) {
				return false;
			}
			if (server == null) {
				if (other.server != null) {
					return false;
				}
			} else if (!server.equals(other.server)) {
				return false;
			}
			if (service == null) {
				if (other.service != null) {
					return false;
				}
			} else if (!service.equals(other.service)) {
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return "{" + server + "," + service + "," + partition + "}";
		}
	}
}
