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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import com.linkedin.camus.coders.MessageEncoder;
import com.linkedin.camus.etl.kafka.CamusJob;


@JsonIgnoreProperties({ "trackingCount", "lastKey", "eventCount", "RANDOM" })
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

  public static final int NUM_TRIES_PUBLISH_COUNTS = 3;

  private String topic;
  private long startTime;
  private long granularity;
  private long errorCount;
  private long endTime;
  private long lastTimestamp;
  private long firstTimestamp = Long.MAX_VALUE;
  protected HashMap<String, Source> counts;

  private transient EtlKey lastKey;
  private transient int eventCount = 0;
  private transient static final Random RANDOM = new Random();

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

  public EtlCounts(EtlCounts other) {
    this(other.topic, other.granularity, other.startTime);
    this.counts = other.counts;
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
    long monitorPartition = DateUtils.getPartition(granularity, key.getTime());
    Source source = new Source(key.getServer(), key.getService(), monitorPartition);
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

  public void writeCountsToMap(ArrayList<Map<String, Object>> allCountObject, FileSystem fs, Path path)
      throws IOException {
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

  public void postTrackingCountToKafka(Configuration conf, String tier, String brokerList) {
    MessageEncoder<IndexedRecord, byte[]> encoder;
    AbstractMonitoringEvent monitoringDetails;
    try {
      encoder =
          (MessageEncoder<IndexedRecord, byte[]>) Class.forName(conf.get(CamusJob.CAMUS_MESSAGE_ENCODER_CLASS))
              .newInstance();

      Properties props = new Properties();
      for (Entry<String, String> entry : conf) {
        props.put(entry.getKey(), entry.getValue());
      }

      encoder.init(props, "TrackingMonitoringEvent");
      monitoringDetails =
          (AbstractMonitoringEvent) Class.forName(getMonitoringEventClass(conf))
              .getDeclaredConstructor(Configuration.class).newInstance(conf);
    } catch (Exception e1) {
      throw new RuntimeException(e1);
    }

    ArrayList<byte[]> monitorSet = new ArrayList<byte[]>();
    int counts = 0;

    for (Map.Entry<String, Source> singleCount : this.getCounts().entrySet()) {
      Source countEntry = singleCount.getValue();
      GenericRecord monitoringRecord =
          monitoringDetails.createMonitoringEventRecord(countEntry, topic, granularity, tier);
      byte[] message = encoder.toBytes((IndexedRecord) monitoringRecord);
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

  protected String getMonitoringEventClass(Configuration conf) {
    return conf.get(MONITORING_EVENT_CLASS);
  }

  private void produceCount(String brokerList, ArrayList<byte[]> monitorSet) {
    // Shuffle the broker

    Properties props = new Properties();
    props.put("metadata.broker.list", brokerList);
    props.put("producer.type", "async");
    props.put("request.required.acks", "1");
    props.put("request.timeout.ms", "30000");
    log.debug("Broker list: " + brokerList);

    Producer producer = null;

    try {
      producer = createProducer(props);
      for (byte[] message : monitorSet) {
        for (int i = 0; i < NUM_TRIES_PUBLISH_COUNTS; i++) {
          try {
            KeyedMessage keyedMessage = new KeyedMessage("TrackingMonitoringEvent", message);
            producer.send(keyedMessage);
            break;
          } catch (Exception e) {
            log.error("Publishing count for topic " + topic + " to " + brokerList.toString() + " has failed " + (i + 1)
                + " times. " + (NUM_TRIES_PUBLISH_COUNTS - i - 1) + " more attempts will be made.");
            if (i == NUM_TRIES_PUBLISH_COUNTS - 1) {
              throw new RuntimeException(e.getMessage() + ": " + "Have retried maximum (" + NUM_TRIES_PUBLISH_COUNTS
                  + ") times.");
            }
            try {
              Thread.sleep((long) (Math.random() * (i + 1) * 1000));
            } catch (InterruptedException e1) {
              log.error("Caught interrupted exception between retries of publishing counts to Kafka. "
                  + e1.getMessage());
            }
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("failed to publish counts to kafka: " + e.getMessage(), e);
    } finally {
      if (producer != null) {
        producer.close();
      }
    }
  }

  protected Producer createProducer(Properties props) {
    return new Producer(new ProducerConfig(props));
  }

}
