package com.linkedin.camus.etl.kafka;

import com.linkedin.camus.etl.kafka.common.LeaderInfo;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedWriter;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A simple consumer to consume metadata from kafka and write our the difference
 * in the offsets of the partitions from previous run. This is used for
 * accounting purposes to figure out how many events are expected to be read
 * from kafka between successive runs of this consumer.
 *
 * @author krishna_puttaswamy
 */

class OffsetInfo
        implements WritableComparable<OffsetInfo>
{
  private String topic;
  private int partition;
  private long latestOffset;

  public String getTopic()
  {
    return topic;
  }

  public long getLatestOffset()
  {
    return latestOffset;
  }

  public int getPartition()

  {
    return partition;
  }

  public String toString()
  {
    return String.format("Topic: %s Partition: %d LatestOffset: %d", topic,
            partition, latestOffset);
  }

  OffsetInfo()
  {
    this.set("dummy", 0, 0);
  }

  OffsetInfo(String topic, int partition, long latestoffset)
  {
    this.set(topic, partition, latestoffset);
  }

  void set(String t, int p, long l)
  {
    this.topic = t;
    this.partition = p;
    this.latestOffset = l;
  }

  @Override
  public void write(DataOutput out)
          throws IOException
  {
    out.writeUTF(topic);
    out.writeInt(partition);
    out.writeLong(latestOffset);
  }

  @Override
  public void readFields(DataInput in)
          throws IOException
  {
    this.topic = in.readUTF();
    this.partition = in.readInt();
    this.latestOffset = in.readLong();
  }

  @Override
  public int compareTo(OffsetInfo o)
  {
    if (partition != o.partition) {
      return partition - o.partition;
    }
    else {
      if (latestOffset > o.latestOffset) {
        return 1;
      }
      else if (latestOffset > o.latestOffset) {
        return -1;
      }
      else {
        return 0;
      }
    }
  }
}

public class SnapshotDailyMetadata
{
  public static final String KAFKA_BLACKLIST_TOPIC = "kafka.blacklist.topics";
  public static final String KAFKA_WHITELIST_TOPIC = "kafka.whitelist.topics";
  public static final String KAFKA_METADATA_CONSUMER_PATH = "kafka.metadata.consumer.path";
  static Properties props = new Properties();
  final static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(SnapshotDailyMetadata.class);

  public static void main(String[] args)
          throws URISyntaxException,
          IOException, ParseException
  {
    Options options = new Options();

    options.addOption("p", true, "properties file for Camus");
    options.addOption("h", false, "help");

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    if (!(cmd.hasOption('p'))) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("SnapshotDailyMetadata.java", options);
      System.exit(1);
    }

    File file = new File(cmd.getOptionValue('p'));
    FileInputStream fStream;
    fStream = new FileInputStream(file);
    props.load(fStream);

    // Get topic metadata for all topics; this gives back the various
    // topics, and for each topic the partitions and the corresponding
    // leaders
    List<TopicMetadata> topicMetadataList = getTopicMetadataFromKafka();
    log.info("Got " + topicMetadataList.size()
            + " topic metadata list: "
            + StringUtils.join(",", topicMetadataList));

    // fetch latest offsets for each partition of the topic we care and
    // write them to hdfs
    List<OffsetInfo> newOffsets = fetchLatestOffset(topicMetadataList);
    writeLatestOffsetInfo(newOffsets);

    // read latest offsets from previous run
    Map<String, Long> oldOffsets = readLatestOffsetInfo();

    // compute the diff in the latest offsets
    JSONArray jsonData = new JSONArray();
    for (OffsetInfo key : newOffsets) {
      String pk = key.getTopic() + "_" + key.getPartition();
      if (oldOffsets.containsKey(pk)) {
        long diff = key.getLatestOffset() - oldOffsets.get(pk);

        JSONObject oneJsonNode = new JSONObject();
        oneJsonNode.put("topic", key.getTopic());
        oneJsonNode.put("partition", key.getPartition());
        oneJsonNode.put("difference", diff);
        jsonData.add(oneJsonNode);

        log.info(String.format("Topic: %s OffsetDifference: %d", pk, diff));
      }
    }

    // write out the diff as a json file
    writeToJsonFile(jsonData);

    // move current latest to old latest file
    renameCurrentFolderToPrevious();
  }

  public static void writeToJsonFile(JSONArray data)
          throws IOException
  {
    Job job = new Job();
    Configuration conf = job.getConfiguration();
    FileSystem fs = FileSystem.get(conf);

    if (!fs.exists(getCurrentOffsetsDir())) {
      fs.mkdirs(getCurrentOffsetsDir());
    }

    Path countersPath = new Path(new Path(getMetadataDir()),
            "diff/difference.json");
    fs.delete(countersPath, true);

    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
            fs.create(countersPath)));
    writer.write(data.toJSONString());
    writer.close();
  }

  public static void renameCurrentFolderToPrevious()
  {
    try {
      Job job = new Job();
      Configuration conf = job.getConfiguration();
      FileSystem fs = FileSystem.get(conf);

      if (!fs.exists(getOldOffsetsDir())) {
        fs.mkdirs(getOldOffsetsDir());
      }

      if (fs.exists(getOldOffsetsPath())) {
        fs.delete(getOldOffsetsPath(), true);
      }

      if (fs.exists(getCurrentOffsetsPath())) {
        fs.rename(getCurrentOffsetsPath(), getOldOffsetsPath());
      }

      log.info("Successfully renamed "
              + getCurrentOffsetsPath() + " to " + getOldOffsetsPath());
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String getMetadataDir()
          throws IOException
  {
    String metadataDir = props.getProperty(KAFKA_METADATA_CONSUMER_PATH);
    if (metadataDir == null) {
      throw new IOException(KAFKA_METADATA_CONSUMER_PATH
              + " is not specified.");
    }
    return metadataDir;
  }

  public static Path getOffsetsDiffPath()
          throws IOException
  {
    return new Path(getMetadataDir(), "current/difference.seq");
  }

  public static Path getCurrentOffsetsDir()
          throws IOException
  {
    return new Path(getMetadataDir(), "current");
  }

  public static Path getCurrentOffsetsPath()
          throws IOException
  {
    return new Path(getCurrentOffsetsDir(), "offsets.seq");
  }

  public static Path getOldOffsetsDir()
          throws IOException
  {
    return new Path(getMetadataDir(), "previous");
  }

  public static Path getOldOffsetsPath()
          throws IOException
  {
    return new Path(getOldOffsetsDir(), "offsets.seq");
  }

  public static Map<String, Long> readLatestOffsetInfo()
  {
    Map<String, Long> partitionToOffsetMap = new HashMap<String, Long>();

    try {
      Path oldOutputPath = getOldOffsetsPath();
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);

      SequenceFile.Reader reader = new SequenceFile.Reader(fs,
              oldOutputPath, conf);

      OffsetInfo key = new OffsetInfo();

      while (reader.next(key, NullWritable.get())) {
        partitionToOffsetMap.put(
                key.getTopic() + "_" + key.getPartition(),
                key.getLatestOffset());
      }

      reader.close();
    }
    catch (IOException e) {
      e.printStackTrace();
    }

    return partitionToOffsetMap;
  }

  public static void writeLatestOffsetInfo(List<OffsetInfo> latestList)
          throws IOException
  {
    Job job = new Job();
    Configuration conf = job.getConfiguration();
    FileSystem fs = FileSystem.get(conf);

    String metadataDir = getMetadataDir();

    if (!fs.exists(new Path(metadataDir))) {
      log.info("creating directory " + metadataDir);
      fs.mkdirs(new Path(metadataDir));
    }

    Path newOutputPath = getCurrentOffsetsPath();

    log.info("creating file " + newOutputPath.toString());
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
            newOutputPath, OffsetInfo.class, NullWritable.class);

    for (OffsetInfo key : latestList) {
      writer.append(key, NullWritable.get());
    }

    writer.close();
  }

  public static String createTopicRegEx(Set<String> topicsSet)
  {
    if (topicsSet.isEmpty()) {
      return "";
    }

    String regex = "";
    StringBuilder stringbuilder = new StringBuilder();
    for (String whiteList : topicsSet) {
      stringbuilder.append(whiteList);
      stringbuilder.append("|");
    }
    regex = "(" + stringbuilder.substring(0, stringbuilder.length() - 1) + ")";
    return regex;
  }

  public static String[] getKafkaBlacklistTopic()
  {
    if (props.get(KAFKA_BLACKLIST_TOPIC) != null
            && !props.getProperty(KAFKA_BLACKLIST_TOPIC).isEmpty()) {
      return StringUtils.getStrings(props
              .getProperty(KAFKA_BLACKLIST_TOPIC));
    }
    else {
      return new String[] {};
    }
  }

  public static String[] getKafkaWhitelistTopic()
  {
    if (props.get(KAFKA_WHITELIST_TOPIC) != null
            && !props.getProperty(KAFKA_WHITELIST_TOPIC).isEmpty()) {
      return StringUtils.getStrings(props
              .getProperty(KAFKA_WHITELIST_TOPIC));
    }
    else {
      return new String[] {};
    }
  }

  public static List<TopicMetadata> filterWhitelistTopics(
          List<TopicMetadata> topicMetadataList,
          Set<String> whiteListTopics)
  {
    ArrayList<TopicMetadata> filteredTopics = new ArrayList<TopicMetadata>();
    String regex = createTopicRegEx(whiteListTopics);
    for (TopicMetadata topicMetadata : topicMetadataList) {
      if (Pattern.matches(regex, topicMetadata.topic())) {
        filteredTopics.add(topicMetadata);
      }
    }
    return filteredTopics;
  }

  public static List<OffsetInfo> fetchLatestOffset(
          List<TopicMetadata> topicMetadataList)
          throws URISyntaxException
  {
    // Filter any white list topics
    Set<String> whiteListTopics = new HashSet<String>(
            Arrays.asList(getKafkaWhitelistTopic()));
    if (!whiteListTopics.isEmpty()) {
      topicMetadataList = filterWhitelistTopics(topicMetadataList, whiteListTopics);
    }

    // Filter all blacklist topics
    Set<String> blackListTopics = new HashSet<String>(
            Arrays.asList(getKafkaBlacklistTopic()));
    String regex = "";
    if (!blackListTopics.isEmpty()) {
      regex = createTopicRegEx(blackListTopics);
    }

    Map<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo = new HashMap<LeaderInfo, ArrayList<TopicAndPartition>>();

    // create the list of partition offset requests to the leaders
    for (TopicMetadata mdata : topicMetadataList) {
      if (Pattern.matches(regex, mdata.topic())) {
        log.info("Discarding topic (blacklisted): "
                + mdata.topic());
      }
      else {
        for (PartitionMetadata pdata : mdata.partitionsMetadata()) {
          LeaderInfo leader = new LeaderInfo(new URI("tcp://"
                  + pdata.leader().getConnectionString()), pdata
                  .leader().id());

          if (offsetRequestInfo.containsKey(leader)) {
            ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo
                    .get(leader);
            topicAndPartitions.add(new TopicAndPartition(mdata
                    .topic(), pdata.partitionId()));
            offsetRequestInfo.put(leader, topicAndPartitions);
          }
          else {
            ArrayList<TopicAndPartition> topicAndPartitions = new ArrayList<TopicAndPartition>();
            topicAndPartitions.add(new TopicAndPartition(mdata
                    .topic(), pdata.partitionId()));
            offsetRequestInfo.put(leader, topicAndPartitions);
          }
        }
      }
    }

    List<OffsetInfo> offsetList = new ArrayList<OffsetInfo>();

    for (LeaderInfo leader : offsetRequestInfo.keySet()) {
      SimpleConsumer consumer = new SimpleConsumer(leader.getUri()
              .getHost(), leader.getUri().getPort(),
              Integer.parseInt(props.getProperty(
                      CamusJob.KAFKA_TIMEOUT_VALUE, "30000")),
              Integer.parseInt(props.getProperty(
                      CamusJob.KAFKA_FETCH_BUFFER_SIZE, "83886080")),
              props.getProperty(CamusJob.KAFKA_CLIENT_NAME,
                      "KafkaMetadatFetcher"));

      // Latest Offset
      PartitionOffsetRequestInfo partitionLatestOffsetRequestInfo = new PartitionOffsetRequestInfo(
              kafka.api.OffsetRequest.LatestTime(), 1);

      Map<TopicAndPartition, PartitionOffsetRequestInfo> latestOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
      ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo
              .get(leader);
      for (TopicAndPartition topicAndPartition : topicAndPartitions) {
        latestOffsetInfo.put(topicAndPartition,
                partitionLatestOffsetRequestInfo);
      }

      int tries = 0;
      OffsetResponse latestOffsetResponse = null;
      while (tries < 3) {
        latestOffsetResponse = consumer
                .getOffsetsBefore(new OffsetRequest(latestOffsetInfo,
                        kafka.api.OffsetRequest.CurrentVersion(),
                        "KafkaMetadatFetcher"));
        if (!latestOffsetResponse.hasError()) {
          break;
        }
        try {
          Thread.sleep(300);
        }
        catch (java.lang.InterruptedException e) {
          // ...
        }
        tries++;
      }

      consumer.close();

      if (latestOffsetResponse != null) {
        for (TopicAndPartition topicAndPartition : topicAndPartitions) {
          long latestOffset = 0;
          long offsets[] = latestOffsetResponse.offsets(
                  topicAndPartition.topic(),
                  topicAndPartition.partition());
          if (offsets.length > 0) {
            latestOffset = offsets[0];
          }

          offsetList.add(new OffsetInfo(topicAndPartition.topic().toString(),
                  topicAndPartition.partition(), latestOffset));
        }
      }
    }

    return offsetList;
  }

  private static SimpleConsumer createConsumer(String broker)
  {
    if (!broker.matches(".+:\\d+")) {
      throw new InvalidParameterException("The kakfa broker " + broker
              + " must follow address:port pattern");
    }
    String[] hostPort = broker.split(":");
    SimpleConsumer consumer = new SimpleConsumer(hostPort[0],
            Integer.valueOf(hostPort[1]), Integer.parseInt(props
            .getProperty(CamusJob.KAFKA_TIMEOUT_VALUE, "30000")),
            Integer.parseInt(props.getProperty(
                    CamusJob.KAFKA_FETCH_BUFFER_SIZE, "83886080")),
            props.getProperty(CamusJob.KAFKA_CLIENT_NAME,
                    "KafkaMetadatFetcher"));
    return consumer;
  }

  public static List<TopicMetadata> getTopicMetadataFromKafka()
  {
    ArrayList<String> metaRequestTopics = new ArrayList<String>();
    String brokerString = props.getProperty(CamusJob.KAFKA_BROKERS);
    if (brokerString.isEmpty()) {
      throw new InvalidParameterException(
              "kafka.brokers must contain at least one node");
    }
    List<String> brokers = Arrays.asList(brokerString.split("\\s*,\\s*"));
    Collections.shuffle(brokers);
    boolean fetchMetaDataSucceeded = false;
    int i = 0;
    List<TopicMetadata> topicMetadataList = null;
    Exception savedException = null;
    while (i < brokers.size() && !fetchMetaDataSucceeded) {
      SimpleConsumer consumer = createConsumer(brokers.get(i));
      log.info(String.format("Fetching metadata from broker %s with client id %s for %d topic(s) %s",
                              brokers.get(i), consumer.clientId(),
                              metaRequestTopics.size(), metaRequestTopics));
      try {
        topicMetadataList = consumer.send(
                new TopicMetadataRequest(metaRequestTopics))
                .topicsMetadata();
        fetchMetaDataSucceeded = true;
      }
      catch (Exception e) {
        savedException = e;
        e.printStackTrace();
      }
      finally {
        consumer.close();
        i++;
      }
    }
    if (!fetchMetaDataSucceeded) {
      throw new RuntimeException("Failed to obtain metadata!",
              savedException);
    }
    return topicMetadataList;
  }
}
