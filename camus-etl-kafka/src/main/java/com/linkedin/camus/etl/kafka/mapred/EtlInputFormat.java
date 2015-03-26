package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageDecoder;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.common.LeaderInfo;
import com.linkedin.camus.workallocater.CamusRequest;
import com.linkedin.camus.workallocater.WorkAllocator;

import java.io.IOException;
import java.net.URI;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageDecoder;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.common.LeaderInfo;
import com.linkedin.camus.workallocater.CamusRequest;
import com.linkedin.camus.workallocater.WorkAllocator;


/**
 * Input format for a Kafka pull job.
 */
public class EtlInputFormat extends InputFormat<EtlKey, CamusWrapper> {

  public static final String KAFKA_BLACKLIST_TOPIC = "kafka.blacklist.topics";
  public static final String KAFKA_WHITELIST_TOPIC = "kafka.whitelist.topics";

  public static final String KAFKA_MOVE_TO_LAST_OFFSET_LIST = "kafka.move.to.last.offset.list";
  public static final String KAFKA_MOVE_TO_EARLIEST_OFFSET = "kafka.move.to.earliest.offset";

  public static final String KAFKA_CLIENT_BUFFER_SIZE = "kafka.client.buffer.size";
  public static final String KAFKA_CLIENT_SO_TIMEOUT = "kafka.client.so.timeout";

  public static final String KAFKA_MAX_PULL_HRS = "kafka.max.pull.hrs";
  public static final String KAFKA_MAX_PULL_MINUTES_PER_TASK = "kafka.max.pull.minutes.per.task";
  public static final String KAFKA_MAX_HISTORICAL_DAYS = "kafka.max.historical.days";

  public static final String CAMUS_MESSAGE_DECODER_CLASS = "camus.message.decoder.class";
  public static final String ETL_IGNORE_SCHEMA_ERRORS = "etl.ignore.schema.errors";
  public static final String ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST = "etl.audit.ignore.service.topic.list";

  public static final String CAMUS_WORK_ALLOCATOR_CLASS = "camus.work.allocator.class";
  public static final String CAMUS_WORK_ALLOCATOR_DEFAULT = "com.linkedin.camus.workallocater.BaseAllocator";

  private static final int BACKOFF_UNIT_MILLISECONDS = 1000;

  public static final int NUM_TRIES_PARTITION_METADATA = 3;
  public static final int NUM_TRIES_FETCH_FROM_LEADER = 3;
  public static final int NUM_TRIES_TOPIC_METADATA = 3;

  public static boolean reportJobFailureDueToOffsetOutOfRange = false;
  public static boolean reportJobFailureUnableToGetOffsetFromKafka = false;
  public static boolean reportJobFailureDueToLeaderNotAvailable = false;

  private static Logger log = null;

  public EtlInputFormat() {
    if (log == null)
      log = Logger.getLogger(getClass());
  }

  public static void setLogger(Logger log) {
    EtlInputFormat.log = log;
  }

  @Override
  public RecordReader<EtlKey, CamusWrapper> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new EtlRecordReader(this, split, context);
  }

  /**
   * Gets the metadata from Kafka
   *
   * @param context
   * @param metaRequestTopics specify the list of topics to get topicMetadata. The empty list means
   * get the TopicsMetadata for all topics.
   * @return the list of TopicMetadata
   */
  public List<TopicMetadata> getKafkaMetadata(JobContext context, List<String> metaRequestTopics) {
    CamusJob.startTiming("kafkaSetupTime");
    String brokerString = CamusJob.getKafkaBrokers(context);
    if (brokerString.isEmpty())
      throw new InvalidParameterException("kafka.brokers must contain at least one node");
    List<String> brokers = Arrays.asList(brokerString.split("\\s*,\\s*"));
    Collections.shuffle(brokers);
    boolean fetchMetaDataSucceeded = false;
    int i = 0;
    List<TopicMetadata> topicMetadataList = null;
    Exception savedException = null;
    while (i < brokers.size() && !fetchMetaDataSucceeded) {
      SimpleConsumer consumer = createBrokerConsumer(context, brokers.get(i));
      log.info(String.format("Fetching metadata from broker %s with client id %s for %d topic(s) %s", brokers.get(i),
          consumer.clientId(), metaRequestTopics.size(), metaRequestTopics));
      try {
        for (int iter = 0; iter < NUM_TRIES_TOPIC_METADATA; iter++) {
          try {
            topicMetadataList = consumer.send(new TopicMetadataRequest(metaRequestTopics)).topicsMetadata();
            fetchMetaDataSucceeded = true;
            break;
          } catch (Exception e) {
            savedException = e;
            log.warn(String.format(
                "Fetching topic metadata with client id %s for topics [%s] from broker [%s] failed, iter[%s]",
                consumer.clientId(), metaRequestTopics, brokers.get(i), iter), e);
            try {
              Thread.sleep((long) (Math.random() * (iter + 1) * 1000));
            } catch (InterruptedException ex) {
              log.warn("Caught InterruptedException: " + ex);
            }
          }
        }
      } finally {
        consumer.close();
        i++;
      }
    }
    if (!fetchMetaDataSucceeded) {
      throw new RuntimeException("Failed to obtain metadata!", savedException);
    }
    CamusJob.stopTiming("kafkaSetupTime");
    return topicMetadataList;
  }

  private SimpleConsumer createBrokerConsumer(JobContext context, String broker) {
    if (!broker.matches(".+:\\d+"))
      throw new InvalidParameterException("The kakfa broker " + broker + " must follow address:port pattern");
    String[] hostPort = broker.split(":");
    return createSimpleConsumer(context, hostPort[0], Integer.valueOf(hostPort[1]));
  }

  public SimpleConsumer createSimpleConsumer(JobContext context, String host, int port) {
    SimpleConsumer consumer =
        new SimpleConsumer(host, port, CamusJob.getKafkaTimeoutValue(context), CamusJob.getKafkaBufferSize(context),
            CamusJob.getKafkaClientName(context));
    return consumer;
  }

  /**
   * Gets the latest offsets and create the requests as needed
   *
   * @param context
   * @param offsetRequestInfo
   * @return
   */
  public ArrayList<CamusRequest> fetchLatestOffsetAndCreateEtlRequests(JobContext context,
      HashMap<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo) {
    ArrayList<CamusRequest> finalRequests = new ArrayList<CamusRequest>();
    for (LeaderInfo leader : offsetRequestInfo.keySet()) {
      SimpleConsumer consumer = createSimpleConsumer(context, leader.getUri().getHost(), leader.getUri().getPort());
      // Latest Offset
      PartitionOffsetRequestInfo partitionLatestOffsetRequestInfo =
          new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1);
      // Earliest Offset
      PartitionOffsetRequestInfo partitionEarliestOffsetRequestInfo =
          new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1);
      Map<TopicAndPartition, PartitionOffsetRequestInfo> latestOffsetInfo =
          new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
      Map<TopicAndPartition, PartitionOffsetRequestInfo> earliestOffsetInfo =
          new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
      ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo.get(leader);
      for (TopicAndPartition topicAndPartition : topicAndPartitions) {
        latestOffsetInfo.put(topicAndPartition, partitionLatestOffsetRequestInfo);
        earliestOffsetInfo.put(topicAndPartition, partitionEarliestOffsetRequestInfo);
      }

      OffsetResponse latestOffsetResponse = getLatestOffsetResponse(consumer, latestOffsetInfo, context);
      OffsetResponse earliestOffsetResponse = null;
      if (latestOffsetResponse != null) {
        earliestOffsetResponse = getLatestOffsetResponse(consumer, earliestOffsetInfo, context);
      }
      consumer.close();
      if (earliestOffsetResponse == null) {
        log.warn(generateLogWarnForSkippedTopics(earliestOffsetInfo, consumer));
        reportJobFailureUnableToGetOffsetFromKafka = true;
        continue;
      }

      for (TopicAndPartition topicAndPartition : topicAndPartitions) {
        long latestOffset = latestOffsetResponse.offsets(topicAndPartition.topic(), topicAndPartition.partition())[0];
        long earliestOffset =
            earliestOffsetResponse.offsets(topicAndPartition.topic(), topicAndPartition.partition())[0];

        //TODO: factor out kafka specific request functionality
        CamusRequest etlRequest =
            new EtlRequest(context, topicAndPartition.topic(), Integer.toString(leader.getLeaderId()),
                topicAndPartition.partition(), leader.getUri());
        etlRequest.setLatestOffset(latestOffset);
        etlRequest.setEarliestOffset(earliestOffset);
        finalRequests.add(etlRequest);
      }
    }
    return finalRequests;
  }

  protected OffsetResponse getLatestOffsetResponse(SimpleConsumer consumer,
      Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo, JobContext context) {
    for (int i = 0; i < NUM_TRIES_FETCH_FROM_LEADER; i++) {
      try {
        OffsetResponse offsetResponse =
            consumer.getOffsetsBefore(new OffsetRequest(offsetInfo, kafka.api.OffsetRequest.CurrentVersion(), CamusJob
                .getKafkaClientName(context)));
        if (offsetResponse.hasError()) {
          throw new RuntimeException("offsetReponse has error.");
        }
        return offsetResponse;
      } catch (Exception e) {
        log.warn("Fetching offset from leader " + consumer.host() + ":" + consumer.port() + " has failed " + (i + 1)
            + " time(s). Reason: " + e.getMessage() + " " + (NUM_TRIES_FETCH_FROM_LEADER - i - 1) + " retries left.");
        if (i < NUM_TRIES_FETCH_FROM_LEADER - 1) {
          try {
            Thread.sleep((long) (Math.random() * (i + 1) * 1000));
          } catch (InterruptedException e1) {
            log.error("Caught interrupted exception between retries of getting latest offsets. " + e1.getMessage());
          }
        }
      }
    }
    return null;
  }

  private String generateLogWarnForSkippedTopics(Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo,
      SimpleConsumer consumer) {
    StringBuilder sb = new StringBuilder();
    sb.append("The following topics will be skipped due to failure in fetching latest offsets from leader "
        + consumer.host() + ":" + consumer.port());
    for (TopicAndPartition topicAndPartition : offsetInfo.keySet()) {
      sb.append("  " + topicAndPartition.topic());
    }
    return sb.toString();
  }

  public String createTopicRegEx(HashSet<String> topicsSet) {
    String regex = "";
    StringBuilder stringbuilder = new StringBuilder();
    for (String whiteList : topicsSet) {
      stringbuilder.append(whiteList);
      stringbuilder.append("|");
    }
    regex = "(" + stringbuilder.substring(0, stringbuilder.length() - 1) + ")";
    Pattern.compile(regex);
    return regex;
  }

  public List<TopicMetadata> filterWhitelistTopics(List<TopicMetadata> topicMetadataList,
      HashSet<String> whiteListTopics) {
    ArrayList<TopicMetadata> filteredTopics = new ArrayList<TopicMetadata>();
    String regex = createTopicRegEx(whiteListTopics);
    for (TopicMetadata topicMetadata : topicMetadataList) {
      if (Pattern.matches(regex, topicMetadata.topic())) {
        filteredTopics.add(topicMetadata);
      } else {
        log.info("Discarding topic : " + topicMetadata.topic());
      }
    }
    return filteredTopics;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    CamusJob.startTiming("getSplits");
    ArrayList<CamusRequest> finalRequests;
    HashMap<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo =
        new HashMap<LeaderInfo, ArrayList<TopicAndPartition>>();
    try {

      // Get Metadata for all topics
      List<TopicMetadata> topicMetadataList = getKafkaMetadata(context, new ArrayList<String>());

      // Filter any white list topics
      HashSet<String> whiteListTopics = new HashSet<String>(Arrays.asList(getKafkaWhitelistTopic(context)));
      if (!whiteListTopics.isEmpty()) {
        topicMetadataList = filterWhitelistTopics(topicMetadataList, whiteListTopics);
      }

      // Filter all blacklist topics
      HashSet<String> blackListTopics = new HashSet<String>(Arrays.asList(getKafkaBlacklistTopic(context)));
      String regex = "";
      if (!blackListTopics.isEmpty()) {
        regex = createTopicRegEx(blackListTopics);
      }

      for (TopicMetadata topicMetadata : topicMetadataList) {
        if (Pattern.matches(regex, topicMetadata.topic())) {
          log.info("Discarding topic (blacklisted): " + topicMetadata.topic());
        } else if (!createMessageDecoder(context, topicMetadata.topic())) {
          log.info("Discarding topic (Decoder generation failed) : " + topicMetadata.topic());
        } else if (topicMetadata.errorCode() != ErrorMapping.NoError()) {
          log.info("Skipping the creation of ETL request for Whole Topic : " + topicMetadata.topic() + " Exception : "
              + ErrorMapping.exceptionFor(topicMetadata.errorCode()));
        } else {
          for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
            // We only care about LeaderNotAvailableCode error on partitionMetadata level
            // Error codes such as ReplicaNotAvailableCode should not stop us.
            partitionMetadata =
                this.refreshPartitionMetadataOnLeaderNotAvailable(partitionMetadata, topicMetadata, context,
                    NUM_TRIES_PARTITION_METADATA);

            if (partitionMetadata.errorCode() == ErrorMapping.LeaderNotAvailableCode()) {
              log.info("Skipping the creation of ETL request for Topic : " + topicMetadata.topic()
                  + " and Partition : " + partitionMetadata.partitionId() + " Exception : "
                  + ErrorMapping.exceptionFor(partitionMetadata.errorCode()));
              reportJobFailureDueToLeaderNotAvailable = true; 
            } else {
              if (partitionMetadata.errorCode() != ErrorMapping.NoError()) {
                log.warn("Receiving non-fatal error code, Continuing the creation of ETL request for Topic : "
                    + topicMetadata.topic() + " and Partition : " + partitionMetadata.partitionId() + " Exception : "
                    + ErrorMapping.exceptionFor(partitionMetadata.errorCode()));
              }
              LeaderInfo leader =
                  new LeaderInfo(new URI("tcp://" + partitionMetadata.leader().getConnectionString()),
                      partitionMetadata.leader().id());
              if (offsetRequestInfo.containsKey(leader)) {
                ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo.get(leader);
                topicAndPartitions.add(new TopicAndPartition(topicMetadata.topic(), partitionMetadata.partitionId()));
                offsetRequestInfo.put(leader, topicAndPartitions);
              } else {
                ArrayList<TopicAndPartition> topicAndPartitions = new ArrayList<TopicAndPartition>();
                topicAndPartitions.add(new TopicAndPartition(topicMetadata.topic(), partitionMetadata.partitionId()));
                offsetRequestInfo.put(leader, topicAndPartitions);
              }

            }
          }
        }
      }
    } catch (Exception e) {
      log.error("Unable to pull requests from Kafka brokers. Exiting the program", e);
      throw new IOException("Unable to pull requests from Kafka brokers.", e);
    }
    // Get the latest offsets and generate the EtlRequests
    finalRequests = fetchLatestOffsetAndCreateEtlRequests(context, offsetRequestInfo);

    Collections.sort(finalRequests, new Comparator<CamusRequest>() {
      @Override
      public int compare(CamusRequest r1, CamusRequest r2) {
        return r1.getTopic().compareTo(r2.getTopic());
      }
    });

    log.info("The requests from kafka metadata are: \n" + finalRequests);
    writeRequests(finalRequests, context);
    Map<CamusRequest, EtlKey> offsetKeys = getPreviousOffsets(FileInputFormat.getInputPaths(context), context);
    Set<String> moveLatest = getMoveToLatestTopicsSet(context);
    for (CamusRequest request : finalRequests) {
      if (moveLatest.contains(request.getTopic()) || moveLatest.contains("all")) {
        log.info("Moving to latest for topic: " + request.getTopic());
        //TODO: factor out kafka specific request functionality
        EtlKey oldKey = offsetKeys.get(request);
        EtlKey newKey =
            new EtlKey(request.getTopic(), ((EtlRequest) request).getLeaderId(), request.getPartition(), 0,
                request.getLastOffset());

        if (oldKey != null)
          newKey.setMessageSize(oldKey.getMessageSize());

        offsetKeys.put(request, newKey);
      }

      EtlKey key = offsetKeys.get(request);

      if (key != null) {
        request.setOffset(key.getOffset());
        request.setAvgMsgSize(key.getMessageSize());
      }

      if (request.getEarliestOffset() > request.getOffset() || request.getOffset() > request.getLastOffset()) {
        if (request.getEarliestOffset() > request.getOffset()) {
          log.error("The earliest offset was found to be more than the current offset: " + request);
        } else {
          log.error("The current offset was found to be more than the latest offset: " + request);
        }

        boolean move_to_earliest_offset = context.getConfiguration().getBoolean(KAFKA_MOVE_TO_EARLIEST_OFFSET, false);
        boolean offsetUnset = request.getOffset() == EtlRequest.DEFAULT_OFFSET;
        log.info("move_to_earliest: " + move_to_earliest_offset + " offset_unset: " + offsetUnset);
        // When the offset is unset, it means it's a new topic/partition, we also need to consume the earliest offset
        if (move_to_earliest_offset || offsetUnset) {
          log.error("Moving to the earliest offset available");
          request.setOffset(request.getEarliestOffset());
          offsetKeys.put(
              request,
              //TODO: factor out kafka specific request functionality
              new EtlKey(request.getTopic(), ((EtlRequest) request).getLeaderId(), request.getPartition(), 0, request
                  .getOffset()));
        } else {
          log.error("Offset range from kafka metadata is outside the previously persisted offset, " + request + "\n" +
                    " Topic " + request.getTopic() + " will be skipped.\n" +
                    " Please check whether kafka cluster configuration is correct." +
                    " You can also specify config parameter: " + KAFKA_MOVE_TO_EARLIEST_OFFSET +
                    " to start processing from earliest kafka metadata offset.");
          reportJobFailureDueToOffsetOutOfRange = true;
        }
      }
      log.info(request);
    }

    writePrevious(offsetKeys.values(), context);

    CamusJob.stopTiming("getSplits");
    CamusJob.startTiming("hadoop");
    CamusJob.setTime("hadoop_start");

    WorkAllocator allocator = getWorkAllocator(context);
    Properties props = new Properties();
    props.putAll(context.getConfiguration().getValByRegex(".*"));
    allocator.init(props);

    return allocator.allocateWork(finalRequests, context);
  }

  private Set<String> getMoveToLatestTopicsSet(JobContext context) {
    Set<String> topics = new HashSet<String>();

    String[] arr = getMoveToLatestTopics(context);

    if (arr != null) {
      for (String topic : arr) {
        topics.add(topic);
      }
    }

    return topics;
  }

  private boolean createMessageDecoder(JobContext context, String topic) {
    try {
      MessageDecoderFactory.createMessageDecoder(context, topic);
      return true;
    } catch (Exception e) {
      log.error("failed to create decoder", e);
      return false;
    }
  }

  private void writePrevious(Collection<EtlKey> missedKeys, JobContext context) throws IOException {
    FileSystem fs = FileSystem.get(context.getConfiguration());
    Path output = FileOutputFormat.getOutputPath(context);

    if (fs.exists(output)) {
      fs.mkdirs(output);
    }

    output = new Path(output, EtlMultiOutputFormat.OFFSET_PREFIX + "-previous");
    SequenceFile.Writer writer =
        SequenceFile.createWriter(fs, context.getConfiguration(), output, EtlKey.class, NullWritable.class);

    for (EtlKey key : missedKeys) {
      writer.append(key, NullWritable.get());
    }

    writer.close();
  }

  protected void writeRequests(List<CamusRequest> requests, JobContext context) throws IOException {
    FileSystem fs = FileSystem.get(context.getConfiguration());
    Path output = FileOutputFormat.getOutputPath(context);

    if (fs.exists(output)) {
      fs.mkdirs(output);
    }

    output = new Path(output, EtlMultiOutputFormat.REQUESTS_FILE);
    SequenceFile.Writer writer =
        SequenceFile.createWriter(fs, context.getConfiguration(), output, EtlRequest.class, NullWritable.class);

    for (CamusRequest r : requests) {
      //TODO: factor out kafka specific request functionality
      writer.append(r, NullWritable.get());
    }
    writer.close();
  }

  private Map<CamusRequest, EtlKey> getPreviousOffsets(Path[] inputs, JobContext context) throws IOException {
    Map<CamusRequest, EtlKey> offsetKeysMap = new HashMap<CamusRequest, EtlKey>();
    for (Path input : inputs) {
      FileSystem fs = input.getFileSystem(context.getConfiguration());
      for (FileStatus f : fs.listStatus(input, new OffsetFileFilter())) {
        log.info("previous offset file:" + f.getPath().toString());
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, f.getPath(), context.getConfiguration());
        EtlKey key = new EtlKey();
        while (reader.next(key, NullWritable.get())) {
          //TODO: factor out kafka specific request functionality
          CamusRequest request = new EtlRequest(context, key.getTopic(), key.getLeaderId(), key.getPartition());
          if (offsetKeysMap.containsKey(request)) {

            EtlKey oldKey = offsetKeysMap.get(request);
            if (oldKey.getOffset() < key.getOffset()) {
              offsetKeysMap.put(request, key);
            }
          } else {
            offsetKeysMap.put(request, key);
          }
          key = new EtlKey();
        }
        reader.close();
      }
    }
    return offsetKeysMap;
  }

  public PartitionMetadata refreshPartitionMetadataOnLeaderNotAvailable(PartitionMetadata partitionMetadata,
      TopicMetadata topicMetadata, JobContext context, int numTries) throws InterruptedException {
    int tryCounter = 0;
    while (tryCounter < numTries && partitionMetadata.errorCode() == ErrorMapping.LeaderNotAvailableCode()) {
      log.info("Retry to referesh the topicMetadata on LeaderNotAvailable...");
      List<TopicMetadata> topicMetadataList =
          this.getKafkaMetadata(context, Collections.singletonList(topicMetadata.topic()));
      if (topicMetadataList == null || topicMetadataList.size() == 0) {
        log.warn("The topicMetadataList for topic " + topicMetadata.topic() + " is empty.");
      } else {
        topicMetadata = topicMetadataList.get(0);
        boolean partitionFound = false;
        for (PartitionMetadata metadataPerPartition : topicMetadata.partitionsMetadata()) {
          if (metadataPerPartition.partitionId() == partitionMetadata.partitionId()) {
            partitionFound = true;
            if (metadataPerPartition.errorCode() != ErrorMapping.LeaderNotAvailableCode()) {
              return metadataPerPartition;
            } else { //retry again.
              if (tryCounter < numTries - 1) {
                Thread.sleep((long) (Math.random() * (tryCounter + 1) * BACKOFF_UNIT_MILLISECONDS));
              }
              break;
            }
          }
        }
        if (!partitionFound) {
          log.error("No matching partition found in the topicMetadata for Partition: "
              + partitionMetadata.partitionId());
        }
      }
      tryCounter++;
    }
    return partitionMetadata;
  }

  public static void setWorkAllocator(JobContext job, Class<WorkAllocator> val) {
    job.getConfiguration().setClass(CAMUS_WORK_ALLOCATOR_CLASS, val, WorkAllocator.class);
  }

  public static WorkAllocator getWorkAllocator(JobContext job) {
    try {
      return (WorkAllocator) job.getConfiguration()
          .getClass(CAMUS_WORK_ALLOCATOR_CLASS, Class.forName(CAMUS_WORK_ALLOCATOR_DEFAULT)).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void setMoveToLatestTopics(JobContext job, String val) {
    job.getConfiguration().set(KAFKA_MOVE_TO_LAST_OFFSET_LIST, val);
  }

  public static String[] getMoveToLatestTopics(JobContext job) {
    return job.getConfiguration().getStrings(KAFKA_MOVE_TO_LAST_OFFSET_LIST);
  }

  public static void setKafkaClientBufferSize(JobContext job, int val) {
    job.getConfiguration().setInt(KAFKA_CLIENT_BUFFER_SIZE, val);
  }

  public static int getKafkaClientBufferSize(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_CLIENT_BUFFER_SIZE, 2 * 1024 * 1024);
  }

  public static void setKafkaClientTimeout(JobContext job, int val) {
    job.getConfiguration().setInt(KAFKA_CLIENT_SO_TIMEOUT, val);
  }

  public static int getKafkaClientTimeout(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_CLIENT_SO_TIMEOUT, 60000);
  }

  public static void setKafkaMaxPullHrs(JobContext job, int val) {
    job.getConfiguration().setInt(KAFKA_MAX_PULL_HRS, val);
  }

  public static int getKafkaMaxPullHrs(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_MAX_PULL_HRS, -1);
  }

  public static void setKafkaMaxPullMinutesPerTask(JobContext job, int val) {
    job.getConfiguration().setInt(KAFKA_MAX_PULL_MINUTES_PER_TASK, val);
  }

  public static int getKafkaMaxPullMinutesPerTask(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_MAX_PULL_MINUTES_PER_TASK, -1);
  }

  public static void setKafkaMaxHistoricalDays(JobContext job, int val) {
    job.getConfiguration().setInt(KAFKA_MAX_HISTORICAL_DAYS, val);
  }

  public static int getKafkaMaxHistoricalDays(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_MAX_HISTORICAL_DAYS, -1);
  }

  public static void setKafkaBlacklistTopic(JobContext job, String val) {
    job.getConfiguration().set(KAFKA_BLACKLIST_TOPIC, val);
  }

  public static String[] getKafkaBlacklistTopic(JobContext job) {
    return getKafkaBlacklistTopic(job.getConfiguration());
  }

  public static String[] getKafkaBlacklistTopic(Configuration conf) {
    final String blacklistStr = conf.get(KAFKA_BLACKLIST_TOPIC);
    if (blacklistStr != null && !blacklistStr.isEmpty()) {
      return conf.getStrings(KAFKA_BLACKLIST_TOPIC);
    } else {
      return new String[] {};
    }
  }

  public static void setKafkaWhitelistTopic(JobContext job, String val) {
    job.getConfiguration().set(KAFKA_WHITELIST_TOPIC, val);
  }

  public static String[] getKafkaWhitelistTopic(JobContext job) {
    return getKafkaWhitelistTopic(job.getConfiguration());
  }

  public static String[] getKafkaWhitelistTopic(Configuration conf) {
    final String whitelistStr = conf.get(KAFKA_WHITELIST_TOPIC);
    if (whitelistStr != null && !whitelistStr.isEmpty()) {
      return conf.getStrings(KAFKA_WHITELIST_TOPIC);
    } else {
      return new String[] {};
    }
  }

  public static void setEtlIgnoreSchemaErrors(JobContext job, boolean val) {
    job.getConfiguration().setBoolean(ETL_IGNORE_SCHEMA_ERRORS, val);
  }

  public static boolean getEtlIgnoreSchemaErrors(JobContext job) {
    return job.getConfiguration().getBoolean(ETL_IGNORE_SCHEMA_ERRORS, false);
  }

  public static void setEtlAuditIgnoreServiceTopicList(JobContext job, String topics) {
    job.getConfiguration().set(ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST, topics);
  }

  public static String[] getEtlAuditIgnoreServiceTopicList(JobContext job) {
    return job.getConfiguration().getStrings(ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST, "");
  }

  public static void setMessageDecoderClass(JobContext job, Class<MessageDecoder> cls) {
    job.getConfiguration().setClass(CAMUS_MESSAGE_DECODER_CLASS, cls, MessageDecoder.class);
  }

  public static Class<MessageDecoder> getMessageDecoderClass(JobContext job) {
    return (Class<MessageDecoder>) job.getConfiguration().getClass(CAMUS_MESSAGE_DECODER_CLASS,
        KafkaAvroMessageDecoder.class);
  }

  public static Class<MessageDecoder> getMessageDecoderClass(JobContext job, String topicName) {
    Class<MessageDecoder> topicDecoder =
        (Class<MessageDecoder>) job.getConfiguration().getClass(CAMUS_MESSAGE_DECODER_CLASS + "." + topicName, null);
    return topicDecoder == null ? getMessageDecoderClass(job) : topicDecoder;
  }

  private class OffsetFileFilter implements PathFilter {

    @Override
    public boolean accept(Path arg0) {
      return arg0.getName().startsWith(EtlMultiOutputFormat.OFFSET_PREFIX);
    }
  }
}
