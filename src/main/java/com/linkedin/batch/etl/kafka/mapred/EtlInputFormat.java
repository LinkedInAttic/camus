package com.linkedin.batch.etl.kafka.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.mapred.AvroWrapper;
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

import scala.actors.threadpool.Arrays;

import com.linkedin.batch.etl.kafka.EtlJob;
import com.linkedin.batch.etl.kafka.common.EtlKey;
import com.linkedin.batch.etl.kafka.common.EtlRequest;
import com.linkedin.batch.etl.kafka.common.EtlZkClient;
import com.linkedin.batch.etl.kafka.schemaregistry.SchemaDetails;
import com.linkedin.batch.etl.kafka.schemaregistry.SchemaRegistry;

/**
 * Input format for a Kafka pull job.
 */
@SuppressWarnings("deprecation")
public class EtlInputFormat extends InputFormat<EtlKey, AvroWrapper<Object>>
{
	
  private final Logger _log = Logger.getLogger(getClass());

  @Override
  public RecordReader<EtlKey, AvroWrapper<Object>> createRecordReader(InputSplit split,
                                                                      TaskAttemptContext context) throws IOException,
      InterruptedException
  {
    return new EtlRecordReader(split, context);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException
  {
    EtlJob.startTiming("getSplits");
    
    _log.info("Beginning of getSplits()");
    
    List<EtlRequest> requests;
    try
    {
      // Create the zkClient for kafka.
      String zkHosts = getZkHosts(context);
      String zkTopicPath = getZkTopicPath(context);
      String zkBrokerPath = getZkBrokerPath(context);
      int zkSessionTimeout = getZkSessionTimeout(context);
      int zkConnectionTimeout = getZkConnectionTimeout(context);
      
      _log.info("Stuff passed into the EtlZkClient constructor:\n\tzkHosts: " + zkHosts + 
    		  "\n\tzkTopicPath: " + zkTopicPath +
    		  "\n\tzkBrokerPath: " + zkBrokerPath + 
    		  "\n\tzkSessionTimeout: " + zkSessionTimeout + 
    		  "\n\tzkConnectionTimeout: " + zkConnectionTimeout);
      
      EtlZkClient zkClient =
          new EtlZkClient(zkHosts,
                          zkSessionTimeout,
                          zkConnectionTimeout,
                          zkTopicPath,
                          zkBrokerPath);
      
      _log.info("zkClient.toString(): " + zkClient.toString());

      List<String> topicList = null;
      
      Set<String> whiteListTopics =
          new HashSet<String>(Arrays.asList(getKafkaWhitelistTopic(context)));

      _log.info("whiteListTopics: " + whiteListTopics);

      Set<String> blackListTopics =
          new HashSet<String>(Arrays.asList(getKafkaBlacklistTopic(context)));

      _log.info("blackListTopics: " + blackListTopics);
      
      if (whiteListTopics.isEmpty())
      {
        _log.info("whiteListTopics is empty!");

        EtlJob.startTiming("kafkaSetupTime");
        topicList = zkClient.getTopics(blackListTopics);
        EtlJob.stopTiming("kafkaSetupTime");
      }
      else
      {
        EtlJob.startTiming("kafkaSetupTime");
        topicList = zkClient.getTopics(whiteListTopics, blackListTopics);
        EtlJob.stopTiming("kafkaSetupTime");
      }
      
      _log.info("topicList: " + topicList);

      _log.info("Number of topics pulled from Zookeeper: " + topicList.size());

      //Get the class name of the concrete implementation of the Schema Registry and get the concrete class implemented
      
      String registryType = EtlInputFormat.getSchemaRegistryType(context);
      
      _log.info("registryType: " + registryType);
      
      SchemaRegistry registry = (SchemaRegistry)Class.forName(registryType).newInstance(); 
 
      _log.info("registry.toString(): " + registry.toString());

      //SchemaRepository registry = SchemaRegistryClient.getInstance(context);
      requests = new ArrayList<EtlRequest>();

      for (String topic : topicList)
      {
        try
        {	
          // SchemaAndId schemaStr = registry.lookupLatest(topic);

          SchemaDetails schemaStr = registry.getLatestSchemaByTopic(topic);
          if (!schemaStr.getSchema().startsWith("<html>"))
          {
            EtlJob.startTiming("kafkaSetupTime");
            requests.addAll(zkClient.getKafkaRequest(topic));
            EtlJob.stopTiming("kafkaSetupTime");
          }
          else
          {
            _log.info(topic + " does not have a registered schema, skipping");
          }
        }
        catch (Exception e)
        {
          _log.error("Topic " + topic + " had error during setup."
              + e.getMessage());
          for (StackTraceElement s : e.getStackTrace())
          {
            _log.error(s.toString());
          }
          continue;
        }
      }
    }
    catch (Exception e)
    {
      _log.error("Unable to pull requests from zookeeper, using previous requests", e);
      requests = getPreviousRequests(context);
    }

    writeRequests(requests, context);
    Map<EtlRequest, EtlKey> offsetKeys =
        getPreviousOffsets(FileInputFormat.getInputPaths(context), context);
    Set<String> moveLatest = getMoveToLatestTopicsSet(context);

    for (EtlRequest request : requests)
    {
      if (moveLatest.contains(request.getTopic()) || moveLatest.contains("all"))
      {
        offsetKeys.put(request, new EtlKey(request.getTopic(),
                                           request.getNodeId(),
                                           request.getPartition(),
                                           0,
                                           request.getLastOffset()));
      }

      EtlKey key = offsetKeys.get(request);
      if (key != null)
      {
        request.setOffset(key.getOffset());
      }

      if (request.getEarliestOffset() > request.getOffset())
      {
        request.setOffset(request.getEarliestOffset());
      }

      _log.info(request);
    }

    writePrevious(offsetKeys.values(), context);

    EtlJob.stopTiming("getSplits");
    EtlJob.startTiming("hadoop");
    EtlJob.setTime("hadoop_start");

    return allocateWork(requests, context);
  }

  private Set<String> getMoveToLatestTopicsSet(JobContext context)
  {
    Set<String> topics = new HashSet<String>();

    String[] arr = getMoveToLatestTopics(context);

    if (arr != null)
    {
      for (String topic : arr)
      {
        topics.add(topic);
      }
    }

    return topics;
  }

  private List<InputSplit> allocateWork(List<EtlRequest> requests, JobContext context) throws IOException
  {
    int numTasks = context.getConfiguration().getInt("mapred.map.tasks", 30);
    // Reverse sort by size
    Collections.sort(requests, new Comparator<EtlRequest>()
    {
      @Override
      public int compare(EtlRequest o1, EtlRequest o2)
      {
        if (o2.estimateDataSize() == o1.estimateDataSize())
        {
          return 0;
        }
        if (o2.estimateDataSize() < o1.estimateDataSize())
        {
          return -1;
        }
        else
        {
          return 1;
        }
      }
    });

    List<InputSplit> kafkaETLSplits = new ArrayList<InputSplit>();

    for (int i = 0; i < numTasks; i++)
    {
      EtlSplit split = new EtlSplit();

      if (requests.size() > 0)
      {
        split.addRequest(requests.get(0));
        kafkaETLSplits.add(split);
        requests.remove(0);
      }
    }

    for (EtlRequest r : requests)
    {
      getSmallestMultiSplit(kafkaETLSplits).addRequest(r);
    }

    return kafkaETLSplits;
  }

  private EtlSplit getSmallestMultiSplit(List<InputSplit> kafkaETLSplits) throws IOException
  {
    EtlSplit smallest = (EtlSplit) kafkaETLSplits.get(0);

    for (int i = 1; i < kafkaETLSplits.size(); i++)
    {
      EtlSplit challenger = (EtlSplit) kafkaETLSplits.get(i);
      if ((smallest.getLength() == challenger.getLength() && smallest.getNumRequests() > challenger.getNumRequests())
          || smallest.getLength() > challenger.getLength())
      {
        smallest = challenger;
      }
    }

    return smallest;
  }

  private void writePrevious(Collection<EtlKey> missedKeys, JobContext context) throws IOException
  {
    FileSystem fs = FileSystem.get(context.getConfiguration());
    Path output = FileOutputFormat.getOutputPath(context);

    if (fs.exists(output))
    {
      fs.mkdirs(output);
    }

    output = new Path(output, EtlMultiOutputFormat.OFFSET_PREFIX + "-previous");
    SequenceFile.Writer writer =
        SequenceFile.createWriter(fs,
                                  context.getConfiguration(),
                                  output,
                                  EtlKey.class,
                                  NullWritable.class);

    for (EtlKey key : missedKeys)
    {
      writer.append(key, NullWritable.get());
    }

    writer.close();
  }

  private void writeRequests(List<EtlRequest> requests, JobContext context) throws IOException
  {
    FileSystem fs = FileSystem.get(context.getConfiguration());
    Path output = FileOutputFormat.getOutputPath(context);

    if (fs.exists(output))
    {
      fs.mkdirs(output);
    }

    output = new Path(output, EtlMultiOutputFormat.REQUESTS_FILE);
    SequenceFile.Writer writer =
        SequenceFile.createWriter(fs,
                                  context.getConfiguration(),
                                  output,
                                  EtlRequest.class,
                                  NullWritable.class);

    for (EtlRequest r : requests)
    {
      writer.append(r, NullWritable.get());
    }
    writer.close();
  }

  private List<EtlRequest> getPreviousRequests(JobContext context) throws IOException
  {
    List<EtlRequest> requests = new ArrayList<EtlRequest>();
    FileSystem fs = FileSystem.get(context.getConfiguration());
    Path input = FileInputFormat.getInputPaths(context)[0];

    input = new Path(input, EtlMultiOutputFormat.REQUESTS_FILE);
    SequenceFile.Reader reader =
        new SequenceFile.Reader(fs, input, context.getConfiguration());

    EtlRequest request = new EtlRequest();
    while (reader.next(request, NullWritable.get()))
    {
      requests.add(new EtlRequest(request));
    }

    reader.close();
    return requests;
  }

  private Map<EtlRequest, EtlKey> getPreviousOffsets(Path[] inputs, JobContext context) throws IOException
  {
    Map<EtlRequest, EtlKey> offsetKeysMap = new HashMap<EtlRequest, EtlKey>();
    for (Path input : inputs)
    {
      FileSystem fs = input.getFileSystem(context.getConfiguration());
      for (FileStatus f : fs.listStatus(input, new OffsetFileFilter()))
      {
        _log.info("previous offset file:" + f.getPath().toString());
        SequenceFile.Reader reader =
            new SequenceFile.Reader(fs, f.getPath(), context.getConfiguration());
        EtlKey key = new EtlKey();
        while (reader.next(key, NullWritable.get()))
        {
          EtlRequest request =
              new EtlRequest(key.getTopic(), key.getNodeId(), key.getPartition());

          if (offsetKeysMap.containsKey(request))
          {
            EtlKey oldKey = offsetKeysMap.get(request);
            if (oldKey.getOffset() < key.getOffset())
            {
              offsetKeysMap.put(request, key);
            }
          }
          else
          {
            offsetKeysMap.put(request, key);
          }
          key = new EtlKey();
        }
        reader.close();
      }
    }

    return offsetKeysMap;
  }

  public static void setMonitorAlternateFields(JobContext job, String val)
  {
    job.getConfiguration().set(EtlJob.KAFKA_MONITOR_ALTERNATE_FIELDS, val);
  }

  public static String[] getMonitorAlternateFields(JobContext job)
  {
    return job.getConfiguration().getStrings(EtlJob.KAFKA_MONITOR_ALTERNATE_FIELDS);
  }

  public static void setMoveToLatestTopics(JobContext job, String val)
  {
    job.getConfiguration().set(EtlJob.KAFKA_MOVE_TO_LAST_OFFSET_LIST, val);
  }

  public static String[] getMoveToLatestTopics(JobContext job)
  {
    return job.getConfiguration().getStrings(EtlJob.KAFKA_MOVE_TO_LAST_OFFSET_LIST);
  }

  public static void setKafkaClientBufferSize(JobContext job, int val)
  {
    job.getConfiguration().setInt(EtlJob.KAFKA_CLIENT_BUFFER_SIZE, val);
  }

  public static int getKafkaClientBufferSize(JobContext job)
  {
    return job.getConfiguration()
              .getInt(EtlJob.KAFKA_CLIENT_BUFFER_SIZE, 2 * 1024 * 1024);
  }

  public static void setKafkaClientTimeout(JobContext job, int val)
  {
    job.getConfiguration().setInt(EtlJob.KAFKA_CLIENT_SO_TIMEOUT, val);
  }

  public static int getKafkaClientTimeout(JobContext job)
  {
    return job.getConfiguration().getInt(EtlJob.KAFKA_CLIENT_SO_TIMEOUT, 60000);
  }

  public static void setKafkaStartTimeMs(JobContext job, long val)
  {
    job.getConfiguration().setLong(EtlJob.KAFKA_START_TIMESTAMP, val);
  }

  public static Long getKafkaStartTimeMs(JobContext job)
  {
    return job.getConfiguration().getLong(EtlJob.KAFKA_START_TIMESTAMP, -1);
  }

  public static void setKafkaMaxPullHrs(JobContext job, int val)
  {
    job.getConfiguration().setInt(EtlJob.KAFKA_MAX_PULL_HRS, val);
  }

  public static int getKafkaMaxPullHrs(JobContext job)
  {
    return job.getConfiguration().getInt(EtlJob.KAFKA_MAX_PULL_HRS, -1);
  }

  public static void setKafkaMaxPullMinutesPerTask(JobContext job, int val)
  {
    job.getConfiguration().setInt(EtlJob.KAFKA_MAX_PULL_MINUTES_PER_TASK, val);
  }

  public static int getKafkaMaxPullMinutesPerTask(JobContext job)
  {
    return job.getConfiguration().getInt(EtlJob.KAFKA_MAX_PULL_MINUTES_PER_TASK, -1);
  }

  public static void setKafkaMaxHistoricalDays(JobContext job, int val)
  {
    job.getConfiguration().setInt(EtlJob.KAFKA_MAX_HISTORICAL_DAYS, val);
  }

  public static int getKafkaMaxHistoricalDays(JobContext job)
  {
    return job.getConfiguration().getInt(EtlJob.KAFKA_MAX_HISTORICAL_DAYS, -1);
  }

  public static void setKafkaBlacklistTopic(JobContext job, String val)
  {
    job.getConfiguration().set(EtlJob.KAFKA_BLACKLIST_TOPIC, val);
  }

  public static String[] getKafkaBlacklistTopic(JobContext job)
  {
    return job.getConfiguration().getStrings(EtlJob.KAFKA_BLACKLIST_TOPIC);
  }

  public static void setKafkaWhitelistTopic(JobContext job, String val)
  {
    job.getConfiguration().set(EtlJob.KAFKA_WHITELIST_TOPIC, val);
  }

  public static String[] getKafkaWhitelistTopic(JobContext job)
  {
    if (job.getConfiguration().get(EtlJob.KAFKA_WHITELIST_TOPIC) != null
        && !job.getConfiguration().get(EtlJob.KAFKA_WHITELIST_TOPIC).isEmpty())
    {
      return job.getConfiguration().getStrings(EtlJob.KAFKA_WHITELIST_TOPIC);
    }
    else
    {
      return new String[] {};
    }
  }

  public static void setZkHosts(JobContext job, String val)
  {
    job.getConfiguration().set(EtlJob.ZK_HOSTS, val);
  }

  public static String getZkHosts(JobContext job)
  {
    return job.getConfiguration().get(EtlJob.ZK_HOSTS);
  }

  public static void setZkTopicPath(JobContext job, String val)
  {
    job.getConfiguration().set(EtlJob.ZK_TOPIC_PATH, val);
  }

  public static String getZkTopicPath(JobContext job)
  {
    return job.getConfiguration().get(EtlJob.ZK_TOPIC_PATH);
  }

  public static void setZkBrokerPath(JobContext job, String val)
  {
    job.getConfiguration().set(EtlJob.ZK_BROKER_PATH, val);
  }

  public static String getZkBrokerPath(JobContext job)
  {
    return job.getConfiguration().get(EtlJob.ZK_BROKER_PATH);
  }

  public static void setZkSessionTimeout(JobContext job, int val)
  {
    job.getConfiguration().setInt(EtlJob.ZK_SESSION_TIMEOUT, val);
  }

  public static int getZkSessionTimeout(JobContext job)
  {
    return job.getConfiguration().getInt(EtlJob.ZK_SESSION_TIMEOUT,
                                         EtlZkClient.DEFAULT_ZOOKEEPER_TIMEOUT);
  }

  public static void setZkConnectionTimeout(JobContext job, int val)
  {
    job.getConfiguration().setInt(EtlJob.ZK_CONNECTION_TIMEOUT, val);
  }

  public static int getZkConnectionTimeout(JobContext job)
  {
    return job.getConfiguration().getInt(EtlJob.ZK_CONNECTION_TIMEOUT,
                                         EtlZkClient.DEFAULT_ZOOKEEPER_TIMEOUT);
  }

  public static void setEtlExecutionBasePath(JobContext job, Path val)
  {
    job.getConfiguration().set(EtlJob.ETL_EXECUTION_BASE_PATH, val.toString());
  }

  public static Path getEtlExecutionBasePath(JobContext job)
  {
    return new Path(job.getConfiguration().get(EtlJob.ETL_EXECUTION_BASE_PATH));
  }

  public static void setEtlExecutionHistoryPath(JobContext job, Path val)
  {
    job.getConfiguration().set(EtlJob.ETL_EXECUTION_HISTORY_PATH, val.toString());
  }

  public static Path getEtlExecutionHistoryPath(JobContext job)
  {
    return new Path(job.getConfiguration().get(EtlJob.ETL_EXECUTION_HISTORY_PATH));
  }

  public static void setEtlIgnoreSchemaErrors(JobContext job, boolean val)
  {
    job.getConfiguration().setBoolean(EtlJob.ETL_IGNORE_SCHEMA_ERRORS, val);
  }

  public static boolean getEtlIgnoreSchemaErrors(JobContext job)
  {
    return job.getConfiguration().getBoolean(EtlJob.ETL_IGNORE_SCHEMA_ERRORS, false);
  }

  public static void setEtlSchemaRegistryUrl(JobContext job, String val)
  {
    job.getConfiguration().set(EtlJob.ETL_SCHEMA_REGISTRY_URL, val);
  }

  public static String getEtlSchemaRegistryUrl(JobContext job)
  {
    return job.getConfiguration().get(EtlJob.ETL_SCHEMA_REGISTRY_URL);
  }

  public static void setEtlDefaultTimezone(JobContext job, String val)
  {
    job.getConfiguration().set(EtlJob.ETL_DEFAULT_TIMEZONE, val);
  }

  public static String getEtlDefaultTimezone(JobContext job)
  {
    return job.getConfiguration().get(EtlJob.ETL_DEFAULT_TIMEZONE);
  }

  public static String getSchemaRegistryType(JobContext job)
  {
    return job.getConfiguration().get(EtlJob.SCHEMA_REGISTRY_TYPE);
  }

  public static String getJDBCSchemaRegistryURL(JobContext job)
  {
    return job.getConfiguration().get(EtlJob.JDBC_SCHEMA_REGISTRY_URL);
  }

  public static String getJDBCSchemaRegistryUser(JobContext job)
  {
    return job.getConfiguration().get(EtlJob.JDBC_SCHEMA_REGISTRY_USER);
  }

  public static String getJDBCSchemaRegistryDriver(JobContext job)
  {
    return job.getConfiguration().get(EtlJob.JDBC_SCHEMA_REGISTRY_DRIVER);
  }

  public static String getJDBCSchemaRegistryPassword(JobContext job)
  {
    return job.getConfiguration().get(EtlJob.JDBC_SCHEMA_REGISTRY_PASSWORD);
  }

  public static String getValidatorClassName(JobContext job)
  {
    return job.getConfiguration().get(EtlJob.SCHEMA_REGISTRY_VALIDATOR_CLASS_NAME);
  }

  public static String getIdGeneratorClassName(JobContext job)
  {
    return job.getConfiguration().get(EtlJob.SCHEMA_REGISTRY_IDGENERATOR_CLASS_NAME);
  }

  public static String getEtlSchemaRegistryPoolSize(JobContext job)
  {
    return job.getConfiguration().get(EtlJob.JDBC_SCHEMA_REGISTRY_POOL_SIZE);
  }

  private class OffsetFileFilter implements PathFilter
  {

    @Override
    public boolean accept(Path arg0)
    {
      return arg0.getName().startsWith(EtlMultiOutputFormat.OFFSET_PREFIX);
    }
  }
}
