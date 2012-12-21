package com.linkedin.batch.etl.kafka;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import scala.actors.threadpool.Arrays;
import azkaban.common.utils.Props;

import com.linkedin.batch.etl.kafka.common.DateUtils;
import com.linkedin.batch.etl.kafka.common.EtlCounts;
import com.linkedin.batch.etl.kafka.common.EtlKey;
import com.linkedin.batch.etl.kafka.common.EtlZkClient;
import com.linkedin.batch.etl.kafka.mapred.AbstractHadoopJob;
import com.linkedin.batch.etl.kafka.mapred.EtlInputFormat;
import com.linkedin.batch.etl.kafka.mapred.EtlMapper;
import com.linkedin.batch.etl.kafka.mapred.EtlMultiOutputFormat;

public class EtlJob extends AbstractHadoopJob
{
  public static final String ETL_DESTINATION_PATH                   =
                                                                        "etl.destination.path";
  public static final String ETL_EXECUTION_BASE_PATH                =
                                                                        "etl.execution.base.path";
  public static final String ETL_EXECUTION_HISTORY_PATH             =
                                                                        "etl.execution.history.path";
  public static final String ETL_COUNTS_PATH                        = "etl.counts.path";
  public static final String ETL_HOURLY_PATH                        = "etl.hourly";
  public static final String ETL_DAILY_PATH                         = "etl.daily";
  public static final String ETL_RUN_MOVE_DATA                      = "etl.run.move.data";
  public static final String ETL_RUN_TRACKING_POST                  =
                                                                        "etl.run.tracking.post";
  public static final String ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST    =
                                                                        "etl.audit.ignore.service.topic.list";
  public static final String ETL_IGNORE_SCHEMA_ERRORS               =
                                                                        "etl.ignore.schema.errors";
  public static final String ETL_SCHEMA_REGISTRY_URL                =
                                                                        "etl.schema.registry.url";
  public static final String ETL_DEFAULT_TIMEZONE                   =
                                                                        "etl.default.timezone";
  public static final String ETL_DEFLATE_LEVEL                      = "etl.deflate.level";
  public static final String ETL_AVRO_WRITER_SYNC_INTERVAL          =
                                                                        "etl.avro.writer.sync.interval";
  public static final String ETL_OUTPUT_FILE_TIME_PARTITION_MINS    =
                                                                        "etl.output.file.time.partition.mins";
  public static final String ETL_KEEP_COUNT_FILES                   =
                                                                        "etl.keep.count.files";
  public static final String ETL_EXECUTION_HISTORY_MAX_OF_QUOTA     =
                                                                        "etl.execution.history.max.of.quota";

  public static final String HADOOP_EXECUTION_INPUT_PATH            = "mapred.input.dir";
  public static final String HADOOP_EXECUTION_OUTPUT_PATH           = "mapred.output.dir";
  public static final String HADOOP_NUM_TASKS                       = "mapred.map.tasks";

  public static final String KAFKA_MOVE_TO_LAST_OFFSET_LIST         =
                                                                        "kafka.move.to.last.offset.list";
  public static final String KAFKA_BLACKLIST_TOPIC                  =
                                                                        "kafka.blacklist.topics";
  public static final String KAFKA_WHITELIST_TOPIC                  =
                                                                        "kafka.whitelist.topics";
  public static final String KAFKA_CLIENT_BUFFER_SIZE               =
                                                                        "kafka.client.buffer.size";
  public static final String KAFKA_CLIENT_SO_TIMEOUT                =
                                                                        "kafka.client.so.timeout";
  public static final String KAFKA_START_TIMESTAMP                  =
                                                                        "kafka.start.timestamp";
  public static final String KAFKA_MAX_PULL_HRS                     =
                                                                        "kafka.max.pull.hrs";
  public static final String KAFKA_MAX_PULL_MINUTES_PER_TASK        =
                                                                        "kafka.max.pull.minutes.per.task";
  public static final String KAFKA_MAX_HISTORICAL_DAYS              =
                                                                        "kafka.max.historical.days";
  public static final String KAFKA_MONITOR_TIME_GRANULARITY_MS      =
                                                                        "kafka.monitor.time.granularity";
  public static final String KAFKA_MONITOR_TIER                     =
                                                                        "kafka.monitor.tier";
  public static final String KAFKA_MONITOR_ALTERNATE_FIELDS         =
                                                                        "kafka.monitor.alternate.fields";

  public static final String ZK_HOSTS                               = "zookeeper.hosts";
  public static final String ZK_AUDIT_HOSTS                         =
                                                                        "zookeeper.audit.hosts";
  public static final String ZK_TOPIC_PATH                          =
                                                                        "zookeeper.broker.topics";
  public static final String ZK_BROKER_PATH                         =
                                                                        "zookeeper.broker.nodes";
  public static final String ZK_SESSION_TIMEOUT                     =
                                                                        "zookeeper.session.timeout";
  public static final String ZK_CONNECTION_TIMEOUT                  =
                                                                        "zookeeper.connection.timeout";

  public static final String BROKER_URI_FILE                        = "brokers.uri";

  public static final String SCHEMA_REGISTRY_TYPE                   =
                                                                        "etl.kafka.schemaregistry.type";
  public static final String JDBC_SCHEMA_REGISTRY_USER              =
                                                                        "etl.kafka.schemaregistry.jdbc.user";
  public static final String JDBC_SCHEMA_REGISTRY_PASSWORD          =
                                                                        "etl.kafka.schemaregistry.jdbc.password";
  public static final String JDBC_SCHEMA_REGISTRY_URL               =
                                                                        "etl.kafka.schemaregistry.jdbc.url";
  public static final String JDBC_SCHEMA_REGISTRY_POOL_SIZE         =
                                                                        "etl.kafka.schemaregistry.jdbc.pool.size";
  public static final String JDBC_SCHEMA_REGISTRY_DRIVER            =
                                                                        "etl.kafka.schemaregistry.jdbc.driverClassName";
  public static final String SCHEMA_REGISTRY_VALIDATOR_CLASS_NAME   =
                                                                        "etl.kafka.schemaregistry.validator.class.name";
  public static final String SCHEMA_REGISTRY_IDGENERATOR_CLASS_NAME =
                                                                        "etl.kafka.schemaregistry.idgenerator.class.name";

  private final Props        props;

  public EtlJob(String id, Props props) throws IOException
  {
    super(id, props);
    this.props = props;
  }

  private static HashMap<String, Long> timingMap = new HashMap<String, Long>();

  public static void startTiming(String name)
  {
    timingMap.put(name,
                  (timingMap.get(name) == null ? 0 : timingMap.get(name))
                      - System.currentTimeMillis());
  }

  public static void stopTiming(String name)
  {
    timingMap.put(name,
                  (timingMap.get(name) == null ? 0 : timingMap.get(name))
                      + System.currentTimeMillis());
  }

  public static void setTime(String name)
  {
    timingMap.put(name,
                  (timingMap.get(name) == null ? 0 : timingMap.get(name))
                      + System.currentTimeMillis());
  }

  private void writeBrokers(FileSystem fs, Job job, List<URI> brokerURI) throws IOException
  {
    Path output = new Path(FileOutputFormat.getOutputPath(job), BROKER_URI_FILE);

    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(output)));

    for (URI u : brokerURI)
    {
      writer.write(u.toString());
      writer.newLine();
    }

    writer.close();
  }

  private List<URI> readBrokers(FileSystem fs, Job job) throws IOException,
      URISyntaxException
  {
    ArrayList<URI> brokerURI = new ArrayList<URI>();
    Path input = new Path(FileInputFormat.getInputPaths(job)[0], BROKER_URI_FILE);

    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(input)));

    String str;

    while ((str = reader.readLine()) != null)
    {
      brokerURI.add(new URI(str));
    }

    reader.close();

    return brokerURI;
  }

  @Override
  public void run() throws Exception
  {
    startTiming("pre-setup");
    startTiming("total");
    info("Starting Kafka ETL Job");

    Job job = super.createJobConf(EtlMapper.class);

    FileSystem fs = FileSystem.get(job.getConfiguration());

    info("The blacklisted topics: "
        + Arrays.toString(EtlInputFormat.getKafkaBlacklistTopic(job)));
    info("The whitelisted topics: "
        + Arrays.toString(EtlInputFormat.getKafkaWhitelistTopic(job)));
    info("Dir Destination set to: " + EtlMultiOutputFormat.getDestinationPath(job));

    info("Getting the base paths.");

    Path execBasePath = EtlInputFormat.getEtlExecutionBasePath(job);
    Path execHistory = EtlInputFormat.getEtlExecutionHistoryPath(job);

    info("Checking if they exist");
    // Check if the path exists and mention that the path was not there and has been
    // created
    // info("Deleting the base path");
    // fs.delete(execBasePath);
    // info("Deleting the history path");
    // fs.delete(execHistory);

    if (!fs.exists(execBasePath))
    {
      info("Creating the base path");
      System.out.println("The execution base path does not exist. Creating the directory");
      fs.mkdirs(execBasePath);
    }
    if (!fs.exists(execHistory))
    {
      info("Creating the history path");
      System.out.println("The history base path does not exist. Creating the directory.");
      fs.mkdirs(execHistory);
    }

    info("Finished creating the paths");
    ContentSummary content = fs.getContentSummary(execBasePath);
    long limit =
        (long) (content.getQuota() * props.getDouble(ETL_EXECUTION_HISTORY_MAX_OF_QUOTA,
                                                     .5));
    limit = limit == 0 ? 50000 : limit;
    long currentCount = content.getFileCount() + content.getDirectoryCount();

    Iterator<FileStatus> iter = Arrays.asList(fs.listStatus(execHistory)).iterator();

    while (limit < currentCount && iter.hasNext())
    {
      FileStatus stat = iter.next();

      System.out.println("removing old execution: " + stat.getPath().getName());
      ContentSummary execContent = fs.getContentSummary(stat.getPath());
      currentCount -= execContent.getFileCount() - execContent.getDirectoryCount();
      fs.delete(stat.getPath(), true);
    }

    FileStatus[] previousExecutions = fs.listStatus(execHistory);

    if (previousExecutions.length > 0)
    {
      Arrays.sort(previousExecutions);
      Path previous = previousExecutions[previousExecutions.length - 1].getPath();
      FileInputFormat.setInputPaths(job, previous);
      info("Previous execution: " + previous.toString());
    }
    else
    {
      info("No previous execution, all topics pulled from offset 0");
    }

    DateTimeFormatter dateFmt =
        DateUtils.getDateTimeFormatter("YYYY-MM-dd-HH-mm-ss", DateTimeZone.UTC);
    Path newExecutionOutput = new Path(execBasePath, new DateTime().toString(dateFmt));
    FileOutputFormat.setOutputPath(job, newExecutionOutput);
    info("New execution temp location: " + newExecutionOutput.toString());

    job.setInputFormatClass(EtlInputFormat.class);
    job.setOutputFormatClass(EtlMultiOutputFormat.class);
    job.setOutputKeyClass(EtlKey.class);
    job.setOutputValueClass(AvroWrapper.class);

    stopTiming("pre-setup");
    super.run(job, false);
    stopTiming("hadoop");

    startTiming("commit");
    HashMap<String, EtlCounts> countsMap = new HashMap<String, EtlCounts>();

    for (FileStatus f : fs.listStatus(newExecutionOutput,
                                      new PrefixFilter(EtlMultiOutputFormat.COUNTS_PREFIX)))
    {
      String topic = f.getPath().getName().split("\\.")[1];

      if (!countsMap.containsKey(topic))
      {
        countsMap.put(topic,
                      new EtlCounts(topic,
                                    EtlMultiOutputFormat.getMonitorTimeGranularityMins(job) * 60 * 1000L));
      }

      countsMap.get(topic).loadStreamingCountsFromDir(fs, f.getPath(), true);

      if (EtlMultiOutputFormat.getEtlKeepCountFiles(job))
      {
        fs.rename(f.getPath(), new Path(EtlMultiOutputFormat.getCountsPath(job),
                                        f.getPath().getName()));
      }
      else
      {
        fs.delete(f.getPath(), true);
      }
    }

    List<URI> brokerURI;
    try
    {
      EtlZkClient zkClient =
          new EtlZkClient(EtlMultiOutputFormat.getZkAuditHosts(job),
                          EtlInputFormat.getZkSessionTimeout(job),
                          EtlInputFormat.getZkConnectionTimeout(job),
                          EtlInputFormat.getZkTopicPath(job),
                          EtlInputFormat.getZkBrokerPath(job));
      brokerURI = new ArrayList<URI>(zkClient.getBrokersToUriMap().values());
    }
    catch (Exception e)
    {
      error("Can't get brokers from zookeeper, using previously found brokers");
      brokerURI = readBrokers(fs, job);
    }

    writeBrokers(fs, job, brokerURI);

    for (EtlCounts count : countsMap.values())
    {
      count.postTrackingCountToKafka(EtlMultiOutputFormat.getMonitorTier(job), brokerURI);
    }

    // for (FileStatus f : fs.listStatus(newExecutionOutput, new
    // PrefixFilter(EtlMultiOutputFormat.ERRORS_PREFIX))){
    // SequenceFile.Reader reader = new SequenceFile.Reader(fs, f.getPath(),
    // fs.getConf());
    //
    // EtlKey key = new EtlKey();
    // ExceptionWritable value = new ExceptionWritable();
    //
    // while (reader.next(key, value)){
    // error(key.toString());
    // error(value.toString());
    // }
    // reader.close();
    // }

    if (job.getConfiguration().getBoolean(EtlJob.ETL_RUN_MOVE_DATA, false))
    {
      fs.rename(newExecutionOutput, execHistory);
    }
    info("Job finished");

    stopTiming("commit");

    stopTiming("total");
    createReport(job, timingMap);

    if (!job.isSuccessful())
    {
      JobClient client = new JobClient(new JobConf(job.getConfiguration()));

      TaskCompletionEvent[] tasks = job.getTaskCompletionEvents(0);

      for (TaskReport task : client.getMapTaskReports(tasks[0].getTaskAttemptId()
                                                              .getJobID()))
      {
        if (task.getCurrentStatus().equals(TIPStatus.FAILED))
        {
          for (String s : task.getDiagnostics())
          {
            error("task error: " + s);
          }
        }
      }
      throw new RuntimeException("hadoop job failed");
    }
  }

  private void createReport(Job job, Map<String, Long> timingMap) throws IOException
  {
    StringBuilder sb = new StringBuilder();

    sb.append("***********Timing Report*************\n");

    sb.append("Job time (seconds):\n");

    double preSetup = timingMap.get("pre-setup") / 1000;
    double getSplits = timingMap.get("getSplits") / 1000;
    double hadoop = timingMap.get("hadoop") / 1000;
    double commit = timingMap.get("commit") / 1000;
    double total = timingMap.get("total") / 1000;

    sb.append(String.format("    %12s %6.1f (%s)\n",
                            "pre setup",
                            preSetup,
                            NumberFormat.getPercentInstance()
                                        .format(preSetup / total)
                                        .toString()));
    sb.append(String.format("    %12s %6.1f (%s)\n",
                            "get splits",
                            getSplits,
                            NumberFormat.getPercentInstance()
                                        .format(getSplits / total)
                                        .toString()));
    sb.append(String.format("    %12s %6.1f (%s)\n",
                            "hadoop job",
                            hadoop,
                            NumberFormat.getPercentInstance()
                                        .format(hadoop / total)
                                        .toString()));
    sb.append(String.format("    %12s %6.1f (%s)\n",
                            "commit",
                            commit,
                            NumberFormat.getPercentInstance()
                                        .format(commit / total)
                                        .toString()));

    int minutes = (int) total / 60;
    int seconds = (int) total % 60;

    sb.append(String.format("Total: %d minutes %d seconds\n", minutes, seconds));

    JobClient client = new JobClient(new JobConf(job.getConfiguration()));

    TaskReport[] tasks = client.getMapTaskReports(JobID.downgrade(super.getJobID()));

    double min = Long.MAX_VALUE, max = 0, mean = 0;
    double minRun = Long.MAX_VALUE, maxRun = 0, meanRun = 0;
    long totalTaskTime = 0;
    TreeMap<Long, List<TaskReport>> taskMap = new TreeMap<Long, List<TaskReport>>();

    for (TaskReport t : tasks)
    {
      long wait = t.getStartTime() - timingMap.get("hadoop_start");
      min = wait < min ? wait : min;
      max = wait > max ? wait : max;
      mean += wait;

      long runTime = t.getFinishTime() - t.getStartTime();
      totalTaskTime += runTime;
      minRun = runTime < minRun ? runTime : minRun;
      maxRun = runTime > maxRun ? runTime : maxRun;
      meanRun += runTime;

      if (!taskMap.containsKey(runTime))
      {
        taskMap.put(runTime, new ArrayList<TaskReport>());
      }
      taskMap.get(runTime).add(t);
    }

    mean /= tasks.length;
    meanRun /= tasks.length;

    // convert to seconds
    min /= 1000;
    max /= 1000;
    mean /= 1000;
    minRun /= 1000;
    maxRun /= 1000;
    meanRun /= 1000;

    sb.append("\nHadoop job task times (seconds):\n");
    sb.append(String.format("    %12s %6.1f\n", "min", minRun));
    sb.append(String.format("    %12s %6.1f\n", "mean", meanRun));
    sb.append(String.format("    %12s %6.1f\n", "max", maxRun));
    sb.append(String.format("    %12s %6.1f/%.1f = %.2f\n",
                            "skew",
                            meanRun,
                            maxRun,
                            meanRun / maxRun));

    sb.append("\nTask wait time (seconds):\n");
    sb.append(String.format("    %12s %6.1f\n", "min", min));
    sb.append(String.format("    %12s %6.1f\n", "mean", mean));
    sb.append(String.format("    %12s %6.1f\n", "max", max));

    CounterGroup totalGrp = job.getCounters().getGroup("total");

    long decode = totalGrp.findCounter("decode-time(ms)").getValue();
    long request = totalGrp.findCounter("request-time(ms)").getValue();
    long map = totalGrp.findCounter("mapper-time(ms)").getValue();
    long mb = totalGrp.findCounter("data-read").getValue();

    long other = totalTaskTime - map - request - decode;

    sb.append("\nHadoop task breakdown:\n");
    sb.append(String.format("    %12s %s\n",
                            "kafka",
                            NumberFormat.getPercentInstance().format(request
                                / (double) totalTaskTime)));
    sb.append(String.format("    %12s %s\n",
                            "decode",
                            NumberFormat.getPercentInstance().format(decode
                                / (double) totalTaskTime)));
    sb.append(String.format("    %12s %s\n",
                            "map output",
                            NumberFormat.getPercentInstance().format(map
                                / (double) totalTaskTime)));
    sb.append(String.format("    %12s %s\n",
                            "other",
                            NumberFormat.getPercentInstance().format(other
                                / (double) totalTaskTime)));

    sb.append(String.format("\n%16s %s\n", "Total MB read:", mb / 1024 / 1024));

    info(sb.toString());
  }

  /**
   * Path filter that filters based on prefix
   */
  private class PrefixFilter implements PathFilter
  {
    private final String prefix;

    public PrefixFilter(String prefix)
    {
      this.prefix = prefix;
    }

    public boolean accept(Path path)
    {
      // TODO Auto-generated method stub
      return path.getName().startsWith(prefix);
    }
  }
}
