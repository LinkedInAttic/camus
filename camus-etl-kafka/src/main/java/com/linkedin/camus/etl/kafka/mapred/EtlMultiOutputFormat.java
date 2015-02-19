package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.camus.etl.Partitioner;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.AvroRecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.partitioner.DefaultPartitioner;


/**
 * MultipleAvroOutputFormat.
 *
 * File names are determined by output keys.
 */

public class EtlMultiOutputFormat extends FileOutputFormat<EtlKey, Object> {
  public static final String ETL_DESTINATION_PATH = "etl.destination.path";
  public static final String ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY = "etl.destination.path.topic.sub.dir";
  public static final String ETL_DESTINATION_PATH_TOPIC_SUBDIRFORMAT = "etl.destination.path.topic.sub.dirformat";
  public static final String ETL_DESTINATION_PATH_TOPIC_SUBDIRFORMAT_LOCALE = "etl.destination.path.topic.sub.dirformat.locale";
  public static final String ETL_RUN_MOVE_DATA = "etl.run.move.data";
  public static final String ETL_RUN_TRACKING_POST = "etl.run.tracking.post";

  public static final String ETL_DEFAULT_TIMEZONE = "etl.default.timezone";
  public static final String ETL_DEFLATE_LEVEL = "etl.deflate.level";
  public static final String ETL_AVRO_WRITER_SYNC_INTERVAL = "etl.avro.writer.sync.interval";
  public static final String ETL_OUTPUT_FILE_TIME_PARTITION_MINS = "etl.output.file.time.partition.mins";

  public static final String KAFKA_MONITOR_TIME_GRANULARITY_MS = "kafka.monitor.time.granularity";
  public static final String ETL_DEFAULT_PARTITIONER_CLASS = "etl.partitioner.class";
  public static final String ETL_OUTPUT_CODEC = "etl.output.codec";
  public static final String ETL_DEFAULT_OUTPUT_CODEC = "deflate";
  public static final String ETL_RECORD_WRITER_PROVIDER_CLASS = "etl.record.writer.provider.class";

  public static final DateTimeFormatter FILE_DATE_FORMATTER = DateUtils.getDateTimeFormatter("YYYYMMddHH");
  public static final String OFFSET_PREFIX = "offsets";
  public static final String ERRORS_PREFIX = "errors";
  public static final String COUNTS_PREFIX = "counts";

  public static final String REQUESTS_FILE = "requests.previous";
  private static EtlMultiOutputCommitter committer = null;
  private static Map<String, Partitioner> partitionersByTopic = new HashMap<String, Partitioner>();

  private static Logger log = Logger.getLogger(EtlMultiOutputFormat.class);

  @Override
  public RecordWriter<EtlKey, Object> getRecordWriter(TaskAttemptContext context) throws IOException,
      InterruptedException {
    if (committer == null)
      committer = new EtlMultiOutputCommitter(getOutputPath(context), context, log);
    return new EtlMultiOutputRecordWriter(context, committer);
  }

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    if (committer == null)
      committer = new EtlMultiOutputCommitter(getOutputPath(context), context, log);
    return committer;
  }

  public static void setRecordWriterProviderClass(JobContext job, Class<RecordWriterProvider> recordWriterProviderClass) {
    job.getConfiguration().setClass(ETL_RECORD_WRITER_PROVIDER_CLASS, recordWriterProviderClass,
        RecordWriterProvider.class);
  }

  public static Class<RecordWriterProvider> getRecordWriterProviderClass(JobContext job) {
    return (Class<RecordWriterProvider>) job.getConfiguration().getClass(ETL_RECORD_WRITER_PROVIDER_CLASS,
        AvroRecordWriterProvider.class);
  }

  public static RecordWriterProvider getRecordWriterProvider(JobContext job) {
    try {
      return (RecordWriterProvider) job.getConfiguration()
          .getClass(ETL_RECORD_WRITER_PROVIDER_CLASS, AvroRecordWriterProvider.class).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void setDefaultTimeZone(JobContext job, String tz) {
    job.getConfiguration().set(ETL_DEFAULT_TIMEZONE, tz);
  }

  public static String getDefaultTimeZone(JobContext job) {
    return job.getConfiguration().get(ETL_DEFAULT_TIMEZONE, "America/Los_Angeles");
  }

  public static void setDestinationPath(JobContext job, Path dest) {
    job.getConfiguration().set(ETL_DESTINATION_PATH, dest.toString());
  }

  public static Path getDestinationPath(JobContext job) {
    return new Path(job.getConfiguration().get(ETL_DESTINATION_PATH));
  }

  public static void setDestPathTopicSubDir(JobContext job, String subPath) {
    job.getConfiguration().set(ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY, subPath);
  }

  public static Path getDestPathTopicSubDir(JobContext job) {
    return new Path(job.getConfiguration().get(ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY, "hourly"));
  }

  public static void setMonitorTimeGranularityMins(JobContext job, int mins) {
    job.getConfiguration().setInt(KAFKA_MONITOR_TIME_GRANULARITY_MS, mins);
  }

  public static int getMonitorTimeGranularityMins(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_MONITOR_TIME_GRANULARITY_MS, 10);
  }

  public static long getMonitorTimeGranularityMs(JobContext job) {
    return job.getConfiguration().getInt(KAFKA_MONITOR_TIME_GRANULARITY_MS, 10) * 60000L;
  }

  public static void setEtlAvroWriterSyncInterval(JobContext job, int val) {
    job.getConfiguration().setInt(ETL_AVRO_WRITER_SYNC_INTERVAL, val);
  }

  public static int getEtlAvroWriterSyncInterval(JobContext job) {
    return job.getConfiguration().getInt(ETL_AVRO_WRITER_SYNC_INTERVAL, 16000);
  }

  public static void setEtlDeflateLevel(JobContext job, int val) {
    job.getConfiguration().setInt(ETL_DEFLATE_LEVEL, val);
  }

  public static void setEtlOutputCodec(JobContext job, String codec) {
    job.getConfiguration().set(ETL_OUTPUT_CODEC, codec);
  }

  public static String getEtlOutputCodec(JobContext job) {
    return job.getConfiguration().get(ETL_OUTPUT_CODEC, ETL_DEFAULT_OUTPUT_CODEC);

  }

  public static int getEtlDeflateLevel(JobContext job) {
    return job.getConfiguration().getInt(ETL_DEFLATE_LEVEL, 6);
  }

  public static int getEtlOutputFileTimePartitionMins(JobContext job) {
    return job.getConfiguration().getInt(ETL_OUTPUT_FILE_TIME_PARTITION_MINS, 60);
  }

  public static void setEtlOutputFileTimePartitionMins(JobContext job, int val) {
    job.getConfiguration().setInt(ETL_OUTPUT_FILE_TIME_PARTITION_MINS, val);
  }

  public static boolean isRunMoveData(JobContext job) {
    return job.getConfiguration().getBoolean(ETL_RUN_MOVE_DATA, true);
  }

  public static void setRunMoveData(JobContext job, boolean value) {
    job.getConfiguration().setBoolean(ETL_RUN_MOVE_DATA, value);
  }

  public static boolean isRunTrackingPost(JobContext job) {
    return job.getConfiguration().getBoolean(ETL_RUN_TRACKING_POST, false);
  }

  public static void setRunTrackingPost(JobContext job, boolean value) {
    job.getConfiguration().setBoolean(ETL_RUN_TRACKING_POST, value);
  }

  public static String getWorkingFileName(JobContext context, EtlKey key) throws IOException {
    Partitioner partitioner = getPartitioner(context, key.getTopic());
    return partitioner.getWorkingFileName(context, key.getTopic(), key.getLeaderId(), key.getPartition(),
        partitioner.encodePartition(context, key));
  }

  public static void setDefaultPartitioner(JobContext job, Class<?> cls) {
    job.getConfiguration().setClass(ETL_DEFAULT_PARTITIONER_CLASS, cls, Partitioner.class);
  }

  public static Partitioner getDefaultPartitioner(JobContext job) {
    return ReflectionUtils.newInstance(
        job.getConfiguration().getClass(ETL_DEFAULT_PARTITIONER_CLASS, DefaultPartitioner.class, Partitioner.class),
        job.getConfiguration());
  }

  public static Partitioner getPartitioner(JobContext job, String topicName) throws IOException {
    String customPartitionerProperty = ETL_DEFAULT_PARTITIONER_CLASS + "." + topicName;
    if (partitionersByTopic.get(customPartitionerProperty) == null) {
      List<Partitioner> partitioners = new ArrayList<Partitioner>();
      if (partitioners.isEmpty()) {
        return getDefaultPartitioner(job);
      } else {
        partitionersByTopic.put(customPartitionerProperty, partitioners.get(0));
      }
    }
    return partitionersByTopic.get(customPartitionerProperty);
  }

  public static void resetPartitioners() {
    partitionersByTopic = new HashMap<String, Partitioner>();
  }
}
