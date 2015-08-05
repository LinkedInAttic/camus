package com.linkedin.camus.etl.kafka.partitioner;

import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.Partitioner;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;

/**
 * Base class for time based partitioners.
 * Can be configured via {@link #init(long, String, java.util.Locale, org.joda.time.DateTimeZone)}.
 */
abstract public class BaseTimeBasedPartitioner extends Partitioner {

  public static final String DEFAULT_TIME_ZONE = "America/Los_Angeles";

  /** Size of a partition in milliseconds.*/
  private long outfilePartitionMillis = 0;
  private DateTimeFormatter outputDirFormatter;

  /**
   * Initialize the partitioner.
   * This method must be invoked once, and before any any other method.
   *  @param outfilePartitionMillis duration of a partition, e.g. {@code 3,600,000} for hour partitions
   * @param destSubTopicPathFormat format of output sub-dir to be created under topic directory,
   *   typically something like {@code "'hourly'/YYYY/MM/dd/HH"}.
   *   For formatting rules see {@link org.joda.time.format.DateTimeFormat}.
   * @param locale locale to use for formatting of path
   * @param outputTimeZone time zone to use for date calculations
   */
  protected void init(long outfilePartitionMillis, String destSubTopicPathFormat, Locale locale, DateTimeZone outputTimeZone) {
    this.outfilePartitionMillis = outfilePartitionMillis;
    this.outputDirFormatter = DateUtils.getDateTimeFormatter(destSubTopicPathFormat, outputTimeZone).withLocale(locale);
  }

  @Override
  public String encodePartition(JobContext context, IEtlKey key) {
    return Long.toString(DateUtils.getPartition(outfilePartitionMillis, key.getTime(), outputDirFormatter.getZone()));
  }

  @Override
  public String generatePartitionedPath(JobContext context, String topic, String encodedPartition) {
    DateTime bucket = new DateTime(Long.valueOf(encodedPartition));
    return topic + "/" + bucket.toString(outputDirFormatter);
  }

  @Override
  public String generateFileName(JobContext context, String topic, String brokerId, int partitionId, int count,
      long offset, String encodedPartition) {
    return topic + "." + brokerId + "." + partitionId + "." + count + "." + offset + "." + encodedPartition;
  }

  @Override
  public String getWorkingFileName(JobContext context, String topic, String brokerId, int partitionId,
      String encodedPartition) {
    return "data." + topic.replace('.', '_') + "." + brokerId + "." + partitionId + "." + encodedPartition;
  }

}
