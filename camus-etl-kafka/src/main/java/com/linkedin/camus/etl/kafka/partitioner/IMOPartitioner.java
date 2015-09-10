package com.linkedin.camus.etl.kafka.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTimeZone;
import org.joda.time.DateTime;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat.ETL_DEFAULT_TIMEZONE;
import static com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat.ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY;

/**
 * Partitions incoming data into daily partitions, and generates pathnames of the form:
 * {@code etl.destination.path/topic-name/YYYY/MM/dd}.
 *
 * The following configurations are supported:
 * <ul>
 *     <li>{@code etl.destination.path} - top-level data output directory, required</li>
 *     <li>{@code etl.default.timezone} - timezone of the events, defaults to {@code America/Los_Angeles}</li>
 * </ul>
 */
public class IMOPartitioner extends BaseTimeBasedPartitioner {
  public static final String CURRENT_TIME = Integer.toString((int) (System.currentTimeMillis() / 1000L));

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      DateTimeZone outputTimeZone = DateTimeZone.forID(conf.get(ETL_DEFAULT_TIMEZONE, DEFAULT_TIME_ZONE));

      long outfilePartitionMs = TimeUnit.HOURS.toMillis(24);
      String destSubTopicPathFormat = "YYYY/MM/dd";
      init(outfilePartitionMs, destSubTopicPathFormat, Locale.US, outputTimeZone);
    }

    super.setConf(conf);
  }

  @Override
  public String generatePartitionedPath(JobContext context, String topic, String encodedPartition, int partitionId) {
    DateTime bucket = new DateTime(Long.valueOf(encodedPartition));
    return topic + "/" + bucket.toString(outputDirFormatter) + "/" + partitionId;
  }

  @Override
  public String generateFileName(JobContext context, String topic, String brokerId, int partitionId, int count,
      long offset, String encodedPartition) {
    return topic + "." + partitionId + "." + CURRENT_TIME + "." + brokerId + "." + count + "." + offset + "." + encodedPartition;
  }

  @Override
  public String getWorkingFileName(JobContext context, String topic, String brokerId, int partitionId,
      String encodedPartition) {
    return "data#" + topic + "#" + brokerId + "#" + partitionId + "#" + encodedPartition;
  }
}
