package com.linkedin.camus.etl.kafka.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTimeZone;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat.ETL_DEFAULT_TIMEZONE;
import static com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat.ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY;

/**
 * Partitions incoming data into daily partitions, and generates pathnames of the form:
 * {@code etl.destination.path/topic-name/recent/YYYY/MM/dd}.
 *
 * The following configurations are supported:
 * <ul>
 *     <li>{@code etl.destination.path} - top-level data output directory, required</li>
 *     <li>{@code etl.destination.path.topic.sub.dir} - sub-dir to create under topic dir, defaults to {@code daily}</li>
 *     <li>{@code etl.default.timezone} - timezone of the events, defaults to {@code America/Los_Angeles}</li>
 * </ul>
 */
public class DailyPartitioner extends BaseTimeBasedPartitioner {

  private static final String DEFAULT_TOPIC_SUB_DIR = "daily";

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      String destPathTopicSubDir = conf.get(ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY, DEFAULT_TOPIC_SUB_DIR);
      DateTimeZone outputTimeZone = DateTimeZone.forID(conf.get(ETL_DEFAULT_TIMEZONE, DEFAULT_TIME_ZONE));

      long outfilePartitionMs = TimeUnit.HOURS.toMillis(24);
      String destSubTopicPathFormat = "'" + destPathTopicSubDir + "'/YYYY/MM/dd";
      init(outfilePartitionMs, destSubTopicPathFormat, Locale.US, outputTimeZone);
    }

    super.setConf(conf);
  }

}
