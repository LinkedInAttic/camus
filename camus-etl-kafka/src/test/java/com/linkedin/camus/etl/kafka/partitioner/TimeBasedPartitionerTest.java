package com.linkedin.camus.etl.kafka.partitioner;

import com.linkedin.camus.etl.kafka.common.EtlKey;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.junit.Assert.*;

public class TimeBasedPartitionerTest {

  @Test
  public void testDefaultConfiguration() throws Exception {
    TimeBasedPartitioner underTest = new TimeBasedPartitioner();
    underTest.setConf(configurationFor(null, null, null, null));

    long time = new DateTime(2014, 2, 1, 3, 0, 0, 0, DateTimeZone.forID("America/Los_Angeles")).getMillis();
    String partition = underTest.encodePartition(null, etlKeyWithTime(time));
    String path = underTest.generatePartitionedPath(null, "tpc", partition);

    assertEquals("tpc/hourly/2014/02/01/03", path);
  }

  @Test
  public void testPicksUpConfiguration() throws Exception {
    TimeBasedPartitioner underTest = new TimeBasedPartitioner();
    underTest.setConf(configurationFor(
        "120", "'bi-hourly'/'year='YYYY/'month='MMMM/'day='dd/'hour='H", "nl_NL", "Europe/Amsterdam"));

    long time = new DateTime(2014, 2, 1, 3, 0, 0, 0, DateTimeZone.forID("Europe/Amsterdam")).getMillis();
    String partition = underTest.encodePartition(null, etlKeyWithTime(time));
    String path = underTest.generatePartitionedPath(null, "tpc", partition);

    assertEquals("tpc/bi-hourly/year=2014/month=februari/day=01/hour=2", path);
  }

  private EtlKey etlKeyWithTime(long time) {
    EtlKey etlKey = new EtlKey();
    etlKey.setTime(time);
    return etlKey;
  }

  private Configuration configurationFor(String partitionDurationMinutes, String subDirFormat,
                                         String subDirFormatLocale, String dateTimeZone) {
    Configuration conf = new Configuration();
    if (partitionDurationMinutes != null) conf.set("etl.output.file.time.partition.mins", partitionDurationMinutes);
    if (subDirFormat != null) conf.set("etl.destination.path.topic.sub.dirformat", subDirFormat);
    if (subDirFormatLocale != null) conf.set("etl.destination.path.topic.sub.dirformat.locale", subDirFormatLocale);
    if (dateTimeZone != null) conf.set("etl.default.timezone", dateTimeZone);
    return conf;
  }

}