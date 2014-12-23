package com.linkedin.camus.etl.kafka.partitioner;

import com.linkedin.camus.etl.kafka.common.EtlKey;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HourlyPartitionerTest {

  @Test
  public void testDefaultConfiguration() throws Exception {
    HourlyPartitioner underTest = new HourlyPartitioner();
    underTest.setConf(configurationFor(null, null));

    long time = new DateTime(2014, 2, 1, 3, 0, 0, 0, DateTimeZone.forID("America/Los_Angeles")).getMillis();
    String partition = underTest.encodePartition(null, etlKeyWithTime(time));
    String path = underTest.generatePartitionedPath(null, "tpc", partition);

    assertEquals("tpc/hourly/2014/02/01/03", path);
  }

  @Test
  public void testPicksUpConfiguration() throws Exception {
    HourlyPartitioner underTest = new HourlyPartitioner();
    underTest.setConf(configurationFor("incoming", "Europe/Amsterdam"));

    long time = new DateTime(2014, 2, 1, 3, 0, 0, 0, DateTimeZone.forID("Europe/Amsterdam")).getMillis();
    String partition = underTest.encodePartition(null, etlKeyWithTime(time));
    String path = underTest.generatePartitionedPath(null, "tpc", partition);

    assertEquals("tpc/incoming/2014/02/01/03", path);
  }

  private EtlKey etlKeyWithTime(long time) {
    EtlKey etlKey = new EtlKey();
    etlKey.setTime(time);
    return etlKey;
  }

  private Configuration configurationFor(String path, String dateTimeZone) {
    Configuration conf = new Configuration();
    if (path != null) conf.set("etl.destination.path.topic.sub.dir", path);
    if (dateTimeZone != null) conf.set("etl.default.timezone", dateTimeZone);
    return conf;
  }

}