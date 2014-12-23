package com.linkedin.camus.etl.kafka.partitioner;

import com.linkedin.camus.etl.kafka.common.EtlKey;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DailyPartitionerTest {

  @Test
  public void testDefaultConfiguration() throws Exception {
    DailyPartitioner underTest = new DailyPartitioner();
    underTest.setConf(configurationFor(null, null));

    long time = new DateTime(2014, 2, 1, 1, 0, 0, 0, DateTimeZone.forID("America/Los_Angeles")).getMillis();
    String partition = underTest.encodePartition(null, etlKeyWithTime(time));
    String path = underTest.generatePartitionedPath(null, "tpc", partition);

    assertEquals("tpc/daily/2014/02/01", path);
  }

  @Test
  public void testPicksUpConfiguration() throws Exception {
    DailyPartitioner underTest = new DailyPartitioner();
    underTest.setConf(configurationFor("incoming", "Europe/Amsterdam"));

    long time = new DateTime(2014, 2, 1, 1, 0, 0, 0, DateTimeZone.forID("Europe/Amsterdam")).getMillis();
    String partition = underTest.encodePartition(null, etlKeyWithTime(time));
    String path = underTest.generatePartitionedPath(null, "tpc", partition);

    assertEquals("tpc/incoming/2014/02/01", path);
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
