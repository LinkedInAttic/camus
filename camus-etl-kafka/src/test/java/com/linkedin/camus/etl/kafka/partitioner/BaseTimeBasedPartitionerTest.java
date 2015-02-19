package com.linkedin.camus.etl.kafka.partitioner;

import com.linkedin.camus.etl.kafka.common.EtlKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class BaseTimeBasedPartitionerTest {
  private static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forID("Europe/Amsterdam");

  private BaseTimeBasedPartitioner underTest = new BiHourlyPartitioner();

  @Test
  public void testEncodePartition() throws Exception {
    long time = new DateTime(2014, 1, 1, 3, 0, 0, 0, DATE_TIME_ZONE).getMillis();
    String partition = underTest.encodePartition(null, etlKeyWithTime(time));
    assertEquals("1388538000000", partition);
  }

  @Test
  public void testGeneratePartitionedPath() throws Exception {
    String path = underTest.generatePartitionedPath(null, "tpc", "1388538000000");
    assertEquals("tpc/bi-hourly/year=2014/month=janvier/day=01/hour=2", path);
  }

  @Test
  public void testGenerateFileName() throws Exception {
    String fileName = underTest.generateFileName(null, "tpc", "brk1", 1, 2, 45330016, "1388538000000");
    assertEquals("tpc.brk1.1.2.45330016.1388538000000", fileName);
  }

  @Test
  public void testGetWorkingFileName() throws Exception {
    String workingFileName = underTest.getWorkingFileName(null, "tpc", "brk1", 1, "1388538000000");
    assertEquals("data.tpc.brk1.1.1388538000000", workingFileName);

  }

  private EtlKey etlKeyWithTime(long time) {
    EtlKey etlKey = new EtlKey();
    etlKey.setTime(time);
    return etlKey;
  }

  private static class BiHourlyPartitioner extends BaseTimeBasedPartitioner {
    public BiHourlyPartitioner() {
      init(TimeUnit.HOURS.toMillis(2), "'bi-hourly'/'year='YYYY/'month='MMMM/'day='dd/'hour='H", Locale.FRENCH, DATE_TIME_ZONE);
    }
  }
}