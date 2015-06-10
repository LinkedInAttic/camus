package com.linkedin.camus.etl.kafka.coders;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;

import com.linkedin.camus.etl.kafka.common.AbstractMonitoringEvent;
import com.linkedin.camus.etl.kafka.common.Source;

public class MonitoringEventForUnitTest extends AbstractMonitoringEvent {

  public MonitoringEventForUnitTest(Configuration config) {
    super(config);
  }

  public MonitoringEventForUnitTest() {
    super(new Configuration());
  }

  @Override
  public GenericRecord createMonitoringEventRecord(Source countEntry, String topic, long granularity, String tier) {
    return new GenericRecordForUnitTest();
  }

}