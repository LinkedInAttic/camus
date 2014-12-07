package com.linkedin.camus.etl.kafka.common;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;


/**
 * All details for monitoring the event counts 
 * @author ggupta
 *
 */

public abstract class AbstractMonitoringEvent {
  public Configuration conf;

  /**
   * Constructor to accept a configuration parameter
   * @param config
   */
  public AbstractMonitoringEvent(Configuration config) {
    this.conf = config;
  }

  /**
   * Create a generic record containing the monitoring details to be published to Kafka
   * @param countEntry
   * @param topic
   * @param granularity
   * @param tier
   * @param conf
   * @return GenericRecord
   */
  public abstract GenericRecord createMonitoringEventRecord(Source countEntry, String topic, long granularity,
      String tier);

}
