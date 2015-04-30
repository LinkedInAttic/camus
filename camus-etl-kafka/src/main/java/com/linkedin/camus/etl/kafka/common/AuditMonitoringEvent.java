package com.linkedin.camus.etl.kafka.common;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.UUID;

import Vsw.AvroDto.Core.DateTimeWrapper;
import Vsw.AvroDto.Core.GuidWrapper;
import Vsw.AvroDto.Core.LogInformation;
import Vsw.AvroDto.Kafka.AuditMessage;
import Vsw.AvroDto.Kafka.Datacenters;
import Vsw.AvroDto.Kafka.Tiers;

public class AuditMonitoringEvent extends AbstractMonitoringEvent {
  public static final String DATACENTER_NAME = "datacenter.name";
  private static String server;
  private static String datacenter = "lv";
  private Datacenters dataCenterValue;

  public AuditMonitoringEvent(Configuration config) {
    super(config);
    datacenter = this.conf.get(DATACENTER_NAME, datacenter);

    try {
      server = InetAddress.getLocalHost().toString();
    } catch (UnknownHostException e) {
      server = "unknown server name";
    }

    try {
      this.dataCenterValue = Datacenters.valueOf(datacenter);
    } catch (IllegalArgumentException iae) {
      this.dataCenterValue = Datacenters.lv;
    }
  }

  public GenericRecord createMonitoringEventRecord(Source countEntry, String topic, long granularity, String tier) {
    DateTime start = new DateTime(countEntry.getStart());
    DateTime end = start.plusMinutes((int)(granularity / 60000L)).minusMillis(1);

    LogInformation logInformation = new LogInformation();
    logInformation.setApplication("Camus");
    logInformation.setServerName(server);
    logInformation.setLogDateUtc(new DateTimeWrapper(Long.valueOf((new Date()).getTime())));
    logInformation.setLogGuid(new GuidWrapper(UUID.randomUUID().toString().getBytes()));

    AuditMessage auditMessage = new AuditMessage();
    auditMessage.setTopic(topic);
    auditMessage.setApplication("Camus");
    auditMessage.setTier(Tiers.consumer);
    auditMessage.setCount(Integer.valueOf((int)countEntry.getCount()));
    auditMessage.setBucketStart(new DateTimeWrapper(Long.valueOf(start.getMillis())));
    auditMessage.setBucketEnd(new DateTimeWrapper(Long.valueOf(end.getMillis())));
    auditMessage.setDatacenter(this.dataCenterValue);
    auditMessage.setLogInfo(logInformation);

    return auditMessage;
  }
}

