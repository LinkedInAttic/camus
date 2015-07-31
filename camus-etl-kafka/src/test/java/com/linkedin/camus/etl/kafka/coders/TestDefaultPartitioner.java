package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.partitioner.DefaultPartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;


public class TestDefaultPartitioner {

  @Test
  public void testGeneratePartitionPath() throws IOException {
    // generatePartitionPath() should take a timestamp and return a formatted string by default
    Configuration testConfiguration = new Configuration();
    Job testJob = new Job(new Configuration());

    DefaultPartitioner testPartitioner = new DefaultPartitioner();
    testPartitioner.setConf(testConfiguration);

    String actualResult = testPartitioner.generatePartitionedPath(testJob, "testTopic", "1406777693000");
    String expectedResult = "testTopic/hourly/2014/07/30/20";

    assertTrue(actualResult.equals(expectedResult));

    actualResult =
        testPartitioner.generateFileName(testJob, "testTopic", "testBrokerId", 123, 100, 500, "1406777693000");
    expectedResult = "testTopic.testBrokerId.123.100.500.1406777693000";

    assertTrue(actualResult.equals(expectedResult));
  }

  @Test
  public void testEncodedPartition() throws IOException {
    EtlKey testEtlKey = new EtlKey();
    testEtlKey.setTime(1400549463000L);
    Configuration testConfiguration = new Configuration();
    Job testJob = new Job(new Configuration());

    DefaultPartitioner testPartitioner = new DefaultPartitioner();
    testPartitioner.setConf(testConfiguration);

    String actualResult = testPartitioner.encodePartition(testJob, testEtlKey);
    String expectedResult = "1400547600000";

    assertTrue(actualResult.equals(expectedResult));

  }

}
