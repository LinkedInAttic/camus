package com.linkedin.camus.etl.kafka.coders;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import com.linkedin.camus.etl.kafka.common.EtlKey;

public class TestDefaultPartitioner {

	@Test
	public void testGeneratePartitionPath() throws IOException {
		// generatePartitionPath() should take a timestamp and return a
		// formatted string by default
		Configuration testConfiguration = new Configuration();
		Job testJob = new Job(new Configuration());

		DefaultPartitioner testPartitioner = new DefaultPartitioner();
		testPartitioner.setConf(testConfiguration);

		String actualResult = testPartitioner.generatePartitionedPath(testJob,
				"testTopic", "testBrokerId", 123, "1406777693000");
		String expectedResult = "testTopic/hourly/2014/07/31/03";

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

		String actualResult = testPartitioner.encodePartition(testJob,
				testEtlKey);
		String expectedResult = "1400547600000";

		assertTrue(actualResult.equals(expectedResult));

	}

}
