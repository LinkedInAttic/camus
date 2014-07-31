package com.linkedin.camus.etl.kafka.coders;

/**
 * Created by ehrlichja on 7/30/14.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class TestDefaultPartitioner {

    @Test
    public void testGeneratePartitionPath() {
        // generatePartitionPath() should take a timestamp and return a formatted string by default
        Configuration testConfiguration = new Configuration();
        JobID testJobID = new JobID();
        JobContext testJobContext = new JobContext(testConfiguration, testJobID);

        DefaultPartitioner testPartitioner = new DefaultPartitioner();

        String actualResult = testPartitioner.generatePartitionedPath(testJobContext, "testTopic", "testBrokerId", 123, "1406777693000");
        String expectedResult = "testTopic/hourly/2014-07-30T20:34:53.000-07:00";

        assertTrue(actualResult.equals(expectedResult));
    }

}
