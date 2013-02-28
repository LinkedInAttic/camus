package com.linkedin.camus.etl.kafka.mapred;


import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.kafka.coders.DefaultPartitioner;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class EtlMultiOutputCommitterTest {

    TaskAttemptContext taskAttemptContext;
    EtlMultiOutputFormat etlMultiOutputFormat;
    EtlMultiOutputFormat.EtlMultiOutputCommitter committer;
    Partitioner partitioner;

    @Before
    public void setup() throws IOException {
        taskAttemptContext = new TaskAttemptContext(new Configuration(), new TaskAttemptID());
        etlMultiOutputFormat = new EtlMultiOutputFormat();
        committer = (EtlMultiOutputFormat.EtlMultiOutputCommitter) etlMultiOutputFormat.getOutputCommitter(taskAttemptContext);
        partitioner = new DefaultPartitioner();
    }

    @Test
    public void testWorkingFilenameIsCorrectUsingDefaultPartitioner() {
        long now = System.currentTimeMillis();
        EtlKey key = new EtlKey("topic", "1", 0);
        key.setTime(now);

        assertEquals("data.topic.1.0." + convertTime(now), etlMultiOutputFormat.getWorkingFileName(taskAttemptContext, key, partitioner));
    }

    @Test
    public void testWorkingFilenameRemovesDotsFromTopicName() {
        long now = System.currentTimeMillis();
        EtlKey key = new EtlKey("topic.name", "1", 0);
        key.setTime(now);

        assertEquals("data.topic_name.1.0." + convertTime(now), etlMultiOutputFormat.getWorkingFileName(taskAttemptContext, key, partitioner));
    }

    @Test
    public void testPartitionedFilenameIsCorrectUsingDefaultPartitioner() throws IOException {
        String workingFilename = "data.topic-name.1.0.1361656800000-m-00000.avro";
        assertEquals("topic-name/hourly/2013/02/23/14/topic-name.1.0.10.10000.avro",
                     committer.getPartitionedPath(taskAttemptContext, partitioner, workingFilename, 10, 10000));
    }


    @Test
    public void testDefaultPartitionerName() {
        assertEquals("com.linkedin.camus.etl.kafka.coders.DefaultPartitioner", EtlMultiOutputFormat.getPartitionerClassName(taskAttemptContext));
    }

    public long convertTime(long time) {
        long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(taskAttemptContext) * 60000L;
        return DateUtils.getPartition(outfilePartitionMs, time);
    }
}
