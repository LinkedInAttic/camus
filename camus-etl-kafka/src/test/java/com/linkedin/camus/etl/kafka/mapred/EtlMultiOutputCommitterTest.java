package com.linkedin.camus.etl.kafka.mapred;


import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.coders.DefaultPartitioner;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class EtlMultiOutputCommitterTest implements Partitioner {

    TaskAttemptContext taskAttemptContext;
    EtlMultiOutputFormat etlMultiOutputFormat;
    EtlMultiOutputFormat.EtlMultiOutputCommitter committer;
    Configuration configuration;

    @Before
    public void setup() throws Exception {
        configuration = new Configuration();
        configuration.set(EtlMultiOutputFormat.ETL_DEFAULT_PARTITIONER_CLASS, "com.linkedin.camus.etl.kafka.coders.DefaultPartitioner");

        Constructor taskAttemptContextConstructor;
        Class taskAttemptClass = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptContext");
        if (taskAttemptClass.isInterface()) {
            Class taskAttamptImplClass = Class.forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
            taskAttemptContextConstructor = taskAttamptImplClass.getConstructor(Configuration.class, TaskAttemptID.class);
        } else
            taskAttemptContextConstructor = taskAttemptClass.getConstructor(Configuration.class, TaskAttemptID.class);

        taskAttemptContext = (TaskAttemptContext) taskAttemptContextConstructor.newInstance(configuration, new TaskAttemptID());
        etlMultiOutputFormat = new EtlMultiOutputFormat();
        committer = (EtlMultiOutputFormat.EtlMultiOutputCommitter) etlMultiOutputFormat.getOutputCommitter(taskAttemptContext);
    }

    @After
    public void tearDown() {
        EtlMultiOutputFormat.resetPartitioners();
    }

    @Test
    public void testWorkingFilenameIsCorrectUsingDefaultPartitioner() throws IOException {
        long now = System.currentTimeMillis();
        EtlKey key = new EtlKey("topic", "1", 0);
        key.setTime(now);

        assertEquals("data.topic.1.0." + convertTime(now), etlMultiOutputFormat.getWorkingFileName(taskAttemptContext, key));
    }

    @Test
    public void testWorkingFilenameRemovesDotsFromTopicName() throws IOException {
        long now = System.currentTimeMillis();
        EtlKey key = new EtlKey("topic.name", "1", 0);
        key.setTime(now);

        assertEquals("data.topic_name.1.0." + convertTime(now), etlMultiOutputFormat.getWorkingFileName(taskAttemptContext, key));
    }

    @Test
    public void testPartitionedFilenameIsCorrectUsingDefaultPartitioner() throws IOException {
        String workingFilename = "data.topic-name.1.0.1361656800000-m-00000.avro";
        assertEquals("topic-name/hourly/2013/02/23/14/topic-name.1.0.10.10000.avro",
                     committer.getPartitionedPath(taskAttemptContext, workingFilename, 10, 10000));
    }

    @Test
    public void testDefaultPartitionerIsTheDefault() {
        assertTrue(EtlMultiOutputFormat.getDefaultPartitioner(taskAttemptContext) instanceof DefaultPartitioner);
    }

    @Test
    public void testConfiguredDefaultPartitionerIsCorrect() throws ClassNotFoundException {
        taskAttemptContext.getConfiguration().set(EtlMultiOutputFormat.ETL_DEFAULT_PARTITIONER_CLASS,
                "com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputCommitterTest");
        assertTrue(EtlMultiOutputFormat.getDefaultPartitioner(taskAttemptContext) instanceof EtlMultiOutputCommitterTest);
    }

    @Test
    public void testPerTopicPartitioner() throws IOException {
        taskAttemptContext.getConfiguration().set(EtlMultiOutputFormat.ETL_DEFAULT_PARTITIONER_CLASS + ".test-topic",
                "com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputCommitterTest");
        assertTrue(EtlMultiOutputFormat.getDefaultPartitioner(taskAttemptContext) instanceof DefaultPartitioner);
        assertTrue(EtlMultiOutputFormat.getPartitioner(taskAttemptContext, "unknown-topic") instanceof DefaultPartitioner);
        assertTrue(EtlMultiOutputFormat.getPartitioner(taskAttemptContext, "test-topic") instanceof EtlMultiOutputCommitterTest);
    }

    @Test
    public void testDefaultOutputCodecIsDeflate() {
        assertEquals("deflate", EtlMultiOutputFormat.getEtlOutputCodec(taskAttemptContext));
    }
    
    @Test
    public void testSetOutputCodec() {
        EtlMultiOutputFormat.setEtlOutputCodec(taskAttemptContext, "snappy");
        assertEquals("snappy", EtlMultiOutputFormat.getEtlOutputCodec(taskAttemptContext));
    }

    public long convertTime(long time) {
        long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(taskAttemptContext) * 60000L;
        return DateUtils.getPartition(outfilePartitionMs, time);
    }

    @Override
    public String encodePartition(JobContext context, IEtlKey etlKey) {
        return null;
    }

    @Override
    public String generatePartitionedPath(JobContext context, String topic, int brokerId, int partitionId, String encodedPartition) {
        return null;
    }
}
