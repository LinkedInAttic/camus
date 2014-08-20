package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.coders.DefaultPartitioner;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.junit.Before;
import org.junit.Test;

public class EtlMultiOutputFormatTest {

    private JobContextImpl jobContext;

    @Before
    public void setUp() throws Exception {
        jobContext = new JobContextImpl(new Configuration(), new JobID("", 0));
        EtlMultiOutputFormat.resetPartitioners();
    }

    @Test
    public void testGetDefaultPartitioner() throws Exception {
        Partitioner partitioner = EtlMultiOutputFormat.getPartitioner(jobContext, "mytopic");
        Assert.assertEquals(DefaultPartitioner.class, partitioner.getClass());
    }

    @Test
    public void testSetDefaultPartitioner() throws Exception {
        EtlMultiOutputFormat.setDefaultPartitioner(jobContext, CustomPartitioner.class);
        Partitioner defaultPartitioner = EtlMultiOutputFormat.getDefaultPartitioner(jobContext);
        Assert.assertEquals(CustomPartitioner.class, defaultPartitioner.getClass());
    }

    @Test
    public void testGetTopicsDefaultPartioner() throws Exception {
        Partitioner partitioner = EtlMultiOutputFormat.getPartitioner(jobContext, "mytopic");
        Assert.assertEquals(DefaultPartitioner.class, partitioner.getClass());
    }

    @Test
    public void testGetTopicsDefinedPartitioner() throws Exception {
        jobContext.getConfiguration().set(EtlMultiOutputFormat.ETL_DEFAULT_PARTITIONER_CLASS + "." + "mytopic",
                "com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormatTest$CustomPartitioner");

        Partitioner partitioner = EtlMultiOutputFormat.getPartitioner(jobContext, "mytopic");
        Assert.assertEquals(CustomPartitioner.class, partitioner.getClass());
    }

    static class CustomPartitioner extends Partitioner {

        @Override
        public String encodePartition(JobContext context, IEtlKey etlKey) {
            return null;
        }

        @Override
        public String generatePartitionedPath(JobContext context, String topic, String brokerId, int partitionId, String encodedPartition) {
            return null;
        }
    }
}
