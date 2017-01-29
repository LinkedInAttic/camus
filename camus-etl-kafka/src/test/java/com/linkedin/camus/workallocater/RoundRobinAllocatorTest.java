package com.linkedin.camus.workallocater;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.mapred.EtlSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RoundRobinAllocatorTest {
    private WorkAllocator allocator = new RoundRobinAllocator();
    private JobContext context;
    private Configuration configuration;
    private int mappersCount = 5;

    @Before
    public void setUp() {
        configuration = EasyMock.createMock(Configuration.class);
        EasyMock.expect(configuration.getInt(EasyMock.anyString(), EasyMock.anyInt())).andReturn(mappersCount).anyTimes();

        context = EasyMock.createMock(JobContext.class);
        EasyMock.expect(context.getConfiguration()).andReturn(configuration).anyTimes();

        EasyMock.replay(new Object[]{configuration, context});
    }

    @Test
    public void testOneSplit() throws Exception {
        int topicsCount = 1;
        int partitionsPerTopic = 1;
        int expectedSplits = 1;

        performTest(topicsCount, partitionsPerTopic, expectedSplits);
    }

    @Test
    public void testSplitPerPartition() throws Exception {
        int topicsCount = 1;
        int partitionsPerTopic = 3;
        int expectedSplits = 3;

        performTest(topicsCount, partitionsPerTopic, expectedSplits);
    }

    @Test
    public void testNotMoreMappers() throws Exception {
        int topicsCount = 3;
        int partitionsPerTopic = 3;
        int expectedSplits = mappersCount;

        performTest(topicsCount, partitionsPerTopic, expectedSplits);
    }

    @Test
    public void testSplitPerTopic() throws Exception {
        int topicsCount = 3;
        int partitionsPerTopic = 1;
        int expectedSplits = 3;

        performTest(topicsCount, partitionsPerTopic, expectedSplits);
    }

    @Test
    public void testRoundRobiness() throws Exception {
        int topicsCount = 3;
        int partitionsPerTopic = mappersCount;
        int expectedSplits = mappersCount;

        List <InputSplit> splits = performTest(topicsCount, partitionsPerTopic, expectedSplits);

        for (InputSplit split: splits) {
            Set<String> topics = Sets.newHashSet();
            EtlSplit etlSplit = (EtlSplit) split;
            while(etlSplit.getNumRequests() > 0) {
                CamusRequest request = etlSplit.popRequest();
                topics.add(request.getTopic());
            }
            assertEquals("Each split must contain one partition for each topic", topicsCount, topics.size());
        }
    }

    @Test
    public void testDifferenceBetweenSplits() throws Exception {
        int topicsCount = 42;
        int partitionsPerTopic = 5545;
        int expectedSplits = mappersCount;

        performTest(topicsCount, partitionsPerTopic, expectedSplits);
    }

    private List<InputSplit> performTest(int topicsCount, int partitionsPerTopic, int expectedSplitsCount) throws Exception {
        List<CamusRequest> requests = generateRequests(topicsCount, partitionsPerTopic);
        List<InputSplit> streams = allocator.allocateWork(requests, context);

        assertEquals("Must create exact count of splits", expectedSplitsCount, streams.size());
        assertTrue("Max difference between splits must not be more than one", 1 >= calculateMaxDifferenceInSplitSize(streams));
        assertEquals("Must not skip partitions", topicsCount * partitionsPerTopic, calculaterequestsInSplits(streams));

        return streams;
    }

    private List<CamusRequest> generateRequests(int topicsCount, int partitions) {
        List<CamusRequest> requests = Lists.newArrayListWithCapacity(topicsCount * partitions);

        for (int topicNum = 0; topicNum < topicsCount; topicNum++) {
            for (int partNum = 0; partNum < partitions; partNum++) {
                EtlRequest request = new EtlRequest(context, "some_topic_" + topicNum, "some_leader_id", partNum);
                requests.add(request);
            }
        }
        return requests;
    }

    private int calculaterequestsInSplits(List<InputSplit> splits) throws IOException, InterruptedException {
        long requests = 0;
        for (InputSplit split : splits) {
            requests += ((EtlSplit)split).getNumRequests();
        }
        return (int) requests;
    }

    private int calculateMaxDifferenceInSplitSize(List<InputSplit> splits) throws IOException, InterruptedException {
        long min = Integer.MAX_VALUE;
        long max = Integer.MIN_VALUE;

        for (InputSplit split : splits) {
            EtlSplit etlSplit = (EtlSplit) split;
            if (split.getLength() > max) {
                max = etlSplit.getNumRequests();
            }
            if (split.getLength() < min) {
                min = etlSplit.getNumRequests();
            }
        }
        return (int) (max - min);
    }
}
