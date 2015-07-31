package com.linkedin.camus.etl.kafka.mapred;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import kafka.common.ErrorMapping;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;


public class EtlInputFormatTest {

  private static final String DUMMY_VALUE = "dummy:1234";

  @Test
  public void testEmptyWhitelistBlacklistEntries() {
    Configuration conf = new Configuration();
    conf.set(EtlInputFormat.KAFKA_WHITELIST_TOPIC, ",TopicA,TopicB,,TopicC,");
    conf.set(EtlInputFormat.KAFKA_BLACKLIST_TOPIC, ",TopicD,TopicE,,,,,TopicF,");

    String[] whitelistTopics = EtlInputFormat.getKafkaWhitelistTopic(conf);
    Assert.assertEquals(Arrays.asList("TopicA", "TopicB", "TopicC"), Arrays.asList(whitelistTopics));

    String[] blacklistTopics = EtlInputFormat.getKafkaBlacklistTopic(conf);
    Assert.assertEquals(Arrays.asList("TopicD", "TopicE", "TopicF"), Arrays.asList(blacklistTopics));
  }

  @Test
  public void testWithOneRetry() {
    List<Object> mocks = new ArrayList<Object>();

    Configuration configuration = EasyMock.createMock(Configuration.class);
    mocks.add(configuration);
    EasyMock.expect(configuration.get(EasyMock.anyString())).andReturn(DUMMY_VALUE).anyTimes();

    JobContext jobContext = EasyMock.createMock(JobContext.class);
    mocks.add(jobContext);
    EasyMock.expect(jobContext.getConfiguration()).andReturn(configuration).anyTimes();

    List<TopicMetadata> topicMetadatas = new ArrayList<TopicMetadata>();
    TopicMetadataResponse topicMetadataResponse = EasyMock.createMock(TopicMetadataResponse.class);
    mocks.add(topicMetadataResponse);
    EasyMock.expect(topicMetadataResponse.topicsMetadata()).andReturn(topicMetadatas);

    SimpleConsumer simpleConsumer = EasyMock.createMock(SimpleConsumer.class);
    mocks.add(simpleConsumer);
    EasyMock.expect(simpleConsumer.clientId()).andReturn(DUMMY_VALUE).times(2);
    EasyMock.expect(simpleConsumer.send((TopicMetadataRequest) EasyMock.anyObject())).andThrow(
        new RuntimeException("No TopicMD"));
    EasyMock.expect(simpleConsumer.send((TopicMetadataRequest) EasyMock.anyObject())).andReturn(topicMetadataResponse);
    simpleConsumer.close();
    EasyMock.expectLastCall().andVoid().anyTimes();

    EasyMock.replay(mocks.toArray());
    
    EtlInputFormat inputFormat = new EtlInputFormatForUnitTest();
    EtlInputFormatForUnitTest.consumerType = EtlInputFormatForUnitTest.ConsumerType.MOCK;
    EtlInputFormatForUnitTest.consumer = simpleConsumer;
    List<TopicMetadata> actualTopicMetadatas = inputFormat.getKafkaMetadata(jobContext, new ArrayList<String>());
    
    EasyMock.verify(mocks.toArray());

    assertEquals(actualTopicMetadatas, topicMetadatas);
  }

  @Test(expected = RuntimeException.class)
  public void testWithThreeRetries() {
    List<Object> mocks = new ArrayList<Object>();

    Configuration configuration = EasyMock.createMock(Configuration.class);
    mocks.add(configuration);
    EasyMock.expect(configuration.get(EasyMock.anyString())).andReturn(DUMMY_VALUE).anyTimes();

    JobContext jobContext = EasyMock.createMock(JobContext.class);
    mocks.add(jobContext);
    EasyMock.expect(jobContext.getConfiguration()).andReturn(configuration).anyTimes();

    SimpleConsumer simpleConsumer = EasyMock.createMock(SimpleConsumer.class);
    mocks.add(simpleConsumer);
    EasyMock.expect(simpleConsumer.clientId()).andReturn(DUMMY_VALUE)
        .times(EtlInputFormat.NUM_TRIES_TOPIC_METADATA + 1);
    Exception ex = new RuntimeException("No TopicMeta");
    EasyMock.expect(simpleConsumer.send((TopicMetadataRequest) EasyMock.anyObject())).andThrow(ex)
        .times(EtlInputFormat.NUM_TRIES_TOPIC_METADATA);
    simpleConsumer.close();
    EasyMock.expectLastCall().andVoid().anyTimes();

    EasyMock.replay(mocks.toArray());

    EtlInputFormat inputFormat = new EtlInputFormatForUnitTest();
    EtlInputFormatForUnitTest.consumerType = EtlInputFormatForUnitTest.ConsumerType.MOCK;
    EtlInputFormatForUnitTest.consumer = simpleConsumer;
    List<TopicMetadata> actualTopicMetadatas = inputFormat.getKafkaMetadata(jobContext, new ArrayList<String>());

    EasyMock.verify(mocks.toArray());
  }

  /**
   * Test only refreshing the paritionMetadata when the error code is LeaderNotAvailable.
   * @throws Exception 
   */
  @Test
  public void testRefreshPartitioMetadataOnLeaderNotAvailable() throws Exception {
    JobContext dummyContext = null;
    //A partitionMetadata with errorCode LeaderNotAvailable
    PartitionMetadata partitionMetadata1 = createMock(PartitionMetadata.class);
    expect(partitionMetadata1.errorCode()).andReturn(ErrorMapping.LeaderNotAvailableCode());
    expect(partitionMetadata1.partitionId()).andReturn(0);
    replay(partitionMetadata1);

    //A partitionMetadata with errorCode not LeaderNotAvailable
    PartitionMetadata partitionMetadata2 = createMock(PartitionMetadata.class);
    expect(partitionMetadata2.errorCode()).andReturn(ErrorMapping.InvalidMessageCode());
    expect(partitionMetadata2.partitionId()).andReturn(0);
    replay(partitionMetadata2);

    PartitionMetadata mockedReturnedPartitionMetadata = createMock(PartitionMetadata.class);
    expect(mockedReturnedPartitionMetadata.errorCode()).andReturn(ErrorMapping.NoError());
    expect(mockedReturnedPartitionMetadata.partitionId()).andReturn(0);
    replay(mockedReturnedPartitionMetadata);

    TopicMetadata mockedTopicMetadata = createMock(TopicMetadata.class);
    expect(mockedTopicMetadata.topic()).andReturn("testTopic");
    expect(mockedTopicMetadata.partitionsMetadata()).andReturn(
        Collections.singletonList(mockedReturnedPartitionMetadata));
    replay(mockedTopicMetadata);

    EtlInputFormat etlInputFormat =
        createMock(EtlInputFormat.class,
            EtlInputFormat.class.getMethod("getKafkaMetadata", new Class[] { JobContext.class, List.class }));
    EasyMock.expect(etlInputFormat.getKafkaMetadata(dummyContext, Collections.singletonList("testTopic"))).andReturn(
        Collections.singletonList(mockedTopicMetadata));
    etlInputFormat.setLogger(Logger.getLogger(getClass()));
    replay(etlInputFormat);

    // For partitionMetadata2, it will not refresh if the errorcode is not LeaderNotAvailable.
    assertEquals(partitionMetadata2, etlInputFormat.refreshPartitionMetadataOnLeaderNotAvailable(partitionMetadata2,
        mockedTopicMetadata, dummyContext, EtlInputFormat.NUM_TRIES_PARTITION_METADATA));

    // For partitionMetadata1, it will refresh if the errorcode is LeaderNotAvailable.
    assertEquals(mockedReturnedPartitionMetadata, etlInputFormat.refreshPartitionMetadataOnLeaderNotAvailable(
        partitionMetadata1, mockedTopicMetadata, dummyContext, EtlInputFormat.NUM_TRIES_PARTITION_METADATA));

  }

  /**
   * Test only refreshing the paritionMetadata when the error code is LeaderNotAvailable.
   * @throws Exception 
   */
  @Test
  public void testRefreshPartitioMetadataWithThreeRetries() throws Exception {
    JobContext dummyContext = null;
    //A partitionMetadata with errorCode LeaderNotAvailable
    PartitionMetadata partitionMetadata = createMock(PartitionMetadata.class);
    expect(partitionMetadata.errorCode()).andReturn(ErrorMapping.LeaderNotAvailableCode()).times(EtlInputFormat.NUM_TRIES_PARTITION_METADATA * 2);
    expect(partitionMetadata.partitionId()).andReturn(0).times(EtlInputFormat.NUM_TRIES_PARTITION_METADATA * 2);
    replay(partitionMetadata);

    TopicMetadata mockedTopicMetadata = createMock(TopicMetadata.class);
    expect(mockedTopicMetadata.topic()).andReturn("testTopic").times(EtlInputFormat.NUM_TRIES_PARTITION_METADATA);
    expect(mockedTopicMetadata.partitionsMetadata()).andReturn(Collections.singletonList(partitionMetadata)).times(
        EtlInputFormat.NUM_TRIES_PARTITION_METADATA);
    replay(mockedTopicMetadata);

    EtlInputFormat etlInputFormat =
        createMock(EtlInputFormat.class,
            EtlInputFormat.class.getMethod("getKafkaMetadata", new Class[] { JobContext.class, List.class }));
    EasyMock.expect(etlInputFormat.getKafkaMetadata(dummyContext, Collections.singletonList("testTopic"))).andReturn(
        Collections.singletonList(mockedTopicMetadata)).times(EtlInputFormat.NUM_TRIES_PARTITION_METADATA);
    etlInputFormat.setLogger(Logger.getLogger(getClass()));
    replay(etlInputFormat);

    etlInputFormat.refreshPartitionMetadataOnLeaderNotAvailable(partitionMetadata, mockedTopicMetadata, dummyContext,
        EtlInputFormat.NUM_TRIES_PARTITION_METADATA);
    
    verify(mockedTopicMetadata);
    verify(etlInputFormat);
  }

  @After
  public void after() {
    EtlInputFormatForUnitTest.consumerType = EtlInputFormatForUnitTest.ConsumerType.REGULAR;
  }
}
