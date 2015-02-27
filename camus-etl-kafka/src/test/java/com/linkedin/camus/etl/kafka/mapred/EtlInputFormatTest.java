package com.linkedin.camus.etl.kafka.mapred;

import java.util.ArrayList;
import java.util.List;

import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.easymock.EasyMock;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EtlInputFormatTest {

  private static final String DUMMY_VALUE = "dummy:1234";
  
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
    EasyMock.expect(simpleConsumer.send((TopicMetadataRequest)EasyMock.anyObject())).andThrow(new RuntimeException("No TopicMD"));
    EasyMock.expect(simpleConsumer.send((TopicMetadataRequest)EasyMock.anyObject())).andReturn(topicMetadataResponse);
    simpleConsumer.close();
    EasyMock.expectLastCall().andVoid().anyTimes();

    EasyMock.replay(mocks.toArray());
    
    EtlInputFormat inputFormat = new TestEtlInputFormat(simpleConsumer);    
    List<TopicMetadata> actualTopicMetadatas = inputFormat.getKafkaMetadata(jobContext);
    
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
    EasyMock.expect(simpleConsumer.clientId()).andReturn(DUMMY_VALUE).times(EtlInputFormat.NUM_TRIES_TOPIC_METADATA + 1);
    Exception ex = new RuntimeException("No TopicMeta");
    EasyMock.expect(simpleConsumer.send((TopicMetadataRequest)EasyMock.anyObject())).andThrow(ex).times(EtlInputFormat.NUM_TRIES_TOPIC_METADATA);
    simpleConsumer.close();
    EasyMock.expectLastCall().andVoid().anyTimes();

    EasyMock.replay(mocks.toArray());

    EtlInputFormat inputFormat = new TestEtlInputFormat(simpleConsumer);    
    List<TopicMetadata> actualTopicMetadatas = inputFormat.getKafkaMetadata(jobContext);

    EasyMock.verify(mocks.toArray());
  }
  
  private static class TestEtlInputFormat extends EtlInputFormat {
    private final SimpleConsumer _simpleConsumer;
    
    public TestEtlInputFormat(SimpleConsumer simpleConsumer) {
      super();
      _simpleConsumer = simpleConsumer;
    }

    @Override
    public SimpleConsumer createSimpleConsumer(JobContext context, String host, int port) {
      return _simpleConsumer;
    }
  }
  

}
