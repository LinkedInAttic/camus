package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;

import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.easymock.EasyMock;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.common.EtlKey;


public class EtlInputFormatForUnitTest extends EtlInputFormat {
  public static enum ConsumerType {
    REGULAR,
    MOCK
  }

  public static enum RecordReaderClass {
    REGULAR,
    TEST
  }

  public static SimpleConsumer consumer;
  public static ConsumerType consumerType = ConsumerType.REGULAR;
  public static RecordReaderClass recordReaderClass = RecordReaderClass.REGULAR;

  public EtlInputFormatForUnitTest() {
    super();
  }

  @Override
  public SimpleConsumer createSimpleConsumer(JobContext context, String host, int port) {
    switch (consumerType) {
      case REGULAR:
        return new SimpleConsumer(host, port, CamusJob.getKafkaTimeoutValue(context),
            CamusJob.getKafkaBufferSize(context), CamusJob.getKafkaClientName(context));
      case MOCK:
        return consumer;
      default:
        throw new RuntimeException("consumer type not found");
    }
  }

  @Override
  public RecordReader<EtlKey, CamusWrapper> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    switch (recordReaderClass) {
      case REGULAR:
        return new EtlRecordReader(this, split, context);
      case TEST:
        return new EtlRecordReaderForUnitTest(this, split, context);
      default:
        throw new RuntimeException("record reader class not found");
    }
  }

  private SimpleConsumer createRegularMockConsumer() {
    SimpleConsumer simpleConsumer = EasyMock.createNiceMock(SimpleConsumer.class);
    EasyMock.expect(simpleConsumer.clientId()).andReturn("dummy:1234").anyTimes();
    EasyMock.expect(simpleConsumer.send((TopicMetadataRequest) EasyMock.anyObject())).times(2);
    simpleConsumer.close();
    EasyMock.expectLastCall().andVoid().times(2);
    EasyMock.replay(simpleConsumer);
    return simpleConsumer;
  }

  private SimpleConsumer createMockConsumerSendAndClientIdAlwaysFail() {
    SimpleConsumer simpleConsumer = EasyMock.createMock(SimpleConsumer.class);
    EasyMock.expect(simpleConsumer.clientId()).andReturn("dummy:1234").anyTimes();
    Exception ex = new RuntimeException("No TopicMeta");
    EasyMock.expect(simpleConsumer.send((TopicMetadataRequest) EasyMock.anyObject())).andThrow(ex).anyTimes();
    simpleConsumer.close();
    EasyMock.expectLastCall().andVoid().anyTimes();
    EasyMock.replay(simpleConsumer);
    return simpleConsumer;
  }

  private SimpleConsumer createMockConsumerSendAndClientIdFailTwice() {
    SimpleConsumer simpleConsumer = EasyMock.createMock(SimpleConsumer.class);
    EasyMock.expect(simpleConsumer.clientId()).andReturn("dummy:1234").anyTimes();
    Exception ex = new RuntimeException("No TopicMeta");
    EasyMock.expect(simpleConsumer.send((TopicMetadataRequest) EasyMock.anyObject())).andThrow(ex).times(2);
    simpleConsumer.close();
    EasyMock.expectLastCall().andVoid().times(2);
    EasyMock.replay(simpleConsumer);
    return simpleConsumer;
  }

}
