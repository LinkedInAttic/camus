package com.linkedin.camus.etl.kafka.common;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.conf.Configuration;
import org.easymock.EasyMock;

import com.linkedin.camus.etl.kafka.CamusJobTestWithMock;


public class EtlCountsForUnitTest extends EtlCounts {

  public enum ProducerType {
    REGULAR,
    SEND_THROWS_EXCEPTION,
    SEND_SUCCEED_THIRD_TIME;
  }

  public static ProducerType producerType = ProducerType.REGULAR;

  public EtlCountsForUnitTest(EtlCounts other) {
    super(other.getTopic(), other.getGranularity(), other.getStartTime());
    this.counts = other.getCounts();
  }

  @Override
  protected String getMonitoringEventClass(Configuration conf) {
    return "com.linkedin.camus.etl.kafka.coders.MonitoringEventForUnitTest";
  }

  @Override
  protected Producer createProducer(Properties props) {
    switch (producerType) {
      case REGULAR:
        return new Producer(new ProducerConfig(props));
      case SEND_THROWS_EXCEPTION:
        return mockProducerSendThrowsException();
      case SEND_SUCCEED_THIRD_TIME:
        return mockProducerThirdSendSucceed();
      default:
        throw new RuntimeException("producer type not found");
    }
  }

  private Producer mockProducerSendThrowsException() {
    Producer mockProducer = EasyMock.createMock(Producer.class);
    mockProducer.send((KeyedMessage) EasyMock.anyObject());
    EasyMock.expectLastCall().andThrow(new RuntimeException("dummyException")).anyTimes();
    mockProducer.close();
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(mockProducer);
    return mockProducer;
  }

  private Producer mockProducerThirdSendSucceed() {
    Producer mockProducer = EasyMock.createMock(Producer.class);
    mockProducer.send((KeyedMessage) EasyMock.anyObject());
    EasyMock.expectLastCall().andThrow(new RuntimeException("dummyException")).times(2);
    mockProducer.send((KeyedMessage) EasyMock.anyObject());
    EasyMock.expectLastCall().times(1);
    mockProducer.close();
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(mockProducer);
    return mockProducer;
  }

  public static void reset() {
    producerType = ProducerType.REGULAR;
  }
}
