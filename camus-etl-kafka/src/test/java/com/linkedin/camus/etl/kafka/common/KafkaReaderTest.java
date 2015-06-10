package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;

import kafka.javaapi.FetchRequest;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;


public class KafkaReaderTest {
  private KafkaReader kafkaReader;

  @Before
  public void before() throws Exception {
    Configuration conf = new Configuration();
    conf.set(CamusJob.KAFKA_CLIENT_NAME, "DummyClientName");
    TaskAttemptContext context = null;
    try {
      Class<?> taskAttemptContextImplClass = Class.forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
      context =
          (TaskAttemptContext) taskAttemptContextImplClass.getDeclaredConstructor(Configuration.class,
              TaskAttemptID.class).newInstance(conf, new TaskAttemptID());
    } catch (ClassNotFoundException e) {
      context =
          (TaskAttemptContext) Class.forName("org.apache.hadoop.mapreduce.TaskAttemptContext")
              .getDeclaredConstructor(Configuration.class, TaskAttemptID.class).newInstance(conf, new TaskAttemptID());
    }
    EtlRequest request = new EtlRequest();
    request.setOffset(0);
    request.setLatestOffset(1);
    request.setURI(new URI("http://localhost:8888"));
    this.kafkaReader = new KafkaReader(new EtlInputFormat(), context, request, 100, 100);
  }

  @Test
  public void testFetchFailure() throws SecurityException, NoSuchFieldException, IllegalArgumentException,
      IllegalAccessException, IOException {
    SimpleConsumer consumer = EasyMock.createNiceMock(SimpleConsumer.class);
    EasyMock.expect(consumer.fetch((FetchRequest) EasyMock.anyObject())).andReturn(null);
    EasyMock.expectLastCall().times(2);
    EasyMock.expect(consumer.send((TopicMetadataRequest) EasyMock.anyObject())).andThrow(new RuntimeException());
    EasyMock.replay(consumer);

    Field field = kafkaReader.getClass().getDeclaredField("simpleConsumer");
    field.setAccessible(true);
    field.set(kafkaReader, consumer);
    kafkaReader.fetch();
    EasyMock.verify(consumer);
  }
}
