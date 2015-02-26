package com.linkedin.camus.etl.kafka;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kafka.cluster.Broker;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.producer.KeyedMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.gson.Gson;
import com.linkedin.camus.etl.kafka.coders.JsonStringMessageDecoder;
import com.linkedin.camus.etl.kafka.common.SequenceFileRecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

public class CamusJobTestWithMock {

  private static final Random RANDOM = new Random();

  private static final String BASE_PATH = "/camus";
  private static final String DESTINATION_PATH = BASE_PATH + "/destination";
  private static final String EXECUTION_BASE_PATH = BASE_PATH + "/execution";
  private static final String EXECUTION_HISTORY_PATH = EXECUTION_BASE_PATH + "/history";
  
  private static final String KAFKA_HOST = "localhost";
  private static final int KAFKA_PORT = 2121;
  private static final int KAFKA_TIMEOUT_VALUE = 1000;
  private static final int KAFKA_BUFFER_SIZE = 1024;
  private static final String KAFKA_CLIENT_ID = "Camus";

  private static final String TOPIC_1 = "topic_1";
  private static final int PARTITION_1_ID = 0;
  private static final int EARLY_OFFSET = 0;
  private static final int LATE_OFFSET = 1;

  private static FileSystem fs;
  private static Gson gson;
  private static Map<String, List<MyMessage>> messagesWritten;

  // mock objects
  private static List<Object> _mocks = new ArrayList<Object>();
  private static SimpleConsumer _simpleConsumer;

  @BeforeClass
  public static void beforeClass() throws IOException {
    fs = FileSystem.get(new Configuration());
    gson = new Gson();

    // You can't delete messages in Kafka so just writing a set of known messages that can be used for testing
    messagesWritten = new HashMap<String, List<MyMessage>>();
    messagesWritten.put(TOPIC_1, writeKafka(TOPIC_1, 1));
  }

  @AfterClass
  public static void afterClass() {
  }

  private Properties props;
  private CamusJob job;
  private TemporaryFolder folder;
  private String destinationPath;

  @Before
  public void before() throws IOException, NoSuchFieldException, IllegalAccessException {
    resetCamus();

    folder = new TemporaryFolder();
    folder.create();

    String path = folder.getRoot().getAbsolutePath();
    destinationPath = path + DESTINATION_PATH;

    props = new Properties();

    props.setProperty(EtlMultiOutputFormat.ETL_DESTINATION_PATH, destinationPath);
    props.setProperty(CamusJob.ETL_EXECUTION_BASE_PATH, path + EXECUTION_BASE_PATH);
    props.setProperty(CamusJob.ETL_EXECUTION_HISTORY_PATH, path + EXECUTION_HISTORY_PATH);

    props.setProperty(EtlInputFormat.CAMUS_MESSAGE_DECODER_CLASS, JsonStringMessageDecoder.class.getName());
    props.setProperty(EtlMultiOutputFormat.ETL_RECORD_WRITER_PROVIDER_CLASS,
        SequenceFileRecordWriterProvider.class.getName());

    props.setProperty(EtlMultiOutputFormat.ETL_RUN_TRACKING_POST, Boolean.toString(false));
    props.setProperty(CamusJob.KAFKA_CLIENT_NAME, KAFKA_CLIENT_ID);
    props.setProperty(CamusJob.KAFKA_TIMEOUT_VALUE, Integer.toString(KAFKA_TIMEOUT_VALUE));
    props.setProperty(CamusJob.KAFKA_FETCH_BUFFER_SIZE, Integer.toString(KAFKA_BUFFER_SIZE));

    props.setProperty(CamusJob.KAFKA_BROKERS, KAFKA_HOST + ":" + KAFKA_PORT);

    // Run Map/Reduce tests in process for hadoop2  
    props.setProperty("mapreduce.framework.name", "local");
    // Run M/R for Hadoop1
    props.setProperty("mapreduce.jobtracker.address", "local");

    job = new CamusJob(props);
  }

  @After
  public void after() throws IOException {
    EasyMock.verify(_mocks.toArray());
    // Delete all camus data
    folder.delete();
  }

  @Test
  public void runJob1() throws Exception {
    setupJob1();
    
    // Run a second time (no additional messages should be found)
    job = new CamusJob(props);
    job.run(TestEtlInputFormat.class, EtlMultiOutputFormat.class);

    verifyJob1();
  }

  private void setupJob1() {
    // For TopicMetadataResponse
    PartitionMetadata pMeta = EasyMock.createMock(PartitionMetadata.class);
    _mocks.add(pMeta);
    EasyMock.expect(pMeta.errorCode()).andReturn((short)0).anyTimes();
    Broker broker = new Broker(0, "localhost", 2121);
    EasyMock.expect(pMeta.leader()).andReturn(broker).anyTimes();
    EasyMock.expect(pMeta.partitionId()).andReturn(PARTITION_1_ID).anyTimes();
    List<PartitionMetadata> partitionMetadatas = new ArrayList<PartitionMetadata>();
    partitionMetadatas.add(pMeta);    
    TopicMetadata tMeta = EasyMock.createMock(TopicMetadata.class);
    _mocks.add(tMeta);
    EasyMock.expect(tMeta.topic()).andReturn(TOPIC_1).anyTimes();
    EasyMock.expect(tMeta.errorCode()).andReturn((short)0).anyTimes();
    EasyMock.expect(tMeta.partitionsMetadata()).andReturn(partitionMetadatas).anyTimes();
    List<TopicMetadata> topicMetadatas = new ArrayList<TopicMetadata>();
    topicMetadatas.add(tMeta);
    TopicMetadataResponse metadataResponse = EasyMock.createMock(TopicMetadataResponse.class);
    _mocks.add(metadataResponse);
    EasyMock.expect(metadataResponse.topicsMetadata()).andReturn(topicMetadatas).anyTimes();

    // For OffsetResponse
    OffsetResponse offsetResponse = EasyMock.createMock(OffsetResponse.class);
    _mocks.add(offsetResponse);
    // The first call is getLatestOffset, we set the value to 2
    EasyMock.expect(offsetResponse.offsets(EasyMock.anyString(), EasyMock.anyInt())).andReturn(new long[]{LATE_OFFSET}).times(1);
    // The second call is getEarliestOffset, we set the value to 1
    EasyMock.expect(offsetResponse.offsets(EasyMock.anyString(), EasyMock.anyInt())).andReturn(new long[]{EARLY_OFFSET}).times(1);

    // For SimpleConsumer.fetch()
    FetchResponse fetchResponse = EasyMock.createMock(FetchResponse.class);
    EasyMock.expect(fetchResponse.hasError()).andReturn(false).times(1);
    List<MyMessage> myMessages = messagesWritten.get(TOPIC_1);
    MyMessage myMessage = myMessages.get(0);
    String payload = gson.toJson(myMessage);
    String msgKey = Integer.toString(myMessage.number);
    Message message = new Message(payload.getBytes(), msgKey.getBytes());
    List<Message> messages = new ArrayList<Message>();
    messages.add(message);
    ByteBufferMessageSet messageSet = new ByteBufferMessageSet(messages);
    EasyMock.expect(fetchResponse.messageSet(EasyMock.anyString(), EasyMock.anyInt())).andReturn(messageSet).times(1);
    _mocks.add(fetchResponse);
    
    // For SimpleConsumer
    _simpleConsumer = EasyMock.createMock(SimpleConsumer.class);
    _mocks.add(_simpleConsumer);
    EasyMock.expect(_simpleConsumer.send((TopicMetadataRequest)EasyMock.anyObject())).andReturn(metadataResponse).times(1);
    EasyMock.expect(_simpleConsumer.getOffsetsBefore((OffsetRequest)EasyMock.anyObject())).andReturn(offsetResponse).times(2);
    _simpleConsumer.close();
    EasyMock.expectLastCall().andVoid().anyTimes();
    EasyMock.expect(_simpleConsumer.clientId()).andReturn(KAFKA_CLIENT_ID).times(1);
    EasyMock.expect(_simpleConsumer.fetch((FetchRequest)EasyMock.anyObject())).andReturn(fetchResponse).times(1);
    
    EasyMock.replay(_mocks.toArray());
  }
  
  private void verifyJob1() throws Exception {
    assertCamusContains(TOPIC_1);
  }
  
  private void assertCamusContains(String topic) throws InstantiationException, IllegalAccessException, IOException {
    assertCamusContains(topic, messagesWritten.get(topic));
  }

  private void assertCamusContains(String topic, List<MyMessage> messages) throws InstantiationException,
      IllegalAccessException, IOException {
    List<MyMessage> readMessages = readMessages(topic);
    assertThat(readMessages.size(), is(messages.size()));
    assertTrue(readMessages(topic).containsAll(messages));
  }

  private static List<MyMessage> writeKafka(String topic, int numOfMessages) {

    List<MyMessage> messages = new ArrayList<MyMessage>();
    List<KeyedMessage<String, String>> kafkaMessages = new ArrayList<KeyedMessage<String, String>>();

    for (int i = 0; i < numOfMessages; i++) {
      MyMessage msg = new MyMessage(RANDOM.nextInt());
      messages.add(msg);
      kafkaMessages.add(new KeyedMessage<String, String>(topic, Integer.toString(i), gson.toJson(msg)));
    }

    return messages;
  }

  private List<MyMessage> readMessages(String topic) throws IOException, InstantiationException, IllegalAccessException {
    return readMessages(new Path(destinationPath, topic));
  }

  private List<MyMessage> readMessages(Path path) throws IOException, InstantiationException, IllegalAccessException {
    List<MyMessage> messages = new ArrayList<MyMessage>();

    try {
      for (FileStatus file : fs.listStatus(path)) {
        if (file.isDir()) {
          messages.addAll(readMessages(file.getPath()));
        } else {
          SequenceFile.Reader reader = new SequenceFile.Reader(fs, file.getPath(), new Configuration());
          try {
            LongWritable key = (LongWritable) reader.getKeyClass().newInstance();
            Text value = (Text) reader.getValueClass().newInstance();

            while (reader.next(key, value)) {
              messages.add(gson.fromJson(value.toString(), MyMessage.class));
            }
          } finally {
            reader.close();
          }
        }
      }
    } catch (FileNotFoundException e) {
      System.out.println("No camus messages were found in [" + path + "]");
    }

    return messages;
  }

  private static class TestEtlInputFormat extends EtlInputFormat {
    public TestEtlInputFormat() {
      super();
    }

    public static void setLogger(Logger log) {
      EtlInputFormat.setLogger(log);
    }
    
    @Override
    public SimpleConsumer createSimpleConsumer(JobContext context, String host, int port) {
      return _simpleConsumer;
    }
  }
  
  private static class MyMessage {

    private int number;

    // Used by Gson
    public MyMessage() {
    }

    public MyMessage(int number) {
      this.number = number;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof MyMessage))
        return false;

      MyMessage other = (MyMessage) obj;

      return number == other.number;
    }
  }

  private static void resetCamus() throws NoSuchFieldException, IllegalAccessException {
    _mocks.clear();
    _simpleConsumer = null;
    
    // The EtlMultiOutputFormat has a static private field called committer which is only created if null. The problem is this
    // writes the Camus metadata meaning the first execution of the camus job defines where all committed output goes causing us
    // problems if you want to run Camus again using the meta data (i.e. what offsets we processed). Setting it null here forces
    // it to re-instantiate the object with the appropriate output path

    Field field = EtlMultiOutputFormat.class.getDeclaredField("committer");
    field.setAccessible(true);
    field.set(null, null);
  }

  // Kafka Scala layer only provides desearilization as a way to create response, it's very hairy on 
  // the exact order of those bytes.
  private static TopicMetadataResponse createTopicMetadataResponseFromBytes(TopicMetadataRequest request) {      
    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    
    int correlationId = 0;
    byteBuffer.putInt(correlationId);
    
    int brokerId = 0;
    String brokerHost = "localhost";
    int brokerPort = 2012;
    int brokerCount = 1;
    byteBuffer.putInt(brokerCount);
    byteBuffer.putInt(brokerId);
    byteBuffer.putShort((short)brokerHost.length());
    try
    {
      byteBuffer.put(brokerHost.getBytes("UTF-8"));
    }
    catch (UnsupportedEncodingException e)
    {
      throw new RuntimeException(e);
    }
    byteBuffer.putInt(brokerPort);
    
    int topicCount = 1;
    byteBuffer.putInt(topicCount);

    short errorCode = 0;
    byteBuffer.putShort(errorCode);
    String topic = TOPIC_1;
    byteBuffer.putShort((short)topic.length());
    try
    {
      byteBuffer.put(topic.getBytes("UTF-8"));
    }
    catch (UnsupportedEncodingException e)
    {
      throw new RuntimeException(e);
    }
    int partitions = 1;      
    byteBuffer.putInt(partitions);
    byteBuffer.putShort(errorCode);
    int partitionId = 0;
    byteBuffer.putInt(partitionId);
    int leaderId = brokerId;
    byteBuffer.putInt(leaderId);
    int numOfReplicas = 0;
    byteBuffer.putInt(numOfReplicas);
    int numOfInSyncReplicas = 0;
    byteBuffer.putInt(numOfInSyncReplicas);
    
    byteBuffer.rewind();
    
    TopicMetadataResponse response = new TopicMetadataResponse(kafka.api.TopicMetadataResponse.readFrom(byteBuffer));
    return response;
  }

}
