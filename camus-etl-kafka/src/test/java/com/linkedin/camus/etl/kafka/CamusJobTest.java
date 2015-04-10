package com.linkedin.camus.etl.kafka;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.linkedin.camus.etl.kafka.coders.FailDecoder;
import com.linkedin.camus.etl.kafka.coders.JsonStringMessageDecoder;
import com.linkedin.camus.etl.kafka.common.SequenceFileRecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.gson.Gson;


public class CamusJobTest {

  private static final Random RANDOM = new Random();

  private static final String BASE_PATH = "/camus";
  private static final String DESTINATION_PATH = BASE_PATH + "/destination";
  private static final String EXECUTION_BASE_PATH = BASE_PATH + "/execution";
  private static final String EXECUTION_HISTORY_PATH = EXECUTION_BASE_PATH + "/history";

  private static final String TOPIC_1 = "topic_1";
  private static final String TOPIC_2 = "topic_2";
  private static final String TOPIC_3 = "topic_3";

  private static KafkaCluster cluster;
  private static FileSystem fs;
  private static Gson gson;
  private static Map<String, List<Message>> messagesWritten;

  @BeforeClass
  public static void beforeClass() throws IOException {
    cluster = new KafkaCluster();
    fs = FileSystem.get(new Configuration());
    gson = new Gson();

    // You can't delete messages in Kafka so just writing a set of known messages that can be used for testing
    messagesWritten = new HashMap<String, List<Message>>();
    messagesWritten.put(TOPIC_1, writeKafka(TOPIC_1, 10));
    messagesWritten.put(TOPIC_2, writeKafka(TOPIC_2, 10));
    messagesWritten.put(TOPIC_3, writeKafka(TOPIC_3, 10));
  }

  @AfterClass
  public static void afterClass() {
    cluster.shutdown();
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

    props = cluster.getProps();

    props.setProperty(EtlMultiOutputFormat.ETL_DESTINATION_PATH, destinationPath);
    props.setProperty(CamusJob.ETL_EXECUTION_BASE_PATH, path + EXECUTION_BASE_PATH);
    props.setProperty(CamusJob.ETL_EXECUTION_HISTORY_PATH, path + EXECUTION_HISTORY_PATH);

    props.setProperty(EtlInputFormat.CAMUS_MESSAGE_DECODER_CLASS, JsonStringMessageDecoder.class.getName());
    props.setProperty(EtlMultiOutputFormat.ETL_RECORD_WRITER_PROVIDER_CLASS,
        SequenceFileRecordWriterProvider.class.getName());

    props.setProperty(EtlMultiOutputFormat.ETL_RUN_TRACKING_POST, Boolean.toString(false));
    props.setProperty(CamusJob.KAFKA_CLIENT_NAME, "Camus");

    props.setProperty(CamusJob.KAFKA_BROKERS, props.getProperty("metadata.broker.list"));

    // Run Map/Reduce tests in process for hadoop2
    props.setProperty("mapreduce.framework.name", "local");
    // Run M/R for Hadoop1
    props.setProperty("mapreduce.jobtracker.address", "local");

    job = new CamusJob(props);
  }

  @After
  public void after() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException,
    IllegalAccessException {
    // Delete all camus data
    folder.delete();
    Field field = EtlMultiOutputFormat.class.getDeclaredField("committer");
    field.setAccessible(true);
    field.set(null, null);
  }

  @Test
  public void runJob() throws Exception {
    job.run();

    assertCamusContains(TOPIC_1);
    assertCamusContains(TOPIC_2);
    assertCamusContains(TOPIC_3);

    // Run a second time (no additional messages should be found)
    job = new CamusJob(props);
    job.run();

    assertCamusContains(TOPIC_1);
    assertCamusContains(TOPIC_2);
    assertCamusContains(TOPIC_3);
  }

  @Test
  public void runJobWithoutErrorsAndFailOnErrors() throws Exception {
    props.setProperty(CamusJob.ETL_FAIL_ON_ERRORS, Boolean.TRUE.toString());
    job = new CamusJob(props);
    runJob();
  }

  @Test(expected = RuntimeException.class)
  public void runJobWithErrorsAndFailOnErrors() throws Exception {
    props.setProperty(CamusJob.ETL_FAIL_ON_ERRORS, Boolean.TRUE.toString());
    props.setProperty(EtlInputFormat.CAMUS_MESSAGE_DECODER_CLASS, FailDecoder.class.getName());
    props.setProperty(CamusJob.ETL_MAX_PERCENT_SKIPPED_OTHER, "100.0");
    job = new CamusJob(props);
    job.run();
  }

  private void assertCamusContains(String topic) throws InstantiationException, IllegalAccessException, IOException {
    assertCamusContains(topic, messagesWritten.get(topic));
  }

  private void assertCamusContains(String topic, List<Message> messages) throws InstantiationException,
      IllegalAccessException, IOException {
    List<Message> readMessages = readMessages(topic);
    assertThat(readMessages.size(), is(messages.size()));
    assertTrue(readMessages(topic).containsAll(messages));
  }

  private static List<Message> writeKafka(String topic, int numOfMessages) {

    List<Message> messages = new ArrayList<Message>();
    List<KeyedMessage<String, String>> kafkaMessages = new ArrayList<KeyedMessage<String, String>>();

    for (int i = 0; i < numOfMessages; i++) {
      Message msg = new Message(RANDOM.nextInt());
      messages.add(msg);
      kafkaMessages.add(new KeyedMessage<String, String>(topic, Integer.toString(i), gson.toJson(msg)));
    }

    Properties producerProps = cluster.getProps();

    producerProps.setProperty("serializer.class", StringEncoder.class.getName());
    producerProps.setProperty("key.serializer.class", StringEncoder.class.getName());

    Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(producerProps));

    try {
      producer.send(kafkaMessages);
    } finally {
      producer.close();
    }

    return messages;
  }

  private List<Message> readMessages(String topic) throws IOException, InstantiationException, IllegalAccessException {
    return readMessages(new Path(destinationPath, topic));
  }

  private List<Message> readMessages(Path path) throws IOException, InstantiationException, IllegalAccessException {
    List<Message> messages = new ArrayList<Message>();

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
              messages.add(gson.fromJson(value.toString(), Message.class));
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

  private static class Message {

    private int number;

    // Used by Gson
    public Message() {
    }

    public Message(int number) {
      this.number = number;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof Message))
        return false;

      Message other = (Message) obj;

      return number == other.number;
    }
  }

  private static void resetCamus() throws NoSuchFieldException, IllegalAccessException {
    // The EtlMultiOutputFormat has a static private field called committer which is only created if null. The problem is this
    // writes the Camus metadata meaning the first execution of the camus job defines where all committed output goes causing us
    // problems if you want to run Camus again using the meta data (i.e. what offsets we processed). Setting it null here forces
    // it to re-instantiate the object with the appropriate output path

    Field field = EtlMultiOutputFormat.class.getDeclaredField("committer");
    field.setAccessible(true);
    field.set(null, null);
  }

}