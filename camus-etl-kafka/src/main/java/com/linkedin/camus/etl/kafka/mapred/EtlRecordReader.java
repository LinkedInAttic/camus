package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;
import com.linkedin.camus.etl.kafka.common.KafkaReader;
import com.linkedin.camus.schemaregistry.SchemaNotFoundException;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;


public class EtlRecordReader extends RecordReader<EtlKey, CamusWrapper> {
  private static final String PRINT_MAX_DECODER_EXCEPTIONS = "max.decoder.exceptions.to.print";
  private static final String DEFAULT_SERVER = "server";
  private static final String DEFAULT_SERVICE = "service";
  private static final int RECORDS_TO_READ_AFTER_TIMEOUT = 5;

  public static enum KAFKA_MSG {
    DECODE_SUCCESSFUL,
    SKIPPED_SCHEMA_NOT_FOUND,
    SKIPPED_OTHER
  };

  protected TaskAttemptContext context;

  private EtlInputFormat inputFormat;
  private Mapper<EtlKey, Writable, EtlKey, Writable>.Context mapperContext;
  private KafkaReader reader;

  private long totalBytes;
  private long readBytes = 0;
  private int numRecordsReadForCurrentPartition = 0;
  private long bytesReadForCurrentPartition = 0;

  private boolean skipSchemaErrors = false;
  private MessageDecoder decoder;
  private final BytesWritable msgValue = new BytesWritable();
  private final BytesWritable msgKey = new BytesWritable();
  private final EtlKey key = new EtlKey();
  private CamusWrapper value;

  private int maxPullHours = 0;
  private int exceptionCount = 0;
  private long maxPullTime = 0;
  private long endTimeStamp = 0;
  private long curTimeStamp = 0;
  private long startTime = 0;
  private HashSet<String> ignoreServerServiceList = null;
  private PeriodFormatter periodFormatter = null;

  private String statusMsg = "";

  EtlSplit split;
  private static Logger log = Logger.getLogger(EtlRecordReader.class);

  /**
   * Record reader to fetch directly from Kafka
   *
   * @param split
   * @throws IOException
   * @throws InterruptedException
   */
  public EtlRecordReader(EtlInputFormat inputFormat, InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    this.inputFormat = inputFormat;
    initialize(split, context);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    // For class path debugging
    log.info("classpath: " + System.getProperty("java.class.path"));
    ClassLoader loader = EtlRecordReader.class.getClassLoader();
    log.info("PWD: " + System.getProperty("user.dir"));
    log.info("classloader: " + loader.getClass());
    log.info("org.apache.avro.Schema: " + loader.getResource("org/apache/avro/Schema.class"));

    this.split = (EtlSplit) split;
    this.context = context;

    if (context instanceof Mapper.Context) {
      mapperContext = (Mapper.Context) context;
    }

    this.skipSchemaErrors = EtlInputFormat.getEtlIgnoreSchemaErrors(context);

    if (EtlInputFormat.getKafkaMaxPullHrs(context) != -1) {
      this.maxPullHours = EtlInputFormat.getKafkaMaxPullHrs(context);
    } else {
      this.endTimeStamp = Long.MAX_VALUE;
    }

    if (EtlInputFormat.getKafkaMaxPullMinutesPerTask(context) != -1) {
      this.startTime = System.currentTimeMillis();
      this.maxPullTime =
          new DateTime(this.startTime).plusMinutes(EtlInputFormat.getKafkaMaxPullMinutesPerTask(context)).getMillis();
    } else {
      this.maxPullTime = Long.MAX_VALUE;
    }

    ignoreServerServiceList = new HashSet<String>();
    for (String ignoreServerServiceTopic : EtlInputFormat.getEtlAuditIgnoreServiceTopicList(context)) {
      ignoreServerServiceList.add(ignoreServerServiceTopic);
    }

    this.totalBytes = this.split.getLength();

    this.periodFormatter =
        new PeriodFormatterBuilder().appendMinutes().appendSuffix("m").appendSeconds().appendSuffix("s").toFormatter();
  }

  @Override
  public synchronized void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }

  private CamusWrapper getWrappedRecord(Message message) throws IOException {
    CamusWrapper r = null;
    try {
      r = decoder.decode(message);
      mapperContext.getCounter(KAFKA_MSG.DECODE_SUCCESSFUL).increment(1);
    } catch (SchemaNotFoundException e) {
      mapperContext.getCounter(KAFKA_MSG.SKIPPED_SCHEMA_NOT_FOUND).increment(1);
      if (!skipSchemaErrors) {
        throw new IOException(e);
      }
    } catch (Exception e) {
      mapperContext.getCounter(KAFKA_MSG.SKIPPED_OTHER).increment(1);
      if (!skipSchemaErrors) {
        throw new IOException(e);
      }
    }
    return r;
  }

  private static byte[] getBytes(BytesWritable val) {
    byte[] buffer = val.getBytes();

    /*
     * FIXME: remove the following part once the below jira is fixed
     * https://issues.apache.org/jira/browse/HADOOP-6298
     */
    long len = val.getLength();
    byte[] bytes = buffer;
    if (len < buffer.length) {
      bytes = new byte[(int) len];
      System.arraycopy(buffer, 0, bytes, 0, (int) len);
    }

    return bytes;
  }

  @Override
  public float getProgress() throws IOException {
    if (getPos() == 0) {
      return 0f;
    }

    if (getPos() >= totalBytes) {
      return 1f;
    }
    return (float) ((double) getPos() / totalBytes);
  }

  private long getPos() throws IOException {
    return readBytes;
  }

  @Override
  public EtlKey getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public CamusWrapper getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {

    if (System.currentTimeMillis() > maxPullTime
        && this.numRecordsReadForCurrentPartition >= RECORDS_TO_READ_AFTER_TIMEOUT) {
      String maxMsg = "at " + new DateTime(curTimeStamp).toString();
      log.info("Kafka pull time limit reached");
      statusMsg += " max read " + maxMsg;
      context.setStatus(statusMsg);
      log.info(key.getTopic() + " max read " + maxMsg);
      mapperContext.getCounter("total", "request-time(ms)").increment(reader.getFetchTime());
      closeReader();

      String topicNotFullyPulledMsg =
          String.format("Topic %s:%d not fully pulled, max task time reached %s, pulled %d records", key.getTopic(),
              key.getPartition(), maxMsg, this.numRecordsReadForCurrentPartition);
      mapperContext.write(key, new ExceptionWritable(topicNotFullyPulledMsg));
      log.warn(topicNotFullyPulledMsg);

      String timeSpentOnPartition =
          this.periodFormatter.print(new Duration(this.startTime, System.currentTimeMillis()).toPeriod());
      String timeSpentOnTopicMsg =
          String.format("Time spent on topic %s:%d = %s", key.getTopic(), key.getPartition(), timeSpentOnPartition);
      mapperContext.write(key, new ExceptionWritable(timeSpentOnTopicMsg));
      log.info(timeSpentOnTopicMsg);

      reader = null;
    }

    while (true) {
      try {

        if (reader == null || !reader.hasNext()) {
          if (this.numRecordsReadForCurrentPartition != 0) {
            String timeSpentOnPartition =
                this.periodFormatter.print(new Duration(this.startTime, System.currentTimeMillis()).toPeriod());
            log.info("Time spent on this partition = " + timeSpentOnPartition);
            log.info("Num of records read for this partition = " + this.numRecordsReadForCurrentPartition);
            log.info("Bytes read for this partition = " + this.bytesReadForCurrentPartition);
            log.info("Actual avg size for this partition = " + this.bytesReadForCurrentPartition
                / this.numRecordsReadForCurrentPartition);
          }

          EtlRequest request = (EtlRequest) split.popRequest();
          if (request == null) {
            return false;
          }

          // Reset start time, num of records read and bytes read
          this.startTime = System.currentTimeMillis();
          this.numRecordsReadForCurrentPartition = 0;
          this.bytesReadForCurrentPartition = 0;

          if (maxPullHours > 0) {
            endTimeStamp = 0;
          }

          key.set(request.getTopic(), request.getLeaderId(), request.getPartition(), request.getOffset(),
              request.getOffset(), 0);
          value = null;
          log.info("\n\ntopic:" + request.getTopic() + " partition:" + request.getPartition() + " beginOffset:"
              + request.getOffset() + " estimatedLastOffset:" + request.getLastOffset());
          statusMsg += statusMsg.length() > 0 ? "; " : "";
          statusMsg += request.getTopic() + ":" + request.getLeaderId() + ":" + request.getPartition();
          context.setStatus(statusMsg);

          if (reader != null) {
            closeReader();
          }
          reader =
              new KafkaReader(inputFormat, context, request, CamusJob.getKafkaTimeoutValue(mapperContext),
                  CamusJob.getKafkaBufferSize(mapperContext));

          decoder = createDecoder(request.getTopic());
        }
        int count = 0;
        Message message;
        while ((message = reader.getNext(key)) != null) {
          readBytes += key.getMessageSize();
          count++;
          this.numRecordsReadForCurrentPartition++;
          this.bytesReadForCurrentPartition += key.getMessageSize();
          context.progress();
          mapperContext.getCounter("total", "data-read").increment(message.getPayload().length);
          mapperContext.getCounter("total", "event-count").increment(1);

          message.validate();

          long tempTime = System.currentTimeMillis();
          CamusWrapper wrapper;
          try {
            wrapper = getWrappedRecord(message);

            if (wrapper == null) {
              throw new RuntimeException("null record");
            }
          } catch (Exception e) {
            if (exceptionCount < getMaximumDecoderExceptionsToPrint(context)) {
              mapperContext.write(key, new ExceptionWritable(e));
              log.info(e.getMessage());
              exceptionCount++;
            } else if (exceptionCount == getMaximumDecoderExceptionsToPrint(context)) {
              log.info("The same exception has occured for more than " + getMaximumDecoderExceptionsToPrint(context)
                  + " records. All further exceptions will not be printed");
            }
            if (System.currentTimeMillis() > maxPullTime) {
              exceptionCount = 0;
              break;
            }
            continue;
          }

          curTimeStamp = wrapper.getTimestamp();
          try {
            key.setTime(curTimeStamp);
            key.addAllPartitionMap(wrapper.getPartitionMap());
            setServerService();
          } catch (Exception e) {
            mapperContext.write(key, new ExceptionWritable(e));
            continue;
          }

          if (endTimeStamp == 0) {
            DateTime time = new DateTime(curTimeStamp);
            statusMsg += " begin read at " + time.toString();
            context.setStatus(statusMsg);
            log.info(key.getTopic() + " begin read at " + time.toString());
            endTimeStamp = (time.plusHours(this.maxPullHours)).getMillis();
          } else if (curTimeStamp > endTimeStamp) {
            String maxMsg = "at " + new DateTime(curTimeStamp).toString();
            log.info("Kafka Max history hours reached");
            mapperContext.write(
                key,
                new ExceptionWritable(String.format(
                    "Topic not fully pulled, max task time reached %s, pulled %d records", maxMsg,
                    this.numRecordsReadForCurrentPartition)));
            statusMsg += " max read " + maxMsg;
            context.setStatus(statusMsg);
            log.info(key.getTopic() + " max read " + maxMsg);
            mapperContext.getCounter("total", "request-time(ms)").increment(reader.getFetchTime());
            closeReader();
          }

          long secondTime = System.currentTimeMillis();
          value = wrapper;
          long decodeTime = ((secondTime - tempTime));

          mapperContext.getCounter("total", "decode-time(ms)").increment(decodeTime);

          if (reader != null) {
            mapperContext.getCounter("total", "request-time(ms)").increment(reader.getFetchTime());
          }
          return true;
        }
        log.info("Records read : " + count);
        count = 0;
        reader = null;
      } catch (Throwable t) {
        Exception e = new Exception(t.getLocalizedMessage(), t);
        e.setStackTrace(t.getStackTrace());
        mapperContext.write(key, new ExceptionWritable(e));
        reader = null;
        continue;
      }
    }
  }

  protected MessageDecoder createDecoder(String topic) {
    return MessageDecoderFactory.createMessageDecoder(context, topic);
  }

  private void closeReader() throws IOException {
    if (reader != null) {
      try {
        reader.close();
      } catch (Exception e) {
        // not much to do here but skip the task
      } finally {
        reader = null;
      }
    }
  }

  public void setServerService() {
    if (ignoreServerServiceList.contains(key.getTopic()) || ignoreServerServiceList.contains("all")) {
      key.setServer(DEFAULT_SERVER);
      key.setService(DEFAULT_SERVICE);
    }
  }

  public static int getMaximumDecoderExceptionsToPrint(JobContext job) {
    return job.getConfiguration().getInt(PRINT_MAX_DECODER_EXCEPTIONS, 10);
  }
}
