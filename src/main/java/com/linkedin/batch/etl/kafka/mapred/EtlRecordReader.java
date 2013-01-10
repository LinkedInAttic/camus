package com.linkedin.batch.etl.kafka.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.message.Message;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.joda.time.DateTime;

import com.linkedin.batch.etl.kafka.coders.KafkaAvroMessageDecoder;
import com.linkedin.batch.etl.kafka.common.EtlKey;
import com.linkedin.batch.etl.kafka.common.EtlRequest;
import com.linkedin.batch.etl.kafka.common.ExceptionWritable;
import com.linkedin.batch.etl.kafka.common.KafkaReader;
import com.linkedin.batch.etl.kafka.schemaregistry.CachedSchemaResolver;

public class EtlRecordReader extends RecordReader<EtlKey, AvroWrapper<Object>>
{
  private TaskAttemptContext                                 context;

  @SuppressWarnings("rawtypes")
  private Mapper<EtlKey, Writable, EtlKey, Writable>.Context mapperContext;
  private KafkaReader                                        reader;

  private long                                               totalBytes;
  private long                                               readBytes        = 0;

  private boolean                                            skipSchemaErrors = false;
  private KafkaAvroMessageDecoder                            decoder;
  private final BytesWritable                                msgValue         =
                                                                                  new BytesWritable();
  private final EtlKey                                       key              =
                                                                                  new EtlKey();
  private AvroWrapper<Object>                                value            =
                                                                                  new AvroWrapper<Object>(new Object());

  private int                                                maxPullHours     = 0;
  private long                                               maxPullTime      = 0;
  private long                                               beginTimeStamp   = 0;
  private long                                               endTimeStamp     = 0;

  private String                                             statusMsg        = "";

  private Map<String, String>                                alternateMonitorFields;
  private Set<String>                                        ignoreMonitorServiceTopics;

  EtlSplit                                                   split;

  /**
   * Record reader to fetch directly from Kafka
   * 
   * @param split
   * @param job
   * @param reporter
   * @throws IOException
   * @throws InterruptedException
   */
  public EtlRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException
  {
    initialize(split, context);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException
  {
    this.split = (EtlSplit) split;
    this.context = context;

    if (context instanceof Mapper.Context)
    {
      mapperContext = (Context) context;
    }

    this.skipSchemaErrors = EtlInputFormat.getEtlIgnoreSchemaErrors(context);

    if (EtlInputFormat.getKafkaMaxPullHrs(context) != -1)
    {
      this.maxPullHours = EtlInputFormat.getKafkaMaxPullHrs(context);
    }
    else
    {
      this.endTimeStamp = Long.MAX_VALUE;
    }

    if (EtlInputFormat.getKafkaMaxPullMinutesPerTask(context) != -1)
    {
      DateTime now = new DateTime();
      this.maxPullTime =
          now.plusMinutes(EtlInputFormat.getKafkaMaxPullMinutesPerTask(context))
             .getMillis();
    }
    else
    {
      this.maxPullTime = Long.MAX_VALUE;
    }

    if (EtlInputFormat.getKafkaMaxHistoricalDays(context) != -1)
    {
      int maxDays = EtlInputFormat.getKafkaMaxHistoricalDays(context);
      beginTimeStamp = (new DateTime()).minusDays(maxDays).getMillis();
    }
    else
    {
      beginTimeStamp = 0;
    }

    this.totalBytes = this.split.getLength();

    this.alternateMonitorFields = getAlternateFieldsMap(context);

    this.ignoreMonitorServiceTopics = new HashSet<String>();

    String[] ignoreServiceTopics =
        EtlMultiOutputFormat.getEtlAuditIgnoreServiceTopicList(context);
    if (ignoreServiceTopics != null && ignoreServiceTopics.length != 0)
    {
      for (String topic : EtlMultiOutputFormat.getEtlAuditIgnoreServiceTopicList(context))
      {
        ignoreMonitorServiceTopics.add(topic);
      }
    }

    System.out.println("Finished executing the initialize part");
  }

  private Map<String, String> getAlternateFieldsMap(TaskAttemptContext context)
  {
    Map<String, String> map = new HashMap<String, String>();
    String[] arr = EtlInputFormat.getMonitorAlternateFields(context);

    if (arr != null)
    {
      for (String str : arr)
      {
        String[] topic = str.split(":", 2);
        map.put(topic[0], topic[1]);
      }
    }

    return map;
  }

  @Override
  public synchronized void close() throws IOException
  {
    if (reader != null)
    {
      reader.close();
    }
  }

  public Record getRecord(Message msg) throws IOException
  {
    Record r = null;
    try
    {
      r = decoder.toRecord(msg);
    }
    catch (Exception e)
    {
      if (!skipSchemaErrors)
      {
        throw new IOException(e);
      }
    }
    return r;
  }

  public static byte[] getBytes(BytesWritable val)
  {
    byte[] buffer = val.getBytes();

    /*
     * FIXME: remove the following part once the below jira is fixed
     * https://issues.apache.org/jira/browse/HADOOP-6298
     */
    long len = val.getLength();
    byte[] bytes = buffer;
    if (len < buffer.length)
    {
      bytes = new byte[(int) len];
      System.arraycopy(buffer, 0, bytes, 0, (int) len);
    }

    return bytes;
  }

  @Override
  public float getProgress() throws IOException
  {
    if (getPos() == 0)
    {
      return 0f;
    }

    if (getPos() >= totalBytes)
    {
      return 1f;
    }
    return (float) ((double) getPos() / totalBytes);
  }

  private long getTimeStamp(Record record)
  {
    Record header = (Record) record.get("header");

    if (header != null && header.get("time") != null)
    {
      return (Long) header.get("time");
    }
    else if (record.get("timestamp") != null)
    {
      return (Long) record.get("timestamp");
    }
    else
    {
      return System.currentTimeMillis();
    }
  }

  private void setServerSerice(Record record, EtlKey key)
  {
    Record header = (Record) record.get("header");

    if (!ignoreMonitorServiceTopics.contains(key.getTopic()))
    {
      if (header != null)
      {
        key.setServer(header.get("server").toString());
        key.setService(header.get("service").toString());
      }
      else if (alternateMonitorFields.containsKey(key.getTopic()))
      {
        String[] fields = alternateMonitorFields.get(key.getTopic()).split(":");
        key.setServer(record.get(fields[0]).toString());
        key.setService(record.get(fields[1]).toString());
      }
    }
    else
    {
      key.setServer("aggregated");
      key.setService("aggregated");
    }
  }

  private long getPos() throws IOException
  {
    if (reader != null)
    {
      return readBytes + reader.getReadBytes();
    }
    else
    {
      return readBytes;
    }
  }

  @Override
  public EtlKey getCurrentKey() throws IOException,
      InterruptedException
  {
    return key;
  }

  @Override
  public AvroWrapper<Object> getCurrentValue() throws IOException,
      InterruptedException
  {
    return value;
  }

  @Override
  public boolean nextKeyValue() throws IOException,
      InterruptedException
  {

    // we only pull for a specified time. unfinished work will be rescheduled in the next
    // run.
    if (System.currentTimeMillis() > maxPullTime)
    {
      if (reader != null)
      {
        closeReader();
      }
      return false;
    }

    while (true)
    {
      try
      {
        if (reader == null || reader.hasNext() == false)
        {
          EtlRequest request = split.popRequest();
          if (request == null)
          {
            return false;
          }

          if (maxPullHours > 0)
          {
            endTimeStamp = 0;
          }

          key.set(request.getTopic(),
                  request.getNodeId(),
                  request.getPartition(),
                  request.getOffset(),
                  request.getOffset(),
                  0);
          value = new AvroWrapper<Object>(new Object());

          System.out.println("topic:" + request.getTopic() + " partition:"
              + request.getPartition() + " beginOffset:" + request.getOffset()
              + " estimatedLastOffset:" + request.getLastOffset());

          statusMsg += statusMsg.length() > 0 ? "; " : "";
          statusMsg +=
              request.getTopic() + ":" + request.getNodeId() + ":"
                  + request.getPartition();
          context.setStatus(statusMsg);

          if (reader != null)
          {
            closeReader();
          }
          reader =
              new KafkaReader(request,
                              EtlInputFormat.getKafkaClientTimeout(mapperContext),
                              EtlInputFormat.getKafkaClientBufferSize(mapperContext));

          // We even need to get the source information here
          decoder =
              new KafkaAvroMessageDecoder(new CachedSchemaResolver(request.getTopic(), context.getConfiguration()));
          /*
           * decoder = new KafkaAvroMessageDecoder(new
           * CachedSchemaResolver(EtlInputFormat.getEtlSchemaRegistryUrl(context),
           * request.getTopic()));
           */
        }

        while (reader.getNext(key, msgValue))
        {
          context.progress();
          mapperContext.getCounter("total", "data-read").increment(msgValue.getLength());
          mapperContext.getCounter("total", "event-count").increment(1);
          byte[] bytes = getBytes(msgValue);

          // check the checksum of message
          Message message = new Message(bytes);
          long checksum = key.getChecksum();
          if (checksum != message.checksum())
          {
            throw new ChecksumException("Invalid message checksum " + message.checksum()
                + ". Expected " + key.getChecksum(), key.getOffset());
          }

          long tempTime = System.currentTimeMillis();
          Record record;
          try
          {
            record = getRecord(message);
          }
          catch (Exception e)
          {
            mapperContext.write(key, new ExceptionWritable(e));
            continue;
          }

          if (record == null)
          {
            mapperContext.write(key,
                                new ExceptionWritable(new RuntimeException("null record")));
            continue;
          }

          long timeStamp;
          try
          {
            timeStamp = getTimeStamp(record);
            key.setTime(timeStamp);
            setServerSerice(record, key);
          }
          catch (Exception e)
          {
            mapperContext.write(key, new ExceptionWritable(e));
            continue;
          }

          if (timeStamp < beginTimeStamp)
          {
            mapperContext.getCounter("total", "skip-old").increment(1);
          }
          else if (endTimeStamp == 0)
          {
            DateTime time = new DateTime(timeStamp);
            statusMsg += " begin read at " + time.toString();
            context.setStatus(statusMsg);
            System.out.println(key.getTopic() + " begin read at " + time.toString());
            endTimeStamp = (time.plusHours(this.maxPullHours)).getMillis();
          }
          else if (timeStamp > endTimeStamp || System.currentTimeMillis() > maxPullTime)
          {
            statusMsg += " max read at " + new DateTime(timeStamp).toString();
            context.setStatus(statusMsg);
            System.out.println(key.getTopic() + " max read at "
                + new DateTime(timeStamp).toString());
            mapperContext.getCounter("total", "request-time(ms)")
                         .increment(reader.getFetchTime());
            closeReader();
          }

          long secondTime = System.currentTimeMillis();
          value.datum(record);
          long decodeTime = ((secondTime - tempTime));

          mapperContext.getCounter("total", "decode-time(ms)").increment(decodeTime);

          if (reader != null)
          {
            mapperContext.getCounter("total", "request-time(ms)")
                         .increment(reader.getFetchTime());
          }
          return true;
        }
        reader = null;
      }
      catch (Throwable t)
      {
        mapperContext.write(key,
                            new ExceptionWritable(new Exception(t.getLocalizedMessage(),
                                                                t)));
        reader = null;
        continue;
      }
    }
  }

  private void closeReader() throws IOException
  {
    if (reader != null)
    {
      try
      {
        readBytes += reader.getReadBytes();
        reader.close();
      }
      catch (Exception e)
      {
        // not much to do here but skip the task
      }
      finally
      {
        reader = null;
      }
    }
  }
}
