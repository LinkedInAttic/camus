package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.EtlRecordListener;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;

public class EtlMultiOutputRecordWriter extends RecordWriter<EtlKey, Object> {
  private final TaskAttemptContext context;
  private Writer errorWriter = null;
  private String currentTopic = "";
  private long beginTimeStamp = 0;
  private static Logger log = Logger.getLogger(EtlMultiOutputRecordWriter.class);
  private final Counter topicSkipOldCounter;

  private final HashMap<String, RecordWriter<IEtlKey, CamusWrapper>> dataWriters = new HashMap<String, RecordWriter<IEtlKey, CamusWrapper>>();

  private final EtlMultiOutputCommitter committer;

  public EtlMultiOutputRecordWriter(TaskAttemptContext context, EtlMultiOutputCommitter committer)
      throws IOException, InterruptedException {
    this.context = context;
    this.committer = committer;
    errorWriter = SequenceFile.createWriter(FileSystem.get(context.getConfiguration()), context.getConfiguration(),
        new Path(committer.getWorkPath(),
            FileOutputFormat.getUniqueFile(context, EtlMultiOutputFormat.ERRORS_PREFIX, "")),
        EtlKey.class, ExceptionWritable.class);

    if (EtlInputFormat.getKafkaMaxHistoricalDays(context) != -1) {
      int maxDays = EtlInputFormat.getKafkaMaxHistoricalDays(context);
      beginTimeStamp = (new DateTime()).minusDays(maxDays).getMillis();
    } else {
      beginTimeStamp = 0;
    }
    log.info("beginTimeStamp set to: " + beginTimeStamp);
    topicSkipOldCounter = getTopicSkipOldCounter();
  }

  private Counter getTopicSkipOldCounter() {
    try {
      // In Hadoop 2, TaskAttemptContext.getCounter() is available
      Method getCounterMethod = context.getClass().getMethod("getCounter", String.class, String.class);
      return ((Counter) getCounterMethod.invoke(context, "total", "skip-old"));
    } catch (NoSuchMethodException e) {
      // In Hadoop 1, TaskAttemptContext.getCounter() is not available
      // Have to cast context to TaskAttemptContext in the mapred package, then get a StatusReporter instance
      org.apache.hadoop.mapred.TaskAttemptContext mapredContext = (org.apache.hadoop.mapred.TaskAttemptContext) context;
      return ((StatusReporter) mapredContext.getProgressible()).getCounter("total", "skip-old");
    } catch (IllegalArgumentException e) {
      log.error("IllegalArgumentException while obtaining counter 'total:skip-old': " + e.getMessage());
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      log.error("IllegalAccessException while obtaining counter 'total:skip-old': " + e.getMessage());
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      log.error("InvocationTargetException obtaining counter 'total:skip-old': " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    for (String w : dataWriters.keySet()) {
      dataWriters.get(w).close(context);
    }
    errorWriter.close();
  }

  @Override
  public void write(EtlKey key, Object val) throws IOException, InterruptedException {
    if (val instanceof CamusWrapper<?>) {
      if (key.getTime() < beginTimeStamp) {
        // TODO: fix this logging message, should be logged once as a total count of old records skipped for each topic
        // for now, commenting this out
        // log.warn("Key's time: " + key + " is less than beginTime: " + beginTimeStamp);
        topicSkipOldCounter.increment(1);
        committer.addOffset(key);
      } else {
        if (!key.getTopic().equals(currentTopic)) {
          for (RecordWriter<IEtlKey, CamusWrapper> writer : dataWriters.values()) {
            writer.close(context);
          }
          dataWriters.clear();
          currentTopic = key.getTopic();
        }

        committer.addCounts(key);
        CamusWrapper value = (CamusWrapper) val;
        String workingFileName = EtlMultiOutputFormat.getWorkingFileName(context, key);
        if (!dataWriters.containsKey(workingFileName)) {
          dataWriters.put(workingFileName, getDataRecordWriter(context, workingFileName, value));
          log.info("Writing to data file: " + workingFileName);
        }

        try {
          writeAttempt(context, key);
          dataWriters.get(workingFileName).write(key, value);
          writeSuccess(context, key);
        } catch (IOException e) {
          writeFailure(context, key, e);
          throw e;
        } catch (InterruptedException e) {
          writeFailure(context, key, e);
          throw e;
        } catch (RuntimeException e) {
          writeFailure(context, key, e);
          throw e;
        }

      }
    } else if (val instanceof ExceptionWritable) {
      committer.addOffset(key);
      log.warn("ExceptionWritable key: " + key + " value: " + val);
      errorWriter.append(key, (ExceptionWritable) val);
    } else {
      log.warn("Unknow type of record: " + val);
    }
  }

  private static void writeAttempt(TaskAttemptContext context, EtlKey key) {
    try {
      EtlRecordListener listener = EtlMultiOutputFormat.getEltRecordWriterListener(context, key.getTopic());
      if (listener != null) {
        listener.onNew(context, key);
      }
    } catch (Exception e) {
      log.error("Unable to notify new record write to listener", e);
    }
  }

  private static void writeSuccess(TaskAttemptContext context, EtlKey key) {
    try {
      EtlRecordListener listener = EtlMultiOutputFormat.getEltRecordWriterListener(context, key.getTopic());
      if (listener != null) {
        listener.onSuccess(context, key);
      }
    } catch (Exception e) {
      log.error("Unable to notify successful record write to listener", e);
    }
  }

  private static void writeFailure(TaskAttemptContext context, EtlKey key, Exception ex) {
    try {
      EtlRecordListener listener = EtlMultiOutputFormat.getEltRecordWriterListener(context, key.getTopic());
      if (listener != null) {
        listener.onFailure(context, key, ex);
      }
    } catch (Exception e) {
      log.error("Unable to notify failed record write to listener: " + ex.getMessage(), e);
    }
  }

  private RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName,
      CamusWrapper value) throws IOException, InterruptedException {
    RecordWriterProvider recordWriterProvider = null;
    try {
      // recordWriterProvider = EtlMultiOutputFormat.getRecordWriterProviderClass(context).newInstance();
      Class<RecordWriterProvider> rwp = EtlMultiOutputFormat.getRecordWriterProviderClass(context);
      Constructor<RecordWriterProvider> crwp = rwp.getConstructor(TaskAttemptContext.class);
      recordWriterProvider = crwp.newInstance(context);
    } catch (InstantiationException e) {
      throw new IllegalStateException(e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return recordWriterProvider.getDataRecordWriter(context, fileName, value, committer);
  }
}
