package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;


public class EtlMultiOutputRecordWriter extends RecordWriter<EtlKey, Object> {
  private TaskAttemptContext context;
  private Writer errorWriter = null;
  private String currentTopic = "";
  private long beginTimeStamp = 0;
  private static Logger log = Logger.getLogger(EtlMultiOutputRecordWriter.class);

  private HashMap<String, RecordWriter<IEtlKey, CamusWrapper>> dataWriters =
      new HashMap<String, RecordWriter<IEtlKey, CamusWrapper>>();

  private EtlMultiOutputCommitter committer;

  public EtlMultiOutputRecordWriter(TaskAttemptContext context, EtlMultiOutputCommitter committer) throws IOException,
      InterruptedException {
    this.context = context;
    this.committer = committer;
    errorWriter =
        SequenceFile.createWriter(
            FileSystem.get(context.getConfiguration()),
            context.getConfiguration(),
            new Path(committer.getWorkPath(), EtlMultiOutputFormat.getUniqueFile(context,
                EtlMultiOutputFormat.ERRORS_PREFIX, "")), EtlKey.class, ExceptionWritable.class);

    if (EtlInputFormat.getKafkaMaxHistoricalDays(context) != -1) {
      int maxDays = EtlInputFormat.getKafkaMaxHistoricalDays(context);
      beginTimeStamp = (new DateTime()).minusDays(maxDays).getMillis();
    } else {
      beginTimeStamp = 0;
    }
    log.info("beginTimeStamp set to: " + beginTimeStamp);
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
        //TODO: fix this logging message, should be logged once as a total count of old records skipped for each topic
        // for now, commenting this out
        //log.warn("Key's time: " + key + " is less than beginTime: " + beginTimeStamp);
        ((Mapper.Context) context).getCounter("total", "skip-old").increment(1);
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
        dataWriters.get(workingFileName).write(key, value);
      }
    } else if (val instanceof ExceptionWritable) {
      committer.addOffset(key);
      log.warn("ExceptionWritable key: " + key + " value: " + val);
      errorWriter.append(key, (ExceptionWritable) val);
    } else {
      log.warn("Unknow type of record: " + val);
    }
  }

  private RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName,
      CamusWrapper value) throws IOException, InterruptedException {
    RecordWriterProvider recordWriterProvider = null;
    try {
      //recordWriterProvider = EtlMultiOutputFormat.getRecordWriterProviderClass(context).newInstance();
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
