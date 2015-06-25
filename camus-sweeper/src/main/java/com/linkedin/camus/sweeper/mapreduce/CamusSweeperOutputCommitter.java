package com.linkedin.camus.sweeper.mapreduce;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A subclass of org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter that uses a custom
 * output file name: x.y.avro, where x = number of records in the file, y = current time.
 */
public class CamusSweeperOutputCommitter extends FileOutputCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(CamusSweeperOutputCommitter.class);

  private final Path outputPath;
  private final FileSystem fs;

  public CamusSweeperOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    this.outputPath = outputPath;
    this.fs = FileSystem.get(context.getConfiguration());
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    long recordCount = getRecordCountCounter(context);
    String fileName = recordCount + "." + System.currentTimeMillis() + ".avro";

    for (FileStatus status : fs.listStatus(super.getWorkPath())) {
      if (status.getPath().getName().endsWith("avro")) {
        LOG.info(String.format("Moving %s to %s", status.getPath(), new Path(outputPath, fileName)));
        fs.rename(status.getPath(), new Path(outputPath, fileName));
      }
    }
    super.commitTask(context);
  }

  private long getRecordCountCounter(TaskAttemptContext context) {
    try {
      //In Hadoop 2, TaskAttemptContext.getCounter() is available
      Method getCounterMethod = context.getClass().getMethod("getCounter", Enum.class);
      return ((Counter) getCounterMethod.invoke(context, AvroKeyReducer.EVENT_COUNTER.RECORD_COUNT)).getValue();
    } catch (NoSuchMethodException e) {
      //In Hadoop 1, TaskAttemptContext.getCounter() is not available
      //Have to cast context to TaskAttemptContext in the mapred package, then get a StatusReporter instance
      org.apache.hadoop.mapred.TaskAttemptContext mapredContext = (org.apache.hadoop.mapred.TaskAttemptContext) context;
      return ((StatusReporter) mapredContext.getProgressible()).getCounter(AvroKeyReducer.EVENT_COUNTER.RECORD_COUNT)
          .getValue();
    } catch (Exception e) {
      throw new RuntimeException("Error reading record count counter", e);
    }
  }

}
