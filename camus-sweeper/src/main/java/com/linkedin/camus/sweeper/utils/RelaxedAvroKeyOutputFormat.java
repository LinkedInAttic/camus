package com.linkedin.camus.sweeper.utils;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroKeyRecordWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.linkedin.camus.sweeper.mapreduce.CamusSweeperOutputCommitter;


/**
 * The AvroKeyOutputFormat class to relax the name validation
 * 
 * @author hcai
 *
 * @param <T> The Java type of the Avro data to serialize.
 */
public class RelaxedAvroKeyOutputFormat<T> extends AvroKeyOutputFormat<T> {
  private static final Log LOG = LogFactory.getLog(RelaxedAvroKeyOutputFormat.class.getName());

  private static final String CONF_OUTPUT_KEY_SCHEMA = "avro.schema.output.key";

  private FileOutputCommitter commiter = null;

  @Override
  public RecordWriter<AvroKey<T>, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException {
    LOG.info("getRecordWriter for" + context);
    // Get the writer schema.
    String schemaString = context.getConfiguration().get(CONF_OUTPUT_KEY_SCHEMA);
    Schema writerSchema =
        schemaString != null ? RelaxedSchemaUtils.parseSchema(schemaString, context.getConfiguration()) : null;

    if (null == writerSchema) {
      throw new IOException("AvroKeyOutputFormat requires an output schema. Use AvroJob.setOutputKeySchema().");
    }

    return new AvroKeyRecordWriter<T>(writerSchema, GenericData.get(), getCompressionCodec(context),
        getAvroFileOutputStream(context));

  }

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    if (this.commiter == null) {
      this.commiter = new CamusSweeperOutputCommitter(FileOutputFormat.getOutputPath(context), context);
    }
    return this.commiter;
  }

}
