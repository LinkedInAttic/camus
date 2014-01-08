package com.linkedin.camus.sweeper.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

public class CamusSweeperAvroKeyJob extends CamusSweeperJob
{
  @Override
  public void configureJob(String topic, Job job)
  {
    // setting up our input format and map output types
    super.configureInput(job, AvroKeyCombineFileInputFormat.class, AvroKeyMapper.class, AvroKey.class, AvroValue.class);

    // setting up our output format and output types
    super.configureOutput(job, AvroKeyOutputFormat.class, AvroKeyReducer.class, AvroKey.class, NullWritable.class);

    // finding the newest file from our input. this file will contain the newest version of our avro
    // schema.
    Schema schema;
    try
    {
      schema = getNewestSchemaFromSource(job);
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }

    // checking if we have a key schema used for deduping. if we don't then we make this a map only
    // job and set the key schema
    // to the newest input schema
    String keySchemaStr = getConfValue(job, topic, "camus.sweeper.avro.key.schema");
    Schema keySchema;
    if (keySchemaStr == null || keySchemaStr.isEmpty())
    {
      job.setNumReduceTasks(0);
      keySchema = schema;
    }
    else
    {
      keySchema = new Schema.Parser().parse(keySchemaStr);
    }

    setupSchemas(topic, job, schema, keySchema);

    // setting the compression level. Only used if compression is enabled. default is 6
    job.getConfiguration().setInt(AvroOutputFormat.DEFLATE_LEVEL_KEY,
                                  job.getConfiguration().getInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, 6));
  }

  private void setupSchemas(String topic, Job job, Schema schema, Schema keySchema)
  {
    log.info("Input Schema set to " + schema.toString());
    AvroJob.setInputKeySchema(job, schema);

    AvroJob.setMapOutputKeySchema(job, keySchema);
    AvroJob.setMapOutputValueSchema(job, schema);

    Schema reducerSchema =
        new Schema.Parser().parse(getConfValue(job, topic, "camus.output.schema", schema.toString()));
    AvroJob.setOutputKeySchema(job, reducerSchema);
    log.info("Output Schema set to " + reducerSchema.toString());
  }

  private Schema getNewestSchemaFromSource(Job job) throws IOException
  {
    FileSystem fs = FileSystem.get(job.getConfiguration());
    Path[] sourceDirs = FileInputFormat.getInputPaths(job);

    List<FileStatus> files = new ArrayList<FileStatus>();

    for (Path sourceDir : sourceDirs)
    {
      files.addAll(Arrays.asList(fs.listStatus(sourceDir)));
    }

    Collections.sort(files, new LastModifiedComparitor());

    for (FileStatus f : files)
    {
      Schema schema = getNewestSchemaFromSource(f.getPath(), fs);
      if (schema != null)
        return schema;
    }
    return null;
  }

  private Schema getNewestSchemaFromSource(Path sourceDir, FileSystem fs) throws IOException
  {
    FileStatus[] files = fs.listStatus(sourceDir);
    Arrays.sort(files, new LastModifiedComparitor());
    for (FileStatus f : files)
    {
      if (f.isDir())
      {
        Schema schema = getNewestSchemaFromSource(f.getPath(), fs);
        if (schema != null)
          return schema;
      }
      else if (f.getPath().getName().endsWith(".avro"))
      {
        FsInput fi = new FsInput(f.getPath(), fs.getConf());
        GenericDatumReader<GenericRecord> genReader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(fi, genReader);
        return reader.getSchema();
      }
    }
    return null;
  }

  class LastModifiedComparitor implements Comparator<FileStatus>
  {

    @Override
    public int compare(FileStatus o1, FileStatus o2)
    {
      if (o2.getModificationTime() > o1.getModificationTime())
        return -1;
      else if (o2.getModificationTime() < o1.getModificationTime())
        return 1;
      else
        return 0;
    }
  }

}
