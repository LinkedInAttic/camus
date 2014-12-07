package com.linkedin.camus.sweeper.mapreduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyRecordReader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.linkedin.camus.sweeper.utils.RelaxedSchemaUtils;


public class AvroKeyCombineFileInputFormat extends CombineFileInputFormat<AvroKey<GenericRecord>, NullWritable> {
  private static final long GET_SPLIT_NUM_FILES_TRHESHOLD_DEFAULT = 5000;

  @Override
  public List<InputSplit> getSplits(JobContext arg0) throws IOException {
    FileSystem fs = FileSystem.get(arg0.getConfiguration());

    super.setMaxSplitSize(arg0.getConfiguration().getLong("mapred.max.split.size", 471859200));
    super.setMinSplitSizeNode(arg0.getConfiguration().getLong("mapred.min.split.size", 471859200));

    List<InputSplit> splits = new ArrayList<InputSplit>();

    addAllSplits(arg0, Arrays.asList(getInputPaths(arg0)), splits, fs);

    List<InputSplit> cleanedSplits = new ArrayList<InputSplit>();

    for (int i = 0; i < splits.size(); i++) {
      CombineFileSplit oldSplit = (CombineFileSplit) splits.get(i);
      String[] locations = oldSplit.getLocations();

      if (locations.length > 10)
        locations = Arrays.copyOf(locations, 10);

      cleanedSplits.add(new CombineFileSplit(oldSplit.getPaths(), oldSplit.getStartOffsets(), oldSplit.getLengths(),
          locations));
    }
    return cleanedSplits;
  }

  private void addAllSplits(JobContext job, List<Path> dirs, List<InputSplit> splits, FileSystem fs)
      throws FileNotFoundException, IOException {
    List<Path> subdirs = new ArrayList<Path>();
    long fileCount = 0;

    for (Path input : dirs) {
      long count = fs.getContentSummary(input).getFileCount();
      subdirs.add(input);
      if (fileCount + count < GET_SPLIT_NUM_FILES_TRHESHOLD_DEFAULT) {
        fileCount += count;
      } else {
        List<Path> files = new ArrayList<Path>();
        addAllAvro(files, subdirs, fs);
        Job innerJob = new Job(job.getConfiguration());
        setInputPaths(innerJob, files.toArray(new Path[files.size()]));

        splits.addAll(super.getSplits(innerJob));
        subdirs.clear();
        fileCount = 0;
      }
    }

    if (fileCount > 0) {
      List<Path> files = new ArrayList<Path>();
      addAllAvro(files, subdirs, fs);
      Job innerJob = new Job(job.getConfiguration());
      setInputPaths(innerJob, files.toArray(new Path[files.size()]));

      splits.addAll(super.getSplits(innerJob));
    }
  }

  private void addAllAvro(List<Path> files, List<Path> dirs, FileSystem fs) throws FileNotFoundException, IOException {
    for (Path dir : dirs) {
      for (FileStatus f : fs.listStatus(dir)) {
        if (f.isDir())
          addAllAvro(files, Collections.singletonList(f.getPath()), fs);
        else if (f.getPath().getName().endsWith(".avro"))
          files.add(f.getPath());
      }
    }
  }

  @Override
  public RecordReader<AvroKey<GenericRecord>, NullWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
      throws IOException {
    return new CombineFileRecordReader<AvroKey<GenericRecord>, NullWritable>((CombineFileSplit) arg0, arg1,
        AvroKeyCombineFileReader.class);
  }

  public static class AvroKeyCombineFileReader extends RecordReader<AvroKey<GenericRecord>, NullWritable> {
    AvroKeyRecordReader<GenericRecord> innerReader;
    CombineFileSplit split;
    TaskAttemptContext context;
    Integer idx;

    public AvroKeyCombineFileReader(CombineFileSplit split, TaskAttemptContext arg1, Integer idx) throws IOException,
        InterruptedException {
      this.split = split;
      this.context = arg1;
      this.idx = idx;
      Schema schema = RelaxedSchemaUtils.getInputKeySchema(arg1.getConfiguration());
      //System.err.println(schema);
      innerReader = new AvroKeyRecordReader<GenericRecord>(schema);
    }

    @Override
    public void close() throws IOException {
      innerReader.close();
    }

    @Override
    public AvroKey<GenericRecord> getCurrentKey() throws IOException, InterruptedException {
      return innerReader.getCurrentKey();
    }

    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
      return innerReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return innerReader.getProgress();
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
      innerReader.initialize(new FileSplit(split.getPath(idx), split.getOffset(idx), split.getLength(idx), null), arg1);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return innerReader.nextKeyValue();
    }

  }

}
