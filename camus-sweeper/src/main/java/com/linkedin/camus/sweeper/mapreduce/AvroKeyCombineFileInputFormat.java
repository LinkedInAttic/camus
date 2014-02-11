package com.linkedin.camus.sweeper.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyRecordReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class AvroKeyCombineFileInputFormat extends CombineFileInputFormat<AvroKey<GenericRecord>, NullWritable>
{
  
  @Override
  public List<InputSplit> getSplits(JobContext arg0) throws IOException
  {
    super.setMaxSplitSize(arg0.getConfiguration().getLong("mapred.max.split.size", 471859200));
    super.setMinSplitSizeNode(arg0.getConfiguration().getLong("mapred.min.split.size", 471859200));
    List<InputSplit> splits = super.getSplits(arg0);
    
    List<InputSplit> cleanedSplits = new ArrayList<InputSplit>();
    
    for (int i = 0; i < splits.size(); i++){
        CombineFileSplit oldSplit = (CombineFileSplit) splits.get(i);
        
        String[] locations = oldSplit.getLocations();
        
        if (locations.length > 10)
            locations = Arrays.copyOf(locations, 10);
        
        cleanedSplits.add(new CombineFileSplit(oldSplit.getPaths(), oldSplit.getStartOffsets(), oldSplit.getLengths(), locations));
    }
    return cleanedSplits;
  }

  @Override
  public RecordReader<AvroKey<GenericRecord>, NullWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException
  {
    return new CombineFileRecordReader<AvroKey<GenericRecord>, NullWritable>((CombineFileSplit) arg0,
                                                                             arg1,
                                                                             AvroKeyCombineFileReader.class);
  }

  public static class AvroKeyCombineFileReader extends RecordReader<AvroKey<GenericRecord>, NullWritable>
  {
    AvroKeyRecordReader<GenericRecord> innerReader;
    CombineFileSplit split;
    TaskAttemptContext context;
    Integer idx;

    public AvroKeyCombineFileReader(CombineFileSplit split, TaskAttemptContext arg1, Integer idx) throws IOException,
        InterruptedException
    {
      this.split = split;
      this.context = arg1;
      this.idx = idx;
      Schema schema = AvroJob.getInputKeySchema(arg1.getConfiguration());
      //System.err.println(schema);
      innerReader = new AvroKeyRecordReader<GenericRecord>(schema);
    }

    @Override
    public void close() throws IOException
    {
      innerReader.close();
    }

    @Override
    public AvroKey<GenericRecord> getCurrentKey() throws IOException,
        InterruptedException
    {
      return innerReader.getCurrentKey();
    }

    @Override
    public NullWritable getCurrentValue() throws IOException,
        InterruptedException
    {
      return innerReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException,
        InterruptedException
    {
      return innerReader.getProgress();
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException,
        InterruptedException
    {
      innerReader.initialize(new FileSplit(split.getPath(idx), split.getOffset(idx), split.getLength(idx), null), arg1);
    }

    @Override
    public boolean nextKeyValue() throws IOException,
        InterruptedException
    {
      return innerReader.nextKeyValue();
    }

  }

}
