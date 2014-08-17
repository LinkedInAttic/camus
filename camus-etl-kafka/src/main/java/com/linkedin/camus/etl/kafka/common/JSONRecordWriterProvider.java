package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

public class JSONRecordWriterProvider implements RecordWriterProvider {
  final private Writable _emptyKey = NullWritable.get();

  @Override
  public String getFilenameExtension() {
    return ".gz";
  }

  @SuppressWarnings("rawtypes")
  @Override
  public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName, CamusWrapper data, FileOutputCommitter committer) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    FileSystem fs = FileSystem.get(conf);

    // get the path of the temporary output file
    Path file = committer.getWorkPath();
    file = new Path(file, EtlMultiOutputFormat.getUniqueFile(context, fileName, getFilenameExtension()));

    CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
    CompressionCodec codec = codecFactory.getCodec(file);

    final CompressionOutputStream writer = codec.createOutputStream(fs.create(file));

    return new RecordWriter<IEtlKey, CamusWrapper>() {
      @Override
      public void write(IEtlKey iEtlKey, CamusWrapper camusWrapper) throws IOException, InterruptedException {
        writer.write((camusWrapper.getRecord().toString() + '\n').getBytes("UTF-8"));
      }

      @Override
      public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        writer.close();
      }
    };
  }
}
