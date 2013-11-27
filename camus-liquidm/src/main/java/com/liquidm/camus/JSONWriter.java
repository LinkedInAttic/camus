package com.liquidm.camus;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;

public class JSONWriter implements RecordWriterProvider {
  final private Writable _emptyKey = NullWritable.get();

  @Override
  public String getFilenameExtension() {
    return ".json.bz2";
  }

  @SuppressWarnings("rawtypes")
  @Override
  public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName, CamusWrapper data, FileOutputCommitter committer) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    FileSystem fs = FileSystem.get(conf);

    // get the path of the temporary output file
    Path file = committer.getWorkPath();
    file = new Path(file, EtlMultiOutputFormat.getUniqueFile(context, fileName, getFilenameExtension()));

    CompressionCodec codec = new BZip2Codec();
    SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.BLOCK;

    final SequenceFile.Writer writer =
        SequenceFile.createWriter(fs, conf, file, NullWritable.class, String.class, compressionType, codec);

    return new RecordWriter<IEtlKey, CamusWrapper>() {

      @Override
      public void write(IEtlKey iEtlKey, CamusWrapper camusWrapper) throws IOException, InterruptedException {
        writer.append(_emptyKey, new Text((camusWrapper.getRecord().toString() + '\n').getBytes("UTF-8")));
      }

      @Override
      public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        writer.close();
      }
    };
  }
}