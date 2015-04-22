package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class AvroParquetFileRecordWriterProvider implements RecordWriterProvider {
  public final static String EXT = ".parquet";
  public static final String ETL_OUTPUT_PARQUET_DICTIONARY = "etl.output.parquet.dictionary";
  public static final String ETL_OUTPUT_PARQUET_BLOCK_SIZE = "etl.output.parquet.blocksize";
  public static final String ETL_OUTPUT_PARQUET_PAGE_SIZE = "etl.output.parquet.pagesize";
  public static final String ETL_OUTPUT_PARQUET_COMPRESSION_TYPE = "etl.output.parquet.compression";

  protected Boolean dictionaryEnabled;
  protected int blockSize;
  protected int pageSize;
  protected CompressionCodecName compressionCodec;

  @Override
  public String getFilenameExtension() {
    return EXT;
  }

  public AvroParquetFileRecordWriterProvider(TaskAttemptContext context) {
    Configuration conf = context.getConfiguration();
    dictionaryEnabled = conf.getBoolean(ETL_OUTPUT_PARQUET_DICTIONARY, true);
    blockSize = conf.getInt(ETL_OUTPUT_PARQUET_BLOCK_SIZE, AvroParquetWriter.DEFAULT_BLOCK_SIZE);
    pageSize = conf.getInt(ETL_OUTPUT_PARQUET_PAGE_SIZE, AvroParquetWriter.DEFAULT_PAGE_SIZE);
    compressionCodec = CompressionCodecName.fromConf(conf.get(ETL_OUTPUT_PARQUET_COMPRESSION_TYPE, "uncompressed"));
  }

  @Override
  public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName,
                                                                 CamusWrapper camusWrapper, FileOutputCommitter committer) throws IOException, InterruptedException {
    Schema schema = ((GenericRecord) camusWrapper.getRecord()).getSchema();
    Path path = new Path(committer.getWorkPath(), EtlMultiOutputFormat.getUniqueFile(context, fileName, EXT));

    final AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(
        path, schema, compressionCodec, blockSize, pageSize, dictionaryEnabled);

    return new RecordWriter<IEtlKey, CamusWrapper>() {

      @Override
      public void write(IEtlKey iEtlKey, CamusWrapper camusWrapper) throws IOException, InterruptedException {
        writer.write((GenericRecord) camusWrapper.getRecord());
      }

      @Override
      public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        writer.close();
      }
    };
  }
}
