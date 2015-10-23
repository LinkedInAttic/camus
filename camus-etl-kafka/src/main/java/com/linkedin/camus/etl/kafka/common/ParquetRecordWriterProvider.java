package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

/**
 * Provides a RecordWriter that uses AvroParquetWriter to write
 * Parquet records to HDFS. Compression settings are controlled via ETL_OUTPUT_CODEC
 * Supports Snappy & Gzip compression codecs.
 *
 */
public class ParquetRecordWriterProvider implements RecordWriterProvider {
	public final static String EXT = ".parquet";

	public ParquetRecordWriterProvider(TaskAttemptContext context) {
	}

	@Override
	public String getFilenameExtension() {
		return EXT;
	}

	@Override
	public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName,
			CamusWrapper data, FileOutputCommitter committer) throws IOException, InterruptedException {

		CompressionCodecName compressionCodecName = null;
		int blockSize = 256 * 1024 * 1024;
		int pageSize = 64 * 1024;

		if (FileOutputFormat.getCompressOutput(context)) {
			if ("snappy".equals(EtlMultiOutputFormat.getEtlOutputCodec(context))) {
				compressionCodecName = CompressionCodecName.SNAPPY;
			} else {
				compressionCodecName = CompressionCodecName.GZIP;
			}
		}

		Path path = committer.getWorkPath();
		path = new Path(path, EtlMultiOutputFormat.getUniqueFile(context, fileName, EXT));
		Schema avroSchema = ((GenericRecord) data.getRecord()).getSchema();
		final ParquetWriter parquetWriter = new AvroParquetWriter(path, avroSchema, compressionCodecName, blockSize,
				pageSize);

		return new RecordWriter<IEtlKey, CamusWrapper>() {
			@Override
			public void write(IEtlKey ignore, CamusWrapper data) throws IOException {
				parquetWriter.write(data.getRecord());
			}

			@Override
			public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
				parquetWriter.close();
			}
		};
	}
}
