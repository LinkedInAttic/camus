package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

public class StringRecordWriterProvider implements RecordWriterProvider {
	public final static String EXT = ".gz";

	public StringRecordWriterProvider(TaskAttemptContext context) {
	}

	@Override
	public String getFilenameExtension() {
		return EXT;
	}

	@Override
	public RecordWriter<IEtlKey, CamusWrapper<String>> getDataRecordWriter(
			TaskAttemptContext context, String fileName,
			CamusWrapper<String> data, FileOutputCommitter committer)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);

		Path path = committer.getWorkPath();
		path = new Path(path, EtlMultiOutputFormat.getUniqueFile(context,
				fileName, EXT));

		CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
		CompressionCodec codec = codecFactory.getCodec(path);

		final CompressionOutputStream writer = codec.createOutputStream(fs
				.create(path));

		return new RecordWriter<IEtlKey, CamusWrapper<String>>() {
			@Override
			public void write(IEtlKey ignore, CamusWrapper<String> data)
					throws IOException {
				String record = (String) data.getRecord() + "\n";
				writer.write(record.getBytes());
			}

			@Override
			public void close(TaskAttemptContext arg0) throws IOException,
					InterruptedException {
				writer.close();
			}
		};
	}
}
