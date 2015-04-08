package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.RecordWriterWithCloseStatus;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

/**
 * Provides a RecordWriter that uses SequenceFile.Writer to write
 * SequenceFiles records to HDFS.  Compression settings are controlled via
 * the usual hadoop configuration values.
 *
 * - mapreduce.output.fileoutputformat.compress         - true or false
 * - mapreduce.output.fileoutputformat.compress.codec   - org.apache.hadoop.io.compress.* (SnappyCodec, etc.)
 * - mapreduce.output.fileoutputformat.compress.type    - BLOCK or RECORD
 *
 */
public class SequenceFileRecordWriterProvider implements RecordWriterProvider {
    public static final String ETL_OUTPUT_RECORD_DELIMITER = "etl.output.record.delimiter";
    public static final String DEFAULT_RECORD_DELIMITER = "";

    private static Logger log = Logger.getLogger(SequenceFileRecordWriterProvider.class);

    protected String recordDelimiter = null;
        
    public SequenceFileRecordWriterProvider(TaskAttemptContext  context) {
    	
    }

    // TODO: Make this configurable somehow.
    // To do this, we'd have to make SequenceFileRecordWriterProvider have an
    // init(JobContext context) method signature that EtlMultiOutputFormat would always call.
    @Override
    public String getFilenameExtension() {
        return "";
    }

    @SuppressWarnings("rawtypes")
    @Override
    public RecordWriterWithCloseStatus<IEtlKey, CamusWrapper> getDataRecordWriter(TaskAttemptContext context, String fileName,
        CamusWrapper camusWrapper, FileOutputCommitter committer) throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();

      // If recordDelimiter hasn't been initialized, do so now
      if (recordDelimiter == null) {
        recordDelimiter = conf.get(ETL_OUTPUT_RECORD_DELIMITER, DEFAULT_RECORD_DELIMITER);
      }

      CompressionCodec compressionCodec = null;
      CompressionType compressionType = CompressionType.NONE;

      // Determine compression type (BLOCK or RECORD) and compression codec to use.
      if (SequenceFileOutputFormat.getCompressOutput(context)) {
        compressionType = SequenceFileOutputFormat.getOutputCompressionType(context);
        Class<?> codecClass = SequenceFileOutputFormat.getOutputCompressorClass(context, DefaultCodec.class);
        // Instantiate the CompressionCodec Class
        compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
      }

      // Get the filename for this RecordWriter.
      Path path =
          new Path(committer.getWorkPath(), EtlMultiOutputFormat.getUniqueFile(context, fileName, getFilenameExtension()));

      log.info("Creating new SequenceFile.Writer with compression type " + compressionType + " and compression codec "
          + (compressionCodec != null ? compressionCodec.getClass().getName() : "null"));
      final SequenceFile.Writer writer =
          SequenceFile.createWriter(path.getFileSystem(conf), conf, path, LongWritable.class, Text.class,
              compressionType, compressionCodec, context);
        // Return a new anonymous RecordWriter that uses the
        // SequenceFile.Writer to write data to HDFS
        return new RecordWriterWithCloseStatus<IEtlKey, CamusWrapper>() {
            private volatile boolean close;

            @Override
            public void write(IEtlKey key, CamusWrapper data) throws IOException, InterruptedException {
            	
            	String record = (String)data.getRecord();
            	// do not do string concatenate if not needed...here...???
            	if(recordDelimiter != null && !recordDelimiter.isEmpty()){
            		record = record + recordDelimiter;
            	}
            	
            	/**
            	 * What if file is closed ?  Should we create a new one here..?
            	 * should we reopen it or not...
            	 */        	
                // Use the timestamp from the EtlKey as the key for this record.
                // TODO: Is there a better key to use here?
                writer.append(new LongWritable(key.getTime()), new Text(record));
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                writer.close();
                close = true;
            }
            @Override
            protected void finalize() throws Throwable {
            	if(!this.close){
            		log.error("This file was not closed so try to close during the JVM finalize..");
            		try{
            			writer.close();
            		}catch(Throwable th){
            			log.error("File Close erorr during finalize()");
            		}
            	}
            	super.finalize();
            }

			@Override
			public boolean isClose() {
				return close;
			}            
        };
    }
    
}
