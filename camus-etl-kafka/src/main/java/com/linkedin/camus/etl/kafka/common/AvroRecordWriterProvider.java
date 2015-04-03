package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.RecordWriterWithCloseStatus;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

import org.apache.log4j.Logger;


/**
 *
 *
 */
public class AvroRecordWriterProvider implements RecordWriterProvider {
    public final static String EXT = ".avro";
    
    private static Logger log = Logger.getLogger(AvroRecordWriterProvider.class);
    

    public AvroRecordWriterProvider(TaskAttemptContext context) {
    }
    
    @Override
    public String getFilenameExtension() {
        return EXT;
    }

    @SuppressWarnings("rawtypes")
	@Override
    public RecordWriterWithCloseStatus<IEtlKey, CamusWrapper> getDataRecordWriter(
            TaskAttemptContext context,
            String fileName,
            CamusWrapper data,
            FileOutputCommitter committer) throws IOException, InterruptedException {
        final DataFileWriter<Object> writer = new DataFileWriter<Object>(
                new SpecificDatumWriter<Object>());

        if (FileOutputFormat.getCompressOutput(context)) {
            if ("snappy".equals(EtlMultiOutputFormat.getEtlOutputCodec(context))) {
                writer.setCodec(CodecFactory.snappyCodec());
            } else {
                int level = EtlMultiOutputFormat.getEtlDeflateLevel(context);
                writer.setCodec(CodecFactory.deflateCodec(level));
            }
        }

        Path path = committer.getWorkPath();
        path = new Path(path, EtlMultiOutputFormat.getUniqueFile(context, fileName, EXT));
        writer.create(((GenericRecord) data.getRecord()).getSchema(),
                path.getFileSystem(context.getConfiguration()).create(path));

        writer.setSyncInterval(EtlMultiOutputFormat.getEtlAvroWriterSyncInterval(context));

        return new RecordWriterWithCloseStatus<IEtlKey, CamusWrapper>() {
        	
        	private volatile boolean close;
            @Override
            public void write(IEtlKey ignore, CamusWrapper data) throws IOException {
                writer.append(data.getRecord());
            }

            @Override
            public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
                writer.close();
                close = true;
            }
            
            /**
             * THis is our last attemp to close the file...
             */
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
