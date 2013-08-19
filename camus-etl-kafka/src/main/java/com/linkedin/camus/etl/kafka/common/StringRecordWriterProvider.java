package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;


/**
 * Provides a RecordWriter that uses FSDataOutputStream to write
 * a String record as bytes to HDFS without any reformatting or compession.
 */
public class StringRecordWriterProvider implements RecordWriterProvider {
    // TODO: Make this configurable somehow.
    @Override
    public String getFilenameExtension() {
        return "";
    }

    @Override
    public RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(
            TaskAttemptContext  context,
            String              fileName,
            CamusWrapper        camusWrapper,
            FileOutputCommitter committer) throws IOException, InterruptedException {

        // Get the filename for this RecordWriter.
        Path path = new Path(
            committer.getWorkPath(),
            EtlMultiOutputFormat.getUniqueFile(
                context, fileName, getFilenameExtension()
            )
        );

        // Create a FSDataOutputStream stream that will write to path.
        final FSDataOutputStream writer = path.getFileSystem(context.getConfiguration()).create(path);

        // Return a new anonymous RecordWriter that uses the
        // FSDataOutputStream writer to write bytes straight into path.
        return new RecordWriter<IEtlKey, CamusWrapper>() {
            @Override
            public void write(IEtlKey ignore, CamusWrapper data) throws IOException {
                writer.write(((String)data.getRecord()).getBytes());
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                writer.close();
            }
        };
    }
}
