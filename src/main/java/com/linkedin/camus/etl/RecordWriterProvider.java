package com.linkedin.camus.etl;

import com.linkedin.camus.coders.CamusWrapper;
import java.io.IOException;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

/**
 *
 *
 */
public interface RecordWriterProvider {

    String getFilenameExtension();

    RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(
            TaskAttemptContext context, String fileName, CamusWrapper data, FileOutputCommitter committer) throws IOException,
            InterruptedException;
}
