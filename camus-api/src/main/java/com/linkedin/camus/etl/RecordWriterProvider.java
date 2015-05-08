package com.linkedin.camus.etl;

import com.linkedin.camus.coders.CamusWrapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;

/**
 *
 *
 */
public interface RecordWriterProvider {

    String getFilenameExtension();

    Path getFilePath(String fileName, FileOutputCommitter committer, TaskAttemptContext context) throws IOException;

    RecordWriter<IEtlKey, CamusWrapper> getDataRecordWriter(
            TaskAttemptContext context, String fileName, CamusWrapper data, FileOutputCommitter committer) throws IOException,
            InterruptedException;
}
