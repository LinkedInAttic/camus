package com.linkedin.camus.etl;

import java.io.IOException;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import com.linkedin.camus.coders.CamusWrapper;

/**
 *
 *
 */
public interface RecordWriterProvider {

    String getFilenameExtension();

    @SuppressWarnings("rawtypes")
	RecordWriterWithCloseStatus<IEtlKey, CamusWrapper> getDataRecordWriter(
            TaskAttemptContext context, String fileName, CamusWrapper data, FileOutputCommitter committer) throws IOException,
            InterruptedException;
    }
