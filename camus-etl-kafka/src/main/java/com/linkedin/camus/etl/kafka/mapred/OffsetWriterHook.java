package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.etl.kafka.common.EtlKey;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map;

public interface OffsetWriterHook {
    public void commit(final TaskAttemptContext context, final Map<String, EtlKey> offsets) throws IOException;
}
