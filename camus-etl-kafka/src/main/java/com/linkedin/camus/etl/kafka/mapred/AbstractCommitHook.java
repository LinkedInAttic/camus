package com.linkedin.camus.etl.kafka.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

public abstract class AbstractCommitHook implements CommitHook {
    protected final TaskAttemptContext context;
    protected final Configuration configuration;
    protected final Path workPath;
    protected final Path outputPath;
    protected final Logger log;

    public AbstractCommitHook(TaskAttemptContext context, Path workPath, Path outputPath) {
        this.log = Logger.getLogger(this.getClass());
        this.workPath = workPath;
        this.outputPath = outputPath;
        this.context = context;
        configuration = context.getConfiguration();
    }
}
