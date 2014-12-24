package com.linkedin.camus.etl.kafka.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public abstract class AbstractOffsetWriterHook implements OffsetWriterHook {
    protected final Path workPath;
    protected final Path outputPath;
    protected final Logger log;

    public AbstractOffsetWriterHook(Path workPath, Path outputPath) {
        this.log = Logger.getLogger(this.getClass());
        this.workPath = workPath;
        this.outputPath = outputPath;
    }
}
