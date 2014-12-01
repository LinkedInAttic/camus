package com.linkedin.camus.etl.kafka.reporter;

import java.util.Map;
import java.io.IOException;
import org.apache.log4j.Logger;

import org.apache.hadoop.mapreduce.Job;

public abstract class BaseReporter {
    public static org.apache.log4j.Logger log;

    public BaseReporter() {
        this.log = org.apache.log4j.Logger.getLogger(BaseReporter.class);
    }

    public abstract void report(Job job, Map<String, Long> timingMap) throws IOException;
}
