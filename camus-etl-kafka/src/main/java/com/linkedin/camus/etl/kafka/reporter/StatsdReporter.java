package com.linkedin.camus.etl.kafka.reporter;

import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import java.util.Map;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.util.ToolRunner;

import com.linkedin.camus.etl.kafka.reporter.TimeReporter;

public class StatsdReporter extends TimeReporter {

    public static final String STATSD_ENABLED = "statsd.enabled";
    public static final String STATSD_HOST = "statsd.host";
    public static final String STATSD_PORT = "statsd.port";

    private static boolean statsdEnabled;
    private static StatsDClient statsd;

    public void report(Job job, Map<String, Long> timingMap) throws IOException {
        super.report(job, timingMap);
        submitCountersToStatsd(job);
    }

    private void submitCountersToStatsd(Job job) throws IOException {
        Counters counters = job.getCounters();
        if (getStatsdEnabled(job)) {
            StatsDClient statsd = new NonBlockingStatsDClient(
                "Camus", getStatsdHost(job), getStatsdPort(job), new String[]{"camus:counters"}
            );
            for (CounterGroup counterGroup: counters) {
                for (Counter counter: counterGroup){
                    statsd.gauge(counterGroup.getDisplayName() + "." + counter.getDisplayName(), counter.getValue());
                }
            }
        }
    }

    public static Boolean getStatsdEnabled(Job job) {
        return job.getConfiguration().getBoolean(STATSD_ENABLED, false);
    }

    public static String getStatsdHost(Job job) {
        return job.getConfiguration().get(STATSD_HOST, "localhost");
    }

    public static int getStatsdPort(Job job) {
        return job.getConfiguration().getInt(STATSD_PORT, 8125);
    }
}
