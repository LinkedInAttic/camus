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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import com.linkedin.camus.coders.CamusWrapper;

import com.linkedin.camus.etl.kafka.reporter.TimeReporter;


public class StatsdReporter extends TimeReporter {

    public static final String STATSD_ENABLED = "statsd.enabled";
    public static final String STATSD_HOST = "statsd.host";
    public static final String STATSD_PORT = "statsd.port";
    public static final String PREFIX = "Camus";

    private static boolean statsdEnabled;
    private static StatsDClient statsd;

    public void report(Job job, Map<String, Long> timingMap) throws IOException {
        super.report(job, timingMap);
        submitCountersToStatsd(job);
    }

    private void submitCountersToStatsd(Job job) throws IOException {
        Counters counters = job.getCounters();
        if (getStatsdEnabled(job.getConfiguration())) {
            StatsDClient statsd = new NonBlockingStatsDClient(
                PREFIX, getStatsdHost(job.getConfiguration()), getStatsdPort(job.getConfiguration()), new String[]{"camus:counters"}
            );
            for (CounterGroup counterGroup: counters) {
                for (Counter counter: counterGroup){
                    statsd.gauge(counterGroup.getDisplayName() + "." + counter.getDisplayName(), counter.getValue());
                }
            }
        }
    }

    public static void logEventDelay(JobContext context, String topic, CamusWrapper wrap) {
        if (getStatsdEnabled(context.getConfiguration())) {
            long currentTime = System.currentTimeMillis();
            long delay = currentTime - wrap.getTimestamp();
            if ( delay >= 3600000 ) {
                statsd = new NonBlockingStatsDClient(
                    PREFIX, getStatsdHost(context.getConfiguration()), getStatsdPort(context.getConfiguration()), new String[]{"topic:" + topic}
                );
                statsd.gauge("load_delay", delay);
            }
        }
    }

    public static Boolean getStatsdEnabled(Configuration config) {
        return config.getBoolean(STATSD_ENABLED, false);
    }

    public static String getStatsdHost(Configuration config) {
        return config.get(STATSD_HOST, "localhost");
    }

    public static int getStatsdPort(Configuration config) {
        return config.getInt(STATSD_PORT, 8125);
    }
}
