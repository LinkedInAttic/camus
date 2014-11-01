package com.linkedin.camus.shopify;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.kafka.coders.JsonStringMessageDecoder;
import com.linkedin.camus.etl.kafka.mapred.EtlRecordReader;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Properties;


public class TimebasedEventMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    JsonStringMessageDecoder jsonStringMessageDecoder = new JsonStringMessageDecoder();
    LongWritable timestampKey = new LongWritable();
    CamusWrapper camusWrapper;
    Long tMinusOneTimeBucket = new DateTime(DateTimeZone.UTC).getMillis() - 3600000;
    StatsDClient statsd;
    boolean statsdEnabled;

    private boolean isLateArrivingEvent(Long timestamp) {
        return new DateTime(timestamp, DateTimeZone.UTC).isBefore(tMinusOneTimeBucket);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Properties props = new Properties();
        Configuration conf = context.getConfiguration();
        props.setProperty("camus.message.timestamp.field", conf.get("camus.message.timestamp.field"));
        props.setProperty("camus.message.timestamp.format", conf.get("camus.message.timestamp.format"));
        this.statsdEnabled = context.getConfiguration().getBoolean(EtlRecordReader.STATSD_ENABLED, false);
        jsonStringMessageDecoder.init(props, "default topic");
        if (this.statsdEnabled) {
            statsd = new NonBlockingStatsDClient(
                    "Camus",
                    EtlRecordReader.getStatsdHost(context),
                    EtlRecordReader.getStatsdPort(context),
                    new String[]{"camus:late_arriving_events"}
            );
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        camusWrapper = jsonStringMessageDecoder.decode(value.getBytes());
        timestampKey.set(camusWrapper.getTimestamp());
        context.write(timestampKey, value);
        if (this.statsdEnabled && isLateArrivingEvent(camusWrapper.getTimestamp())) {
            this.statsd.increment("camus:late_arriving_events");
        }
    }
}
