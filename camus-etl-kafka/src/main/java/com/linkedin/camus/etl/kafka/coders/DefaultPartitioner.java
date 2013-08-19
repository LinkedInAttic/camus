package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

public class DefaultPartitioner implements Partitioner {
    // TODO:  Pacific time is probably a bad default.
    // UTC should be the default.
    public    static final String DEFAULT_TIMEZONE   = "America/Los_Angeles";
    protected static final String OUTPUT_DATE_FORMAT = "YYYY/MM/dd/HH";

    public DateTimeFormatter outputDateFormatter;

    /**
     * Returns a new DefaultPartitioner using the DEFAULT_TIMEZONE.
     */
    public DefaultPartitioner() {
        this(DEFAULT_TIMEZONE);
    }

    /**
     * Returns a new DefaultPartitioner with the given timeZoneString
     */
    public DefaultPartitioner(String timeZoneString) {
        this(DateTimeZone.forID(timeZoneString));
    }

    /**
     * Constructs a new DefaultPartitioner with the given DateTimeZone timeZone.
     */
    public DefaultPartitioner(DateTimeZone timeZone) {
        outputDateFormatter = DateUtils.getDateTimeFormatter(OUTPUT_DATE_FORMAT, timeZone);
    }

    @Override
    public String encodePartition(JobContext context, IEtlKey key) {
        long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(context) * 60000L;
        return ""+DateUtils.getPartition(outfilePartitionMs, key.getTime());
    }

    @Override
    public String generatePartitionedPath(JobContext context, String topic, int brokerId, int partitionId, String encodedPartition) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic).append("/");
        sb.append(EtlMultiOutputFormat.getDestPathTopicSubDir(context)).append("/");
        DateTime bucket = new DateTime(Long.valueOf(encodedPartition));
        sb.append(bucket.toString(outputDateFormatter));
        return sb.toString();
    }
}
