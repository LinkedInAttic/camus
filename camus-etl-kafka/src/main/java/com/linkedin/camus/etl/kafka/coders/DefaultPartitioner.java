package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

public class DefaultPartitioner implements Partitioner {
    public static final DateTimeFormatter OUTPUT_DATE_FORMAT = DateUtils
            .getDateTimeFormatter("YYYY/MM/dd/HH");

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
        sb.append(bucket.toString(OUTPUT_DATE_FORMAT));
        return sb.toString();

    }
}
