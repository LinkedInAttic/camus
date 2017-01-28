package com.linkedin.camus.etl.kafka.coders;

/**
 * Created by ehrlichja on 7/28/14.
 */

import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

public class UnifiedPartitioner extends Partitioner {

    protected static final String OUTPUT_DATE_FORMAT = "YYYY/MM/dd/HH";
    //protected DateTimeZone outputDateTimeZone = null;
    protected DateTimeFormatter outputDateFormatter = null;

    @Override
    public String encodePartition(JobContext context, IEtlKey key) {
        long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(context) * 60000L;
        return ""+DateUtils.getPartition(outfilePartitionMs, key.getTime(), outputDateFormatter.getZone());
    }

    @Override
    public String generatePartitionedPath(JobContext context, String topic, String brokerId, int PartitionId, String encodedPartition) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic).append("/");
        DateTime bucket = new DateTime(Long.valueOf(encodedPartition));
        int year = bucket.year().get();
        int month = bucket.monthOfYear().get();
        int day = bucket.dayOfMonth().get();
        int hour = bucket.hourOfDay().get();
        sb.append("year=").append(year).append("/");
        sb.append("month=").append(month).append("/");
        sb.append("day=").append(day).append("/");
        sb.append("hour=").append(hour).append("/");
        return sb.toString();
    }

    @Override
    public void setConf(Configuration conf) {

        if (conf != null){
            outputDateFormatter = DateUtils.getDateTimeFormatter(OUTPUT_DATE_FORMAT,DateTimeZone.forID(conf.get(EtlMultiOutputFormat.ETL_DEFAULT_TIMEZONE, "America/Los_Angeles")));
        }

        super.setConf(conf);
    }

}
