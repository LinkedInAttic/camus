package com.linkedin.camus.etl.kafka.partitioner;

import org.apache.hadoop.mapreduce.JobContext;

public class TopicGroupingPartitioner extends DefaultPartitioner{

  @Override
  public String getWorkingFileName(JobContext context, String topic, String brokerId, int partitionId,
      String encodedPartition) {
    return super.getWorkingFileName(context, topic, "", 0, encodedPartition);
  }
  
  @Override
  public String generateFileName(JobContext context, String topic, String brokerId, int partitionId, int count,
      long offset, String encodedPartition) {
    return super.generateFileName(context, topic, "", 0, count, 0, encodedPartition);
  }

}
