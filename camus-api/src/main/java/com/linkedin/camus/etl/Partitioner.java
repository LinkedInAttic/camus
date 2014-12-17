package com.linkedin.camus.etl;



import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * Partitions incoming events, and generates directories and file names in which to
 * store the incoming events.
 */
public abstract class Partitioner extends Configured {
    /**
     * Encode partition values into a string, to be embedded into the working filename.
     * Encoded values cannot use '/' or ':'.
     *
     * Use partition values in the etlKey. Values should be extracted from the Record and
     * given to the CamusWrapper.
     *
     * The return of the method will be passed as the "encodedPartition" parameter of
     * generatePartitionedPath() below.
     *
     * @param context The JobContext.
     * @param etlKey The EtlKey containing values extracted from the Record by the MessageDecoder.
     * @return A string that encodes the partitioning values.
     */
    public abstract String encodePartition(JobContext context, IEtlKey etlKey);

    /**
     * Return a string representing the partitioned directory structure where the .avro files will be moved.
     *
     * For example, if you were using Hive style partitioning, a timestamp based partitioning scheme would return
     *    topic-name/year=2012/month=02/day=04/hour=12
     *
     * The return of this method will be prepended with the value of the property etl.destination.path
     * Most users will want to start this path with the topic name.
     * @param context The JobContext
     * @param topic The topic name
     * @param encodedPartition The encoded partition values. This will be the return of the the encodePartition() method
     *                         above.
     * @return A path string where the avro files will be moved to.
     */
    public abstract String generatePartitionedPath(JobContext context, String topic, String encodedPartition);
    
    /**
     * Return a string representing the target filename where data will be moved to.
     *
     * @param context The JobContext
     * @param topic The topic name
     * @param brokerId the brokerId
     * @param partitionId the partitionId
     * @param count totalEventCount in file
     * @param offset final offset in partition was read too.
     * @param encodedPartition The encoded partition values. This will be the return of the the encodePartition() method
     *                         above.
     * @return A path string where the avro files will be moved to.
     */
    public abstract String generateFileName(JobContext context, String topic, String brokerId, int partitionId, 
        int count, long offset, String encodedPartition);
    
    /**
     * Return a string representing the target filename where data will be moved to.
     *
     * @param context The JobContext
     * @param topic The topic name
     * @param brokerId the brokerId
     * @param partitionId the partitionId
     * @param encodedPartition The encoded partition values. This will be the return of the the encodePartition() method
     *                         above.
     * @return A path string where the avro files will be moved to.
     */
    public abstract String getWorkingFileName(JobContext context, String topic, String brokerId, 
        int partitionId, String encodedPartition);

       
}
