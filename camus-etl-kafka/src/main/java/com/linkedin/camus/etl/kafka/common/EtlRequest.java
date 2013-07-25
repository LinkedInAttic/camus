package com.linkedin.camus.etl.kafka.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

import com.linkedin.camus.etl.kafka.CamusJob;

/**
 * A class that represents the kafka pull request.
 * 
 * The class is a container for topic, leaderId, partition, uri and offset. It is
 * used in reading and writing the sequence files used for the extraction job.
 * 
 * @author Richard Park
 */
public class EtlRequest implements Writable {
	
	private static Logger log = Logger.getLogger(EtlRequest.class);
    private JobContext context = null;
    private static final long DEFAULT_OFFSET = 0;

    private String topic = "";
    private String leaderId = "";
    private int partition = 0;

    private URI uri = null;
    private long offset = DEFAULT_OFFSET;
    private long latestOffset = -1;
    private long earliestOffset = -2;
    
    public EtlRequest() {
    }

    public EtlRequest(EtlRequest other) {
        this.topic = other.topic;
        this.leaderId = other.leaderId;
        this.partition = other.partition;
        this.uri = other.uri;
        this.offset = other.offset;
        this.latestOffset = other.latestOffset;
        this.earliestOffset = other.earliestOffset;
    }

    public void setLatestOffset(long latestOffset) {
		this.latestOffset = latestOffset;
	}
    
    public void setEarliestOffset(long earliestOffset) {
		this.earliestOffset = earliestOffset;
	}
    
    /**
     * Constructor for a KafkaETLRequest with the uri set to null and offset set
     * to -1. Both of these attributes can be set later. These attributes are
     * sufficient to ensure uniqueness.
     * 
     * @param topic
     *            The topic name
     * @param leaderId
     *            The leader broker for this partition and topic
     * @param partition
     *            The partition to pull
     */
    public EtlRequest(JobContext context, String topic, String leaderId, int partition) {
        this(context, topic, leaderId, partition, null, DEFAULT_OFFSET);
    }

    /**
     * Constructor for the KafkaETLRequest with the offset to -1.
     * 
     * @param topic
     *            The topic name
     * @param leaderId
     *            The leader broker for this topic and partition
     * @param partition
     *            The partition to pull
     * @param brokerUri
     *            The uri for the broker.
     */
    public EtlRequest(JobContext context, String topic, String leaderId, int partition, URI brokerUri) {
        this(context, topic, leaderId, partition, brokerUri, DEFAULT_OFFSET);
    }

    /**
     * Constructor for the full kafka pull job. Neither the brokerUri nor offset
     * are used to ensure uniqueness.
     * 
     * @param topic
     *            The topic name
     * @param leaderId
     *            The leader broker for this topic and partition
     * @param partition
     *            The partition to pull
     * @param brokerUri
     *            The uri for the broker
     * @param offset
     */
    public EtlRequest(JobContext context, String topic, String leaderId, int partition,
            URI brokerUri, long offset) {
        this.context = context;
        this.topic = topic;
        this.leaderId = leaderId;
        this.uri = brokerUri;
        this.partition = partition;
        setOffset(offset);
    }

    /**
     * Sets the starting offset used by the kafka pull mapper.
     * 
     * @param offset
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }

    /**
     * Sets the broker uri for this request
     * 
     * @param uri
     */
    public void setURI(URI uri) {
        this.uri = uri;
    }

    /**
     * Retrieve the broker node id.
     * 
     * @return
     */
    public String getLeaderId() {
        return this.leaderId;
    }

    /**
     * Retrieve the topic
     * 
     * @return
     */
    public String getTopic() {
        return this.topic;
    }

    /**
     * Retrieves the uri if set. The default is null.
     * 
     * @return
     */
    public URI getURI() {
        return this.uri;
    }

    /**
     * Retrieves the partition number
     * 
     * @return
     */
    public int getPartition() {
        return this.partition;
    }

    /**
     * Retrieves the offset
     * 
     * @return
     */
    public long getOffset() {
        return this.offset;
    }

    
    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }
    
    /**
     * Returns true if the offset is valid (>= to earliest offset && <= to last
     * offset)
     * 
     * @return
     */
    public boolean isValidOffset() {
        return this.offset >= 0;
    }

    @Override
    public String toString() {
        return topic + "\turi:" + (uri != null ? uri.toString() : "") + "\tleader:" + leaderId
                + "\tpartition:" + partition + "\toffset:" + offset + "\tlatest_offset:"
                + getLastOffset();
    }

    @Override
    public int hashCode() {
        return (partition + topic).hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return this.hashCode() == other.hashCode();
    }

    /**
     * Returns the copy of KafkaETLRequest
     */
    @Override
    public EtlRequest clone() {
        return new EtlRequest(context, topic, leaderId, partition, uri, offset);
    }

    public long getEarliestOffset() {
        if (this.earliestOffset == -2 && uri != null) {
            // TODO : Make the hardcoded paramters configurable
            SimpleConsumer consumer = new SimpleConsumer(uri.getHost(), uri.getPort(), 60000,
                    1024 * 1024, "hadoop-etl");
            Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            offsetInfo.put(new TopicAndPartition(topic, partition), new PartitionOffsetRequestInfo(
                    kafka.api.OffsetRequest.EarliestTime(), 1));
            OffsetResponse response = consumer
                    .getOffsetsBefore(new OffsetRequest(offsetInfo, kafka.api.OffsetRequest
                            .CurrentVersion(), "hadoop-etl"));
            long[] endOffset = response.offsets(topic, partition);
            consumer.close();
            this.earliestOffset = endOffset[0];
            return endOffset[0];
        } else {
            return this.earliestOffset;
        }
    }

    public long getLastOffset() {
        if (this.latestOffset == -1 && uri != null)
            return getLastOffset(kafka.api.OffsetRequest.LatestTime());
        else
        {           
        	return this.latestOffset;
        }
    }

    public long getLastOffset(long time) {
        SimpleConsumer consumer = new SimpleConsumer(uri.getHost(), uri.getPort(), 60000,
                1024 * 1024, "hadoop-etl");
        Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        offsetInfo.put(new TopicAndPartition(topic, partition), new PartitionOffsetRequestInfo(
                time, 1));
        OffsetResponse response = consumer.getOffsetsBefore(new OffsetRequest(offsetInfo,
                kafka.api.OffsetRequest.CurrentVersion(),"hadoop-etl"));
        long[] endOffset = response.offsets(topic, partition);
        consumer.close();
        if(endOffset.length == 0)
        {
        	log.info("The exception is thrown because the latest offset retunred zero for topic : " + topic + " and partition " + partition);
        }
        this.latestOffset = endOffset[0];
        return endOffset[0];
    }

    public long estimateDataSize() {
        long endOffset = getLastOffset();
        return endOffset - offset;
    }

    /**
     * Estimates the request size in bytes by connecting to the broker and
     * querying for the offset that bets matches the endTime.
     * 
     * @param endTime
     *            The time in millisec
     */
    public long estimateDataSize(long endTime) {
        long endOffset = getLastOffset(endTime);
        return endOffset - offset;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        topic = UTF8.readString(in);
        leaderId = UTF8.readString(in);
        String str = UTF8.readString(in);
        if (!str.isEmpty())
            try {
                uri = new URI(str);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        partition = in.readInt();
        offset = in.readLong();
        latestOffset = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        UTF8.writeString(out, topic);
        UTF8.writeString(out, leaderId);
        if (uri != null)
            UTF8.writeString(out, uri.toString());
        else
            UTF8.writeString(out, "");
        out.writeInt(partition);
        out.writeLong(offset);
        out.writeLong(latestOffset);
    }
}