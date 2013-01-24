package com.linkedin.camus.etl.kafka.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableComparable;

/**
 * The key for the mapreduce job to pull kafka. Contains offsets and the
 * checksum.
 */
public class EtlKey implements WritableComparable<EtlKey> {
	public static EtlKey DUMMY_KEY = new EtlKey();

	private String nodeId = "";
	private int partition = 0;
	private long beginOffset = 0;
	private long offset = 0;
	private long checksum = 0;
	private String topic = "";
	private long time = 0;
	private String server = "";
	private String service = "";

	/**
	 * dummy empty constructor
	 */
	public EtlKey() {
		this("dummy", "0", 0, 0, 0, 0);
	}

	public EtlKey(EtlKey other) {
		this.nodeId = other.nodeId;
		this.partition = other.partition;
		this.beginOffset = other.beginOffset;
		this.offset = other.offset;
		this.checksum = other.checksum;
		this.topic = other.topic;
		this.time = other.time;
		this.server = other.server;
		this.service = other.service;
	}

	public EtlKey(String topic, String nodeId, int partition) {
		this.set(topic, nodeId, partition, 0, 0, 0);
	}

	public EtlKey(String topic, String nodeId, int partition, long beginOffset, long offset) {
		this.set(topic, nodeId, partition, beginOffset, offset, 0);
	}

	public EtlKey(String topic, String nodeId, int partition, long beginOffset, long offset, long checksum) {
		this.set(topic, nodeId, partition, beginOffset, offset, checksum);
	}

	public void set(String topic, String nodeId, int partition, long beginOffset, long offset, long checksum) {
		this.nodeId = nodeId;
		this.partition = partition;
		this.beginOffset = beginOffset;
		this.offset = offset;
		this.checksum = checksum;
		this.topic = topic;
		this.time = System.currentTimeMillis(); // if event can't be decoded,
												// this time will be used for
												// debugging.
	}
	
	public void clear() {
		nodeId = "";
		partition = 0;
		beginOffset = 0;
		offset = 0;
		checksum = 0;
		topic = "";
		time = 0;
		server = "";
		service = "";
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public String getTopic() {
		return topic;
	}

	public String getNodeId() {
		return nodeId;
	}

	public int getPartition() {
		return this.partition;
	}

	public long getBeginOffset() {
		return this.beginOffset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}
	
	public long getOffset() {
		return this.offset;
	}

	public long getChecksum() {
		return this.checksum;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.nodeId = UTF8.readString(in);
		this.partition = in.readInt();
		this.beginOffset = in.readLong();
		this.offset = in.readLong();
		this.checksum = in.readLong();
		this.topic = in.readUTF();
		this.time = in.readLong();
		this.server = in.readUTF();
		this.service = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		UTF8.writeString(out, this.nodeId);
		out.writeInt(this.partition);
		out.writeLong(this.beginOffset);
		out.writeLong(this.offset);
		out.writeLong(this.checksum);
		out.writeUTF(this.topic);
		out.writeLong(this.time);
		out.writeUTF(this.server);
		out.writeUTF(this.service);
	}

	@Override
	public int compareTo(EtlKey o) {
		if (partition != o.partition) {
			return partition = o.partition;
		} else {
			if (beginOffset > o.beginOffset) {
				return 1;
			} else if (beginOffset < o.beginOffset) {
				return -1;
			} else {
				if (checksum > o.checksum) {
					return 1;
				} else if (checksum < o.checksum) {
					return -1;
				} else {
					return 0;
				}
			}
		}
	}

	@Override
	public String toString() {
		return " topic=" + topic + " partition=" + partition + " nodeId=" + nodeId + " server=" + server + " service=" + service + " beginOffset="
				+ beginOffset + " offset=" + offset + " checksum=" + checksum + " time=" + time;

	}
}
