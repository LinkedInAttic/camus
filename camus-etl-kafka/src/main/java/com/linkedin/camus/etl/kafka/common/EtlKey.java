package com.linkedin.camus.etl.kafka.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.linkedin.camus.etl.IEtlKey;


/**
 * The key for the mapreduce job to pull kafka. Contains offsets and the
 * checksum.
 */
public class EtlKey implements WritableComparable<EtlKey>, IEtlKey {
  public static final Text SERVER = new Text("server");
  public static final Text SERVICE = new Text("service");
  public static EtlKey DUMMY_KEY = new EtlKey();

  private String leaderId = "";
  private int partition = 0;
  private long beginOffset = 0;
  private long offset = 0;
  private long checksum = 0;
  private String topic = "";
  private long time = 0;
  private String server = "";
  private String service = "";
  private MapWritable partitionMap = new MapWritable();

  /**
   * dummy empty constructor
   */
  public EtlKey() {
    this("dummy", "0", 0, 0, 0, 0);
  }

  public EtlKey(EtlKey other) {

    this.partition = other.partition;
    this.beginOffset = other.beginOffset;
    this.offset = other.offset;
    this.checksum = other.checksum;
    this.topic = other.topic;
    this.time = other.time;
    this.server = other.server;
    this.service = other.service;
    this.partitionMap = new MapWritable(other.partitionMap);
  }

  public EtlKey(String topic, String leaderId, int partition) {
    this.set(topic, leaderId, partition, 0, 0, 0);
  }

  public EtlKey(String topic, String leaderId, int partition, long beginOffset, long offset) {
    this.set(topic, leaderId, partition, beginOffset, offset, 0);
  }

  public EtlKey(String topic, String leaderId, int partition, long beginOffset, long offset, long checksum) {
    this.set(topic, leaderId, partition, beginOffset, offset, checksum);
  }

  public void set(String topic, String leaderId, int partition, long beginOffset, long offset, long checksum) {
    this.leaderId = leaderId;
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
    leaderId = "";
    partition = 0;
    beginOffset = 0;
    offset = 0;
    checksum = 0;
    topic = "";
    time = 0;
    server = "";
    service = "";
    partitionMap = new MapWritable();
  }

  public String getServer() {
    return partitionMap.get(SERVER).toString();
  }

  public void setServer(String newServer) {
    partitionMap.put(SERVER, new Text(newServer));
  }

  public String getService() {
    return partitionMap.get(SERVICE).toString();
  }

  public void setService(String newService) {
    partitionMap.put(SERVICE, new Text(newService));
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

  public String getLeaderId() {
    return leaderId;
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
  public long getMessageSize() {
    Text key = new Text("message.size");
    if (this.partitionMap.containsKey(key))
      return ((LongWritable) this.partitionMap.get(key)).get();
    else
      return 1024; //default estimated size
  }

  public void setMessageSize(long messageSize) {
    Text key = new Text("message.size");
    put(key, new LongWritable(messageSize));
  }

  public void put(Writable key, Writable value) {
    this.partitionMap.put(key, value);
  }

  public void addAllPartitionMap(MapWritable partitionMap) {
    this.partitionMap.putAll(partitionMap);
  }

  public MapWritable getPartitionMap() {
    return partitionMap;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.leaderId = UTF8.readString(in);
    this.partition = in.readInt();
    this.beginOffset = in.readLong();
    this.offset = in.readLong();
    this.checksum = in.readLong();
    this.topic = in.readUTF();
    this.time = in.readLong();
    this.server = in.readUTF(); // left for legacy
    this.service = in.readUTF(); // left for legacy
    this.partitionMap = new MapWritable();
    try {
      this.partitionMap.readFields(in);
    } catch (IOException e) {
      this.setServer(this.server);
      this.setService(this.service);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, this.leaderId);
    out.writeInt(this.partition);
    out.writeLong(this.beginOffset);
    out.writeLong(this.offset);
    out.writeLong(this.checksum);
    out.writeUTF(this.topic);
    out.writeLong(this.time);
    out.writeUTF(this.server); // left for legacy
    out.writeUTF(this.service); // left for legacy
    this.partitionMap.write(out);
  }

  @Override
  public int compareTo(EtlKey o) {
    if (partition != o.partition) {
      return partition = o.partition;
    } else {
      if (offset > o.offset) {
        return 1;
      } else if (offset < o.offset) {
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
    StringBuilder builder = new StringBuilder();
    builder.append("topic=");
    builder.append(topic);
    builder.append(" partition=");
    builder.append(partition);
    builder.append("leaderId=");
    builder.append(leaderId);
    builder.append(" server=");
    builder.append(server);
    builder.append(" service=");
    builder.append(service);
    builder.append(" beginOffset=");
    builder.append(beginOffset);
    builder.append(" offset=");
    builder.append(offset);
    builder.append(" msgSize=");
    builder.append(getMessageSize());
    builder.append(" server=");
    builder.append(server);
    builder.append(" checksum=");
    builder.append(checksum);
    builder.append(" time=");
    builder.append(time);

    for (Map.Entry<Writable, Writable> e : partitionMap.entrySet()) {
      builder.append(" " + e.getKey() + "=");
      builder.append(e.getValue().toString());
    }

    return builder.toString();
  }
}
