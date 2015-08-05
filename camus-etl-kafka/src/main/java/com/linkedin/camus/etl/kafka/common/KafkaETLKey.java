package com.linkedin.camus.etl.kafka.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;


/**
 * The key for the mapreduce job to pull kafka. Contains offsets and the
 * checksum.
 */
public class KafkaETLKey implements WritableComparable<KafkaETLKey> {
  public static KafkaETLKey DUMMY_KEY = new KafkaETLKey();

  private int inputIndex;
  private long offset;
  private long checksum;
  private String topic;

  /**
   * dummy empty constructor
   */
  public KafkaETLKey() {
    this("dummy", 0, 0, 0);
  }

  public KafkaETLKey(String topic, int index, long offset, long checksum) {
    this.inputIndex = index;
    this.offset = offset;
    this.checksum = checksum;
    this.topic = topic;
  }

  public void set(String topic, int index, long offset, long checksum) {
    this.inputIndex = index;
    this.offset = offset;
    this.checksum = checksum;
    this.topic = topic;
  }

  public String getTopic() {
    return topic;
  }

  public int getIndex() {
    return this.inputIndex;
  }

  public long getOffset() {
    return this.offset;
  }

  public long getChecksum() {
    return this.checksum;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.inputIndex = in.readInt();
    this.offset = in.readLong();
    this.checksum = in.readLong();
    this.topic = in.readUTF();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.inputIndex);
    out.writeLong(this.offset);
    out.writeLong(this.checksum);
    out.writeUTF(this.topic);
  }

  @Override
  public int compareTo(KafkaETLKey o) {
    if (inputIndex != o.inputIndex) {
      return inputIndex = o.inputIndex;
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
    return "index=" + inputIndex + " offset=" + offset + " checksum=" + checksum;
  }

}
