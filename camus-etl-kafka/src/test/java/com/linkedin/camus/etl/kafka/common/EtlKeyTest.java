package com.linkedin.camus.etl.kafka.common;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static junit.framework.Assert.assertEquals;


public class EtlKeyTest {

  @Test
  public void testShouldReadOldVersionOfEtlKey() throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    OldEtlKey oldKey = new OldEtlKey();
    EtlKey newKey = new EtlKey();
    oldKey.write(out);

    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());

    newKey.readFields(in);
    assertEquals("leaderId", newKey.getLeaderId());
    assertEquals(1, newKey.getPartition());
    assertEquals(2, newKey.getBeginOffset());
    assertEquals(3, newKey.getOffset());
    assertEquals(4, newKey.getChecksum());
    assertEquals("topic", newKey.getTopic());
    assertEquals(5, newKey.getTime());
    assertEquals("server", newKey.getServer());
    assertEquals("service", newKey.getService());
  }

  public static class OldEtlKey implements WritableComparable<OldEtlKey> {
    private String leaderId = "leaderId";
    private int partition = 1;
    private long beginOffset = 2;
    private long offset = 3;
    private long checksum = 4;
    private String topic = "topic";
    private long time = 5;
    private String server = "server";
    private String service = "service";

    public int compareTo(OldEtlKey o) {
      return 0;
    }

    public void write(DataOutput out) throws IOException {
      UTF8.writeString(out, this.leaderId);
      out.writeInt(this.partition);
      out.writeLong(this.beginOffset);
      out.writeLong(this.offset);
      out.writeLong(this.checksum);
      out.writeUTF(this.topic);
      out.writeLong(this.time);
      out.writeUTF(this.server);
      out.writeUTF(this.service);
    }

    public void readFields(DataInput dataInput) throws IOException {
    }
  }
}
