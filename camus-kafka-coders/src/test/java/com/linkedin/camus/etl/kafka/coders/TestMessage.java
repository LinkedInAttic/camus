package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.Message;

import java.io.IOException;

/**
 * Created by michaelandrepearce on 05/04/15.
 */
public class TestMessage implements Message {

    byte[] payload;
    byte[] key;

    private String topic = "";
    private long offset = 0;
    private int partition = 0;
    private long checksum = 0;


    public byte[] getPayload() {
        return this.payload;
    }

    public TestMessage setPayload(byte[] payload) {
        this.payload = payload;
        return this;
    }

    public byte[] getKey() {
        return this.key;
    }

    public TestMessage setKey(byte[] key) {
        this.key = key;
        return this;
    }

    public String getTopic() {
        return this.topic;
    }

    public TestMessage setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public long getOffset() {
        return this.offset;
    }

    public TestMessage setOffset(long offset) {
        this.offset = offset;
        return this;
    }

    public int getPartition() {
        return this.partition;
    }

    public TestMessage setPartition(int partition) {
        this.partition = partition;
        return this;
    }

    public long getChecksum() {
        return this.checksum;
    }

    @Override
    public void validate() throws IOException {

    }

    public TestMessage setChecksum(long checksum) {
        this.checksum = checksum;
        return this;
    }
}
