package com.linkedin.camus.etl.kafka.coders;

import java.util.Properties;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;

public class ByteArrayMessageDecoder extends MessageDecoder<byte[], byte[]> {
    @Override
    public void init(Properties props, String topicName) {
        this.props = props;
        this.topicName = topicName;
    }

    @Override
    public CamusWrapper<byte[]> decode(byte[] payload) {
        return new CamusWrapper<byte[]>(payload, System.currentTimeMillis());
    }
}
