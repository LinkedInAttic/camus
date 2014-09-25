package com.linkedin.camus.etl.kafka.coders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.text.SimpleDateFormat;

import com.google.gson.JsonParser;
import com.google.gson.JsonObject;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;

import org.apache.log4j.Logger;

public class HybridMessageDecoder extends MessageDecoder<byte[], String> {
    private static org.apache.log4j.Logger log = Logger.getLogger(HybridMessageDecoder.class);

    public static final String CAMUS_MESSAGE_TIMESTAMP_FORMAT = "camus.message.timestamp.format";
    public static final String DEFAULT_TIMESTAMP_FORMAT       = "iso";

    public static final String CAMUS_MESSAGE_TIMESTAMP_FIELD  = "camus.message.timestamp.field";
    public static final String DEFAULT_TIMESTAMP_FIELD        = "ts";

    private String timestampFormat;
    private String timestampField;

    @Override
    public void init(Properties props, String topicName) {
        this.props = props;
        this.topicName = topicName;

        timestampFormat = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
        timestampField = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FIELD,  DEFAULT_TIMESTAMP_FIELD);
    }

    @Override
    public CamusWrapper<String> decode(byte[] payload) {
        if (payload.length < 1) {
            throw new RuntimeException("Empty payload!");
        }

        String payloadString = new String(payload);

        // magic byte for messages that we simply pass through
        if (payload[0] == 42) {
            return new CamusWrapper<String>(payloadString, System.currentTimeMillis());
        }

        JsonObject jsonObject;

        try {
            jsonObject = new JsonParser().parse(payloadString).getAsJsonObject();
        } catch (RuntimeException e) {
            log.error("Caught exception while parsing JSON string '" + payloadString + "'.");
            throw new RuntimeException(e);
        }

        long timestamp = 0;

        if (jsonObject.has(timestampField)) {
            if (timestampFormat.equals("unix_seconds") || timestampFormat.equals("unix")) {
                timestamp = jsonObject.get(timestampField).getAsLong();
                timestamp = timestamp * 1000L;
            } else if (timestampFormat.equals("unix_milliseconds")) {
                timestamp = jsonObject.get(timestampField).getAsLong();
            } else {
                String timestampString = jsonObject.get(timestampField).getAsString();
                try {
                    timestamp = new SimpleDateFormat(timestampFormat).parse(timestampString).getTime();
                } catch (Exception e) {
                    log.error("Could not parse timestamp '" + timestampString + "' while decoding JSON message.");
                }
            }
        }

        if (timestamp == 0) {
            log.warn("Couldn't find or parse timestamp field '" + timestampField + "' in JSON message, defaulting to current time.");
            timestamp = System.currentTimeMillis();
        }

        return new CamusWrapper<String>(payloadString, timestamp);
    }
}
