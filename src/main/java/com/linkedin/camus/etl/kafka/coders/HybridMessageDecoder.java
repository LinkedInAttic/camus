package com.linkedin.camus.etl.kafka.coders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.text.SimpleDateFormat;
import java.time.Instant;

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

        timestampField = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FIELD,  DEFAULT_TIMESTAMP_FIELD);
        timestampFormat = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FORMAT, DEFAULT_TIMESTAMP_FORMAT);

        if (timestampFormat.equals("iso")) {
            timestampFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        }

        if (timestampFormat.equals("iso_millis")) {
            timestampFormat = "yyyy-MM-dd'T'HH:mm:ss.S'Z'";
        }
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
                timestamp = jsonObject.get(timestampField).getAsLong() * 1000L;
            } else if (timestampFormat.equals("unix_milliseconds")) {
                timestamp = jsonObject.get(timestampField).getAsLong();
            } else {
                String timestampString = jsonObject.get(timestampField).getAsString();
                try {
                    if (timestampFormat.equals("iso_nanos")) {
                        timestamp = Instant.parse(timestampString).toEpochMilli();
                    } else {
                        timestamp = new SimpleDateFormat(timestampFormat).parse(timestampString).getTime();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        if (timestamp == 0) {
            log.warn("Couldn't find timestamp field '" + timestampField + "' in JSON message, defaulting to current time.");
            timestamp = System.currentTimeMillis();
        }

        return new CamusWrapper<String>(payloadString, timestamp);
    }
}
