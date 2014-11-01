package com.linkedin.camus.etl.kafka.coders;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;


/**
 * MessageDecoder class that will convert the payload into a JSON object,
 * look for a the camus.message.timestamp.field, convert that timestamp to
 * a unix epoch long using camus.message.timestamp.format, and then set the CamusWrapper's
 * timestamp property to the record's timestamp.  If the JSON does not have
 * a timestamp or if the timestamp could not be parsed properly, then
 * System.currentTimeMillis() will be used.
 * <p/>
 * camus.message.timestamp.format will be used with SimpleDateFormat.  If your
 * camus.message.timestamp.field is stored in JSON as a unix epoch timestamp,
 * you should set camus.message.timestamp.format to 'unix_seconds' (if your
 * timestamp units are seconds) or 'unix_milliseconds' (if your timestamp units
 * are milliseconds).
 * <p/>
 * This MessageDecoder returns a CamusWrapper that works with Strings payloads,
 * since JSON data is always a String.
 */
public class JsonStringMessageDecoder extends MessageDecoder<byte[], String> {
    private static org.apache.log4j.Logger log = Logger.getLogger(JsonStringMessageDecoder.class);

    // Property for format of timestamp in JSON timestamp field.
    public static final String CAMUS_MESSAGE_TIMESTAMP_FORMAT = "camus.message.timestamp.format";
    public static final String DEFAULT_TIMESTAMP_FORMAT = "[dd/MMM/yyyy:HH:mm:ss Z]";

    // Property for the JSON field name of the timestamp.
    public static final String CAMUS_MESSAGE_TIMESTAMP_FIELD = "camus.message.timestamp.field";
    public static final String DEFAULT_TIMESTAMP_FIELD = "timestamp";

    JsonParser jsonParser = new JsonParser();
    DateTimeFormatter dateTimeParser = ISODateTimeFormat.dateTimeParser();

    private String timestampFormat;
    private String timestampField;

    @Override
    public void init(Properties props, String topicName) {
        this.props = props;
        this.topicName = topicName;

        timestampFormat = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
        timestampField = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FIELD, DEFAULT_TIMESTAMP_FIELD);
    }

    @Override
    public CamusWrapper<String> decode(byte[] payload) {
        long timestamp = 0;
        String payloadString;
        JsonObject jsonObject;
        payloadString = new String(payload);

        // Parse the payload into a JsonObject.
        try {
            jsonObject = jsonParser.parse(payloadString.trim()).getAsJsonObject();
        } catch (RuntimeException e) {
            log.error("Caught exception while parsing JSON string '" + payloadString + "'.");
            throw new RuntimeException(e);
        }

        // Attempt to read and parse the timestamp element into a long.
        // If timestampFormat is 'unix_seconds',
        // then the timestamp only needs converted to milliseconds.
        // Also support 'unix' for backwards compatibility.
        if (jsonObject.has(timestampField))
            if (timestampFormat.equals("unix_seconds") || timestampFormat.equals("unix")) {
                timestamp = jsonObject.get(timestampField).getAsLong();
                // This timestamp is in seconds, convert it to milliseconds.
                timestamp = timestamp * 1000L;
            }

        // Else if this timestamp is already in milliseconds,
        // just save it as is.
            else if (timestampFormat.equals("unix_milliseconds")) {
                timestamp = jsonObject.get(timestampField).getAsLong();
            }

        // Otherwise parse the timestamp as a string in timestampFormat.
            else {
                String timestampString = jsonObject.get(timestampField).getAsString();
                try {
                    timestamp = dateTimeParser.parseDateTime(timestampString).getMillis();
                } catch (IllegalArgumentException e) {
                    try {
                        timestamp = new SimpleDateFormat(timestampFormat).parse(timestampString).getTime();
                    } catch (ParseException ee) {
                        log.error("Could not parse timestamp '" + timestampString + "' while decoding JSON message.");
                    }
                } catch (Exception ee) {
                    log.error("Could not parse timestamp '" + timestampString + "' while decoding JSON message.");
                }
            }

        // If timestamp wasn't set in the above block,
        // then set it to current time.
        if (timestamp == 0) {
            log.warn("Couldn't find or parse timestamp field '" + timestampField + "' in JSON message, defaulting to current time.");
            timestamp = System.currentTimeMillis();
        }

        return new CamusWrapper<String>(payloadString, timestamp);
    }
}
