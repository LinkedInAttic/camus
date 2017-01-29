package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;


public class CsvStringMessageDecoder extends MessageDecoder<Message, String> {
    private static final Logger log = Logger.getLogger(CsvStringMessageDecoder.class);

    // Property for format of timestamp in JSON timestamp field.
    public static final String CAMUS_MESSAGE_TIMESTAMP_FORMAT = "camus.message.timestamp.format";
    public static final String DEFAULT_TIMESTAMP_FORMAT = "[dd/MMM/yyyy:HH:mm:ss Z]";

    // Property for the JSON field name of the timestamp.
    public static final String CAMUS_MESSAGE_TIMESTAMP_FIELD = "camus.message.timestamp.field";
    public static final String DEFAULT_TIMESTAMP_FIELD = "0";

    DateTimeFormatter dateTimeParser = ISODateTimeFormat.dateTimeParser();

    private String timestampFormat;
    private int timestampField;

    @Override
    public void init(Properties props, String topicName) {
        this.props = props;
        this.topicName = topicName;

        timestampFormat = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
        timestampField = Integer.valueOf(props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FIELD, DEFAULT_TIMESTAMP_FIELD));
    }

    @Override
    public CamusWrapper<String> decode(Message message) {
        long timestamp = 0;
        String payloadString;
        try {
            payloadString = new String(message.getPayload(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error("Unable to load UTF-8 encoding, falling back to system default", e);
            payloadString = new String(message.getPayload());
        }

        String[] csvFields = payloadString.split(",");
        if (csvFields.length > timestampField) {
            if (timestampFormat.equals("unix_seconds") || timestampFormat.equals("unix")) {
                timestamp = Long.valueOf(csvFields[timestampField]);
                timestamp = timestamp * 1000L;
            } else if (timestampFormat.equals("unix_milliseconds")) {
                timestamp = Long.valueOf(csvFields[timestampField]);
            }
            else if (timestampFormat.equals("ISO-8601")) {
                String timestampString = csvFields[timestampField];
                try {
                    timestamp = new DateTime(timestampString).getMillis();
                } catch (IllegalArgumentException e) {
                    log.error("Could not parse timestamp '" + timestampString + "' as ISO-8601 while decoding JSON message.");
                }
            }
            // Otherwise parse the timestamp as a string in timestampFormat.
            else {
                String timestampString = csvFields[timestampField];
                try {
                    timestamp = dateTimeParser.parseDateTime(timestampString).getMillis();
                } catch (IllegalArgumentException e) {
                    try {
                        timestamp = new SimpleDateFormat(timestampFormat).parse(timestampString).getTime();
                    } catch (ParseException pe) {
                        log.error("Could not parse timestamp '" + timestampString + "' while decoding JSON message.");
                    }
                } catch (Exception ee) {
                    log.error("Could not parse timestamp '" + timestampString + "' while decoding JSON message.");
                }
            }
        }

        if (timestamp == 0) {
            log.warn("Couldn't find or parse timestamp field '" + timestampField
                    + "' in JSON message, defaulting to current time.");
            timestamp = System.currentTimeMillis();
        }

        return new CamusWrapper<String>(payloadString, timestamp);
    }
}
