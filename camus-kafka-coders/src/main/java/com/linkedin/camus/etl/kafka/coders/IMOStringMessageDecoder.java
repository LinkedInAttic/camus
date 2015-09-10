package com.linkedin.camus.etl.kafka.coders;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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


/**
 * MessageDecoder class that uses IMO format.
 * This MessageDecoder returns a CamusWrapper that works with Strings payloads,
 * since JSON data is always a String.
 */
public class IMOStringMessageDecoder extends MessageDecoder<Message, String> {
  private static final org.apache.log4j.Logger log = Logger.getLogger(IMOStringMessageDecoder.class);

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
  public CamusWrapper<String> decode(Message message) {
    long timestamp = 0;
    String payloadTimestamp;
    String payloadString;
    JsonObject jsonObject;

    try {
      payloadString = new String(message.getPayload(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error("Unable to load UTF-8 encoding, falling back to system default", e);
      payloadString = new String(message.getPayload());
    }

    // Parse the payload into a JsonObject.
    try {
      payloadTimestamp = payloadString.substring(0, payloadString.indexOf('.'));
      String payloadStringJSON = payloadString.substring(payloadString.indexOf(' ') + 1);
      jsonObject = jsonParser.parse(payloadStringJSON).getAsJsonObject();

      timestamp = Long.parseLong(payloadTimestamp) * 1000L;
    } catch (RuntimeException e) {
      log.error("Caught exception while parsing IMO log '" + payloadString + "'.");
      throw new RuntimeException(e);
    }

    return new CamusWrapper<String>(payloadString, timestamp);
  }
}
