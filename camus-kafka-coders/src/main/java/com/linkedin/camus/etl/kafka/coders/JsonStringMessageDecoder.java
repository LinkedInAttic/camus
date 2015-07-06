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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


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
public class JsonStringMessageDecoder extends MessageDecoder<Message, String> {
  private static final org.apache.log4j.Logger log = Logger.getLogger(JsonStringMessageDecoder.class);

  // Property for format of timestamp in JSON timestamp field.
  public static final String CAMUS_MESSAGE_TIMESTAMP_FORMAT = "camus.message.timestamp.format";
  public static final String DEFAULT_TIMESTAMP_FORMAT = "[dd/MMM/yyyy:HH:mm:ss Z]";

  // Property for the JSON field name of the timestamp.
  public static final String CAMUS_MESSAGE_TIMESTAMP_FIELD = "camus.message.timestamp.field";
  public static final String DEFAULT_TIMESTAMP_FIELD = "timestamp";

  public static final Pattern MULTI_TIMESTAMP_PATTERN = Pattern.compile("camus.message.timestamp.([0-9]+).(format|field)");

  JsonParser jsonParser = new JsonParser();
  DateTimeFormatter dateTimeParser = ISODateTimeFormat.dateTimeParser();

  private List<TimestampInfo> possibleTimestamps = new ArrayList<JsonStringMessageDecoder.TimestampInfo>();

  @Override
  public void init(Properties props, String topicName) {
    this.props = props;
    this.topicName = topicName;

    initPossibleTimestamps(props);
  }

  private void initPossibleTimestamps(Properties props) {
    HashMap<Integer, TimestampInfo> configurationMap = new HashMap<Integer, TimestampInfo>();

    for (String key: props.stringPropertyNames()) {
      Matcher m = MULTI_TIMESTAMP_PATTERN.matcher(key);
      if (m.matches()) {
        Integer index = Integer.valueOf(m.group(1));
        if (!configurationMap.containsKey(index)) {
          configurationMap.put(index, new TimestampInfo());
        }
        TimestampInfo timestampInfo = configurationMap.get(index);

        if ("format".equals(m.group(2))) {
          timestampInfo.format = props.getProperty(key);
        } else if ("field".equals(m.group(2))) {
          timestampInfo.field = props.getProperty(key);
        }
      }
    }
    possibleTimestamps.addAll(configurationMap.values());

    TimestampInfo timestampInfo = new TimestampInfo();
    timestampInfo.format = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
    timestampInfo.field = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FIELD, DEFAULT_TIMESTAMP_FIELD);
    possibleTimestamps.add(timestampInfo);
  }

  @Override
  public CamusWrapper<String> decode(Message message) {
    long timestamp = 0;
    String payloadString;
    JsonObject jsonObject;
    JsonObject jsonObjectOrig;

    try {
      payloadString = new String(message.getPayload(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      log.error("Unable to load UTF-8 encoding, falling back to system default", e);
      payloadString = new String(message.getPayload());
    }

    // Parse the payload into a JsonObject.
    try {
      jsonObjectOrig = jsonParser.parse(payloadString.trim()).getAsJsonObject();
    } catch (RuntimeException e) {
      log.error("Caught exception while parsing JSON string '" + payloadString + "'.");
      throw new RuntimeException(e);
    }

    for (TimestampInfo timestampInfo: possibleTimestamps) {
      if (timestamp != 0) { // timestamp already decoded
        break;
      }

      String timestampField = timestampInfo.field;
      String timestampFormat = timestampInfo.format;
      jsonObject = jsonObjectOrig;

      while (timestampField.indexOf('.') != -1) { // go through nested objects
        String field = timestampField.substring(0, timestampField.indexOf('.'));
        timestampField = timestampField.substring(timestampField.indexOf('.') + 1);
        if (!jsonObject.has(field)) {
          break;
        }
        jsonObject = jsonObject.getAsJsonObject(field);
      }

      // Attempt to read and parse the timestamp element into a long.
      if (jsonObject.has(timestampField)) {
        // If timestampFormat is 'unix_seconds',
        // then the timestamp only needs converted to milliseconds.
        // Also support 'unix' for backwards compatibility.
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
        // Else if timestampFormat is 'ISO-8601', parse that
        else if (timestampFormat.equals("ISO-8601")) {
          String timestampString = jsonObject.get(timestampField).getAsString();
          try {
            timestamp = new DateTime(timestampString).getMillis();
          } catch (IllegalArgumentException e) {
            log.error("Could not parse timestamp '" + timestampString + "' as ISO-8601 while decoding JSON message.");
          }
        }
        // Otherwise parse the timestamp as a string in timestampFormat.
        else {
          String timestampString = jsonObject.get(timestampField).getAsString();
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
    }

    // If timestamp wasn't set in the above block,
    // then set it to current time.
    if (timestamp == 0) {
      log.warn("Couldn't find or parse timestamp fields '" + possibleTimestamps
          + "' in JSON message, defaulting to current time.");
      timestamp = System.currentTimeMillis();
    }

    return new CamusWrapper<String>(payloadString, timestamp);
  }

  static class TimestampInfo {
    public String format;
    public String field;

    @Override
    public String toString() {
      return field + " (" + format + ")";
    }
  }
}
