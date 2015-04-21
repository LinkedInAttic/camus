package com.linkedin.camus.etl.kafka.coders;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Properties;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.apache.log4j.Logger;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;


/**
 * MessageDecoder class that will work with Airbnb Jitney message format, which is an json array
 * with two elements, the one element is the Jitney message meta data, and the second element is
 * payload, the as below:
 * [{"eventType":"example_event_type",
 *   "id":"example_id",
 *   "createdAt":1428000472668,
 *   "sequenceOrigin":1428000397254,
 *   "sequenceNumber":3,
 *   "sourceName":"example_source",
 *   "sourceIpAddress":"127.0.0.1",
 *   "sourceHostname":"localhost",
 *   "version":1},
 *   {"example_payload":
 *     {"userId":1,
 *     "activityType":"test_activity",
 *     ...
 *     },
 *     "eventType":"example_event_type"
 *   }]
 *
 * This decoder converts the payload into a JSON array, and look for a field named 'createdAt' in
 * the each of the JSON object, then set the CamusWrapper's timestamp property to the record's
 * timestamp. If the JSON does not have a timestamp, then System.currentTimeMillis() will be used.
 * The timestamp will be used for placing the message into the correct partition in kafka.
 * This MessageDecoder returns a CamusWrapper that works with Strings payloads.
 */
public class JitneyTimestampDecoder extends MessageDecoder<byte[], String> {
  private static org.apache.log4j.Logger log = Logger.getLogger(JsonStringMessageDecoder.class);

  // Property for format of timestamp in JSON timestamp field.
  public  static final String CAMUS_MESSAGE_TIMESTAMP_FORMAT = "camus.message.timestamp.format";
  public  static final String DEFAULT_TIMESTAMP_FORMAT       = "[dd/MMM/yyyy:HH:mm:ss Z]";

  // Property for the JSON field name of the timestamp.
  public  static final String CAMUS_MESSAGE_TIMESTAMP_FIELD  = "camus.message.timestamp.field";
  public  static final String DEFAULT_TIMESTAMP_FIELD        = "createdAt";

  private String timestampFormat;
  private String timestampField;

  @Override
  public void init(Properties props, String topicName) {
    this.props     = props;
    this.topicName = topicName;

    timestampFormat = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FORMAT, DEFAULT_TIMESTAMP_FORMAT);
    timestampField  = props.getProperty(CAMUS_MESSAGE_TIMESTAMP_FIELD,  DEFAULT_TIMESTAMP_FIELD);
  }

  @Override
  public CamusWrapper<String> decode(byte[] payload) {
    long       timestamp = 0;
    String     payloadString;
    // Need to specify Charset because the default might not be UTF-8.
    // Bug fix for https://jira.airbnb.com:8443/browse/PRODUCT-5551.
    payloadString =  new String(payload, Charset.forName("UTF-8"));

    // Parse the payload into a JsonObject.
    try {
      Object obj = JSONValue.parse(payloadString);
      if (obj instanceof JSONObject) {
        timestamp = getTimestamp((JSONObject) obj);
      } else if (obj instanceof JSONArray) {
        for (Object elem : (JSONArray) obj) {
          timestamp = getTimestamp((JSONObject) elem);
          if (timestamp != 0L) {
            break;
          }
        }
      }
    } catch (RuntimeException e) {
      log.error("Caught exception while parsing JSON string '" + payloadString + "'.");
      throw new RuntimeException(e);
    }

    // If timestamp wasn't set in the above block,
    // then set it to current time.
    if (timestamp == 0) {
      log.warn("Couldn't find or parse timestamp field '" + timestampField + "' in JSON message, defaulting to current time.");
      timestamp = System.currentTimeMillis();;
    }

    return new CamusWrapper<String>(payloadString, timestamp);
  }

  private long getTimestamp(JSONObject jsonObject) {
    // Attempt to read and parse the timestamp element into a long.
    if (jsonObject.get(timestampField) != null) {
      Object ts = jsonObject.get(timestampField);
      if (ts instanceof String) {
        try {
          return new SimpleDateFormat(timestampFormat).parse((String)ts).getTime();
        } catch (Exception e) {
          log.error("Could not parse timestamp '" + ts + "' while decoding JSON message.");
        }
      } else if (ts instanceof Long) {
        return (Long)ts;
      }
    }
    return 0L;
  }
}
