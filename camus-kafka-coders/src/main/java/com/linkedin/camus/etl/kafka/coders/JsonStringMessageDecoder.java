package com.linkedin.camus.etl.kafka.coders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.text.SimpleDateFormat;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;

import org.apache.log4j.Logger;


/**
 * MessageDecoder class that will convert the payload into a JSON object,
 * look for a field named 'timestamp', and then set the CamusWrapper's
 * timestamp property to the record's timestamp.  If the JSON does not have
 * a timestamp, then System.currentTimeMillis() will be used.
 * This MessageDecoder returns a CamusWrapper that works with Strings payloads,
 * since JSON data is always a String.
 */
public class JsonStringMessageDecoder extends MessageDecoder<byte[], String> {
	private static org.apache.log4j.Logger log = Logger.getLogger(JsonStringMessageDecoder.class);

	// Property for format of timestamp in JSON timestamp field.
	public  static final String CAMUS_MESSAGE_TIMESTAMP_FORMAT = "camus.message.timestamp.format";
	public  static final String DEFAULT_TIMESTAMP_FORMAT       = "[dd/MMM/yyyy:HH:mm:ss Z]";

	// Property for the JSON field name of the timestamp.
	public  static final String CAMUS_MESSAGE_TIMESTAMP_FIELD  = "camus.message.timestamp.field";
	public  static final String DEFAULT_TIMESTAMP_FIELD        = "timestamp";

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
                JSONObject jobj;

		payloadString =  new String(payload);

		// Parse the payload into a JsonObject.
		try {
                        jobj = (JSONObject) JSONValue.parse(payloadString);
		} catch (RuntimeException e) {
			log.error("Caught exception while parsing JSON string '" + payloadString + "'.");
			throw new RuntimeException(e);
		}

		// Attempt to read and parse the timestamp element into a long.
		if (jobj.get(timestampField) != null) {
			Object ts = jobj.get(timestampField);
			if (ts instanceof String) {
				try {
					timestamp = new SimpleDateFormat(timestampFormat).parse((String)ts).getTime();
				} catch (Exception e) {
					log.error("Could not parse timestamp '" + ts + "' while decoding JSON message.");
				}
			} else if (ts instanceof Long) {
				timestamp = (Long)ts;
			}
		}

		// If timestamp wasn't set in the above block,
		// then set it to current time.
		final long now = System.currentTimeMillis();
		if (timestamp == 0) {
			log.warn("Couldn't find or parse timestamp field '" + timestampField + "' in JSON message, defaulting to current time.");
			timestamp = now;
		}

		// If timestamp is from >12hours ago, reset it.
		if (now - timestamp > 12 * 60 * 60 * 1000) {
			timestamp = now;
		} else if (timestamp > now) {
			// Can't be in the future, right?
			timestamp = now;
		}

		return new CamusWrapper<String>(payloadString, timestamp);
	}
}
