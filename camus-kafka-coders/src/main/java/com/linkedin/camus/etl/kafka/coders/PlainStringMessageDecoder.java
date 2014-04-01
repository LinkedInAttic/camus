package com.linkedin.camus.etl.kafka.coders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.text.SimpleDateFormat;

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
public class PlainStringMessageDecoder extends MessageDecoder<byte[], String> {
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

		payloadString =  new String(payload);

		// Set the timestamp to current time.
		timestamp = System.currentTimeMillis();

		return new CamusWrapper<String>(payloadString, timestamp);
	}
}
