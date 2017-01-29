package com.linkedin.camus.etl.kafka.coders;

import java.util.Properties;

import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

/**
 * MessageDecoder class that will convert the payload into a String object,
 * look for a field named 'timestamp', and then set the CamusWrapper's
 * timestamp property to the record's timestamp. System.currentTimeMillis() is used.
 * This MessageDecoder returns a CamusWrapper that works with Strings payloads.
 * 
 * @author Marcelo Valle (mvalle@keedio.com https://github.com/keedio)
 * 
 */


public class StringMessageDecoder extends MessageDecoder<Message, String> {

	private static final org.apache.log4j.Logger log = Logger.getLogger(StringMessageDecoder.class);

	@Override
	public void init(Properties props, String topicName) {
		this.props     = props;
		this.topicName = topicName;
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

		// Set the timestamp to current time.
		timestamp = System.currentTimeMillis();

		return new CamusWrapper<String>(payloadString, timestamp);
	}
}
