package com.linkedin.batch.etl.kafka.coders;

public class KafkaMessageDecoderException extends Exception {

	public KafkaMessageDecoderException(String message) {
		super(message);
	}

	public KafkaMessageDecoderException(String message, Exception e) {
		super(message, e);
	}
	
	public KafkaMessageDecoderException(Exception e) {
		super(e);
	}
}