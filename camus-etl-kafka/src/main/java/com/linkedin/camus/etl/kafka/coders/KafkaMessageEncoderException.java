package com.linkedin.camus.etl.kafka.coders;

public class KafkaMessageEncoderException extends RuntimeException {
	private static final long serialVersionUID = 4758535014429852589L;

	public KafkaMessageEncoderException(String message) {
		super(message);
	}

	public KafkaMessageEncoderException(String message, Exception e) {
		super(message, e);
	}

	public KafkaMessageEncoderException(Exception e) {
		super(e);
	}
}