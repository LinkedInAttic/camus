package com.linkedin.camus.etl.kafka.coders;

public class KafkaMessageDecoderException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3908501575563624012L;

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