package com.linkedin.camus.coders;

public class MessageEncoderException extends RuntimeException {
	private static final long serialVersionUID = 4758535014429852589L;

	public MessageEncoderException(String message) {
		super(message);
	}

	public MessageEncoderException(String message, Exception e) {
		super(message, e);
	}

	public MessageEncoderException(Exception e) {
		super(e);
	}
}