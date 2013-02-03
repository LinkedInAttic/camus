package com.linkedin.camus.coders;

import java.util.Properties;

/**
 * Decoder interface. Implemenations should return a CamusWrapper with timestamp
 * set at the very least.  Camus will instantiate a descendent of this class 
 * based on the property camus.decoder.wrapper.class.
 * @author kgoodhop
 *
 * @param <M> The message type to be decoded
 * @param <R> The type of the decoded message
 */
public abstract class MessageDecoder<M,R> {
    public static String CAMUS_DECODER_WRAPPER_CLASS = "camus.decoder.wrapper.class";
	protected Properties props;
	protected String topicName;
	
	public void init(Properties props, String topicName){
	    this.props = props;
        this.topicName = topicName;
	}

	public abstract CamusWrapper<R> decode(M message) ;

}
