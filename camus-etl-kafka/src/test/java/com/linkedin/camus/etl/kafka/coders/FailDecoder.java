package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;


public class FailDecoder extends MessageDecoder<byte[], String> {

  @Override
  public CamusWrapper<String> decode(byte[] message) {
    throw new RuntimeException("decoder failure");
  }

}
