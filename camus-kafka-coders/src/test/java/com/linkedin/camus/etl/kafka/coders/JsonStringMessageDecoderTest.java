package com.linkedin.camus.etl.kafka.coders;

import static org.junit.Assert.assertArrayEquals;

import java.nio.charset.Charset;

import org.junit.Test;

import com.linkedin.camus.coders.CamusWrapper;

public class JsonStringMessageDecoderTest {

  /**
   * Test for Bug https://jira.airbnb.com:8443/browse/PRODUCT-5551
   * JsonStringMessageDecoder need to handle unicode correctly when the default Charset is not utf-8
   * This test should fail if -Dfile.encoding=US-ASCII is set for JVM.
   */
  @Test
  public void testUnicodeDecoding() {
    JsonStringMessageDecoder decoder = new JsonStringMessageDecoder();
    byte[] payload = "{data:中}".getBytes(Charset.forName("UTF-8"));
    CamusWrapper<String> camusWrapper = decoder.decode(payload);
    byte[] decoded = camusWrapper.getRecord().getBytes(Charset.forName("UTF-8"));
    assertArrayEquals(payload, decoded);
  }
}
