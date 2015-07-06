package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

public class TestJsonStringMessageDecoder {

  @Test
  public void testDecodeUnixMilliseconds() {

    // Test that the decoder extracts unix_milliseconds
    // It should take and return milliseconds

    long expectedTimestamp = 1406947271534L;

    Properties testProperties = new Properties();
    testProperties.setProperty("camus.message.timestamp.format", "unix_milliseconds");

    JsonStringMessageDecoder testDecoder = new JsonStringMessageDecoder();
    testDecoder.init(testProperties, "testTopic");
    String payload = "{\"timestamp\":  " + expectedTimestamp + ", \"myData\": \"myValue\"}";
    byte[] bytePayload = payload.getBytes();

    CamusWrapper actualResult = testDecoder.decode(new TestMessage().setPayload(bytePayload));
    long actualTimestamp = actualResult.getTimestamp();
    assertEquals(expectedTimestamp, actualTimestamp);
  }

  @Test
  public void testDecodeUnixSeconds() {

    // Test that the decoder extracts unix_seconds
    // It should receive seconds and return milliseconds

    long testTimestamp = 140694727L;
    long expectedTimestamp = 140694727000L;

    Properties testProperties = new Properties();
    testProperties.setProperty("camus.message.timestamp.format", "unix_seconds");

    JsonStringMessageDecoder testDecoder = new JsonStringMessageDecoder();
    testDecoder.init(testProperties, "testTopic");
    String payload = "{\"timestamp\":  " + testTimestamp + ", \"myData\": \"myValue\"}";
    byte[] bytePayload = payload.getBytes();
    CamusWrapper actualResult = testDecoder.decode(new TestMessage().setPayload(bytePayload));
    long actualTimestamp = actualResult.getTimestamp();

    assertEquals(expectedTimestamp, actualTimestamp);
  }

  @Test
  public void testDecodeWithTimestampFormat() {

    // Test that we can specify a date and a pattern and
    // get back unix timestamp milliseconds

    String testFormat = "yyyy-MM-dd HH:mm:ss Z";
    String testTimestamp = "2014-02-01 01:15:27 UTC";
    long expectedTimestamp = 1391217327000L;

    Properties testProperties = new Properties();
    testProperties.setProperty("camus.message.timestamp.format", testFormat);

    JsonStringMessageDecoder testDecoder = new JsonStringMessageDecoder();
    testDecoder.init(testProperties, "testTopic");
    String payload = "{\"timestamp\":  \"" + testTimestamp + "\", \"myData\": \"myValue\"}";
    byte[] bytePayload = payload.getBytes();
    CamusWrapper actualResult = testDecoder.decode(new TestMessage().setPayload(bytePayload));
    long actualTimestamp = actualResult.getTimestamp();

    assertEquals(expectedTimestamp, actualTimestamp);

  }

  @Test
  public void testDecodeWithIsoFormat() {

    // Test that when no format is specified then both
    // ISO 8601 format: 1994-11-05T08:15:30-05:00
    // and 1994-11-05T13:15:30Z are accepted

    String testTimestamp1 = "1994-11-05T08:15:30-05:00";
    String testTimestamp2 = "1994-11-05T13:15:30Z";
    long expectedTimestamp = 784041330000L;

    Properties testProperties = new Properties();

    JsonStringMessageDecoder testDecoder = new JsonStringMessageDecoder();
    testDecoder.init(testProperties, "testTopic");
    String payload = "{\"timestamp\":  \"" + testTimestamp1 + "\", \"myData\": \"myValue\"}";
    byte[] bytePayload = payload.getBytes();
    CamusWrapper actualResult = testDecoder.decode(new TestMessage().setPayload(bytePayload));
    long actualTimestamp = actualResult.getTimestamp();

    assertEquals(expectedTimestamp, actualTimestamp);

    payload = "{\"timestamp\":  \"" + testTimestamp2 + "\", \"myData\": \"myValue\"}";
    bytePayload = payload.getBytes();
    actualResult = testDecoder.decode(new TestMessage().setPayload(bytePayload));
    actualTimestamp = actualResult.getTimestamp();

    assertEquals(expectedTimestamp, actualTimestamp);
  }

  @Test(expected = RuntimeException.class)
  public void testBadJsonInput() {
    byte[] bytePayload = "{\"key: value}".getBytes();

    JsonStringMessageDecoder testDecoder = new JsonStringMessageDecoder();
    testDecoder.decode(new TestMessage().setPayload(bytePayload));
  }

  @Test
  public void testMultiTimestampFields() {
    long testTimestamp1 = 1406947271534L;
    long expectedTimestamp1 = 1406947271534L;

    String testTimestamp2 = "1994-11-05T13:15:30Z";
    long expectedTimestamp2 = 784041330000L;

    Properties testProperties = new Properties();
    testProperties.setProperty("camus.message.timestamp.1.format", "unix_milliseconds");
    testProperties.setProperty("camus.message.timestamp.1.field", "unix_tiemestamp");
    testProperties.setProperty("camus.message.timestamp.2.format", "ISO-8601");
    testProperties.setProperty("camus.message.timestamp.2.field", "custom_timestamp");

    JsonStringMessageDecoder testDecoder = new JsonStringMessageDecoder();
    testDecoder.init(testProperties, "testTopic");

    String payload = "{\"unix_tiemestamp\":  \"" + testTimestamp1 + "\", \"myData\": \"myValue\"}";
    CamusWrapper actualResult = testDecoder.decode(new TestMessage().setPayload(payload.getBytes()));
    long actualTimestamp = actualResult.getTimestamp();

    assertEquals(expectedTimestamp1, actualTimestamp);

    payload = "{\"custom_timestamp\":  \"" + testTimestamp2 + "\", \"myData\": \"myValue\"}";
    actualResult = testDecoder.decode(new TestMessage().setPayload(payload.getBytes()));
    actualTimestamp = actualResult.getTimestamp();

    assertEquals(expectedTimestamp2, actualTimestamp);
  }

  @Test
  public void testNestedJSONField() {
    long expectedTimestamp = 1406947271534L;

    Properties testProperties = new Properties();

    testProperties.setProperty("camus.message.timestamp.format", "unix_milliseconds");
    testProperties.setProperty("camus.message.timestamp.field", "metadata.timestamp");

    JsonStringMessageDecoder testDecoder = new JsonStringMessageDecoder();
    testDecoder.init(testProperties, "testTopic");
    String payload = "{\"metadata\": {\"timestamp\":  " + expectedTimestamp + "}, \"myData\": \"myValue\"}";
    byte[] bytePayload = payload.getBytes();

    CamusWrapper actualResult = testDecoder.decode(new TestMessage().setPayload(bytePayload));
    long actualTimestamp = actualResult.getTimestamp();
    assertEquals(expectedTimestamp, actualTimestamp);
  }

}
