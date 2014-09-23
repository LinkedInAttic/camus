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

        long testTimestamp = 1406947271534L;
        long expectedTimestamp = testTimestamp;
        
        Properties testProperties = new Properties();
        testProperties.setProperty("camus.message.timestamp.format", "unix_milliseconds");

        JsonStringMessageDecoder testDecoder = new JsonStringMessageDecoder();
        testDecoder.init(testProperties, "testTopic");
        String payload = "{\"timestamp\":  "+ testTimestamp + ", \"myData\": \"myValue\"}";
        byte[] bytePayload = payload.getBytes();

        CamusWrapper actualResult = testDecoder.decode(bytePayload);
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
        String payload = "{\"timestamp\":  "+ testTimestamp + ", \"myData\": \"myValue\"}";
        byte[] bytePayload = payload.getBytes();
        CamusWrapper actualResult = testDecoder.decode(bytePayload);
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
        String payload = "{\"timestamp\":  \""+ testTimestamp + "\", \"myData\": \"myValue\"}";
        byte[] bytePayload = payload.getBytes();
        CamusWrapper actualResult = testDecoder.decode(bytePayload);
        long actualTimestamp = actualResult.getTimestamp();

        assertEquals(expectedTimestamp, actualTimestamp);

    }

    @Test(expected=RuntimeException.class)
    public void testBadJsonInput() {
        byte[] bytePayload = "{\"key: value}".getBytes();

        JsonStringMessageDecoder testDecoder = new JsonStringMessageDecoder();
        CamusWrapper actualResult = testDecoder.decode(bytePayload);
    }

}
