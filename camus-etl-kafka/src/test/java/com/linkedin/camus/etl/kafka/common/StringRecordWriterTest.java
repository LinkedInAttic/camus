package com.linkedin.camus.etl.kafka.common;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;

public class StringRecordWriterTest {

  /**
   * Test for Bug https://jira.airbnb.com:8443/browse/PRODUCT-5551
   * StringRecordWriter need to handle unicode correctly when the default
   * Charset is not utf-8 This test should fail if -Dfile.encoding=US-ASCII is
   * set for JVM.
   */
  @Test
  public void testUnicode() throws IOException, InterruptedException {
    String recordDelimiter = "";
    OutputStream outputStream = mock(OutputStream.class);
    ArgumentCaptor<byte[]> argument = ArgumentCaptor.forClass(byte[].class);
    @SuppressWarnings("rawtypes")
    RecordWriter<IEtlKey, CamusWrapper> writer = new StringRecordWriter(
        outputStream, recordDelimiter);
    String record = "中"; // a unicode character
    CamusWrapper<String> value = new CamusWrapper<String>(record);
    writer.write(null, value);
    verify(outputStream).write(argument.capture());
    byte[] outputBytes = argument.getAllValues().get(0);
    assertArrayEquals(record.getBytes(Charset.forName("UTF-8")), outputBytes);
  }

  @Test
  public void testDelimiter() throws IOException, InterruptedException {
    String recordDelimiter = ";";
    OutputStream outputStream = mock(OutputStream.class);
    ArgumentCaptor<byte[]> argument = ArgumentCaptor.forClass(byte[].class);
    @SuppressWarnings("rawtypes")
    RecordWriter<IEtlKey, CamusWrapper> writer = new StringRecordWriter(
        outputStream, recordDelimiter);
    String record = "abc";
    CamusWrapper<String> value = new CamusWrapper<String>(record);
    writer.write(null, value);
    verify(outputStream).write(argument.capture());
    byte[] outputBytes = argument.getAllValues().get(0);
    byte[] inputBytes = (record + recordDelimiter).getBytes(Charset
        .forName("UTF-8"));
    assertArrayEquals(inputBytes, outputBytes);
  }
}
