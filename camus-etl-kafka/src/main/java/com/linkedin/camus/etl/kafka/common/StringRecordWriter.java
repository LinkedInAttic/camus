package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.IEtlKey;

@SuppressWarnings("rawtypes")
public class StringRecordWriter extends RecordWriter<IEtlKey, CamusWrapper> {
  private final OutputStream output;
  private final String recordDelimiter;

  public StringRecordWriter(OutputStream compressedOutput,
      String recordDelimiter) {
    this.output = compressedOutput;
    this.recordDelimiter = recordDelimiter;
  }

  @Override
  public void write(IEtlKey ignore, CamusWrapper data) throws IOException {
    String record = (String) data.getRecord() + recordDelimiter;
    // Need to specify Charset because the default might not be UTF-8.
    // Bug fix for https://jira.airbnb.com:8443/browse/PRODUCT-5551.
    output.write(record.getBytes(Charset.forName("UTF-8")));
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException,
      InterruptedException {
    output.close();
  }
}