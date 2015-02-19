package com.linkedin.camus.etl.kafka.common;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.io.Text;


public class ExceptionWritable extends Text {

  public ExceptionWritable() {
    super();
  }

  public ExceptionWritable(String exception) {
    super(exception);
  }

  public ExceptionWritable(Exception e) {
    set(null, e);
  }

  public ExceptionWritable(String message, Exception e) {
    set(message, e);
  }

  public void set(String message, Throwable e) {
    StringWriter strWriter = new StringWriter();
    PrintWriter printer = new PrintWriter(strWriter);

    if (message != null) {
      printer.write(message);
      printer.write("\n");
    }

    e.printStackTrace(printer);

    super.set(strWriter.toString());
    printer.close();
  }

  public void set(Exception e) {
    set(null, e);
  }

  public void set(ExceptionWritable other) {
    super.set(other.getBytes());
  }
}
