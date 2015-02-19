package com.linkedin.camus.etl.kafka.common;

public class Source {

  private long count;
  private long start;
  private String service;
  private String server;

  public Source(String server, String service, long monitorGranularity) {
    this.server = server;
    this.service = service;
    this.start = monitorGranularity;
  }

  public Source() {

  }

  public long getCount() {
    return count;
  }

  public String getServer() {
    return server;
  }

  public String getService() {
    return service;
  }

  public long getStart() {
    return start;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public void setServer(String server) {
    this.server = server;
  }

  public void setService(String service) {
    this.service = service;
  }

  public void setStart(long start) {
    this.start = start;
  }

  @Override
  public int hashCode() {

    return (server + service + start).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return this.hashCode() == obj.hashCode();
  }

  @Override
  public String toString() {
    return "{" + server + "," + service + "," + start + "}";
  }

}
