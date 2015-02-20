package com.linkedin.camus.schemaregistry;

public interface Serde<O> {
  public byte[] toBytes(O obj);

  public O fromBytes(byte[] bytes);
}
