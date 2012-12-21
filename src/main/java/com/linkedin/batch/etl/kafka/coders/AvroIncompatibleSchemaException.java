package com.linkedin.batch.etl.kafka.coders;

/**
 * 
 * @author
 * 
 */
public class AvroIncompatibleSchemaException extends RuntimeException
{
  private static final long serialVersionUID = 1L;

  public AvroIncompatibleSchemaException(Throwable t)
  {
    super(t);
  }

  public AvroIncompatibleSchemaException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public AvroIncompatibleSchemaException(String msg)
  {
    super(msg);
  }
}
