package com.linkedin.batch.etl.kafka.coders;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.specific.SpecificRecord;

public class Utils
{

  public static byte[] decode(String hex)
  {
    byte[] bytes = new byte[hex.length() / 2];

    for (int x = 0; x < bytes.length; x++)
    {
      bytes[x] = (byte) Integer.parseInt(hex.substring(2 * x, 2 * x + 2), 16);
    }

    return bytes;
  }

  public static String hex(byte[] bytes)
  {
    StringBuilder builder = new StringBuilder(2 * bytes.length);
    for (int i = 0; i < bytes.length; i++)
    {
      String hexString = Integer.toHexString(0xFF & bytes[i]);
      if (hexString.length() < 2)
      {
        hexString = "0" + hexString;
      }
      // System.out.println("Byte = " + bytes[i] + " Hex string = " + hexString);
      builder.append(hexString);
    }
    return builder.toString();
  }

  public static byte[] md5(byte[] bytes)
  {
    try
    {
      MessageDigest digest = MessageDigest.getInstance("md5");
      return digest.digest(bytes);
    }
    catch (NoSuchAlgorithmException e)
    {
      throw new IllegalStateException("This can't happen.", e);
    }
  }

  public static byte[] utf8(String s)
  {
    try
    {
      return s.getBytes("UTF-8");
    }
    catch (UnsupportedEncodingException e)
    {
      throw new IllegalStateException("This can't happen");
    }
  }

  public static <T> T notNull(T t)
  {
    if (t == null)
    {
      throw new IllegalArgumentException("Null value not allowed.");
    }
    else
    {
      return t;
    }
  }

  public static void croak(String message)
  {
    croak(message, 1);
  }

  public static void croak(String message, int exitCode)
  {
    System.err.println(message);
    System.exit(exitCode);
  }

  public static boolean equals(Object x, Object y)
  {
    if (x == y)
    {
      return true;
    }
    else if (x == null)
    {
      return false;
    }
    else if (y == null)
    {
      return false;
    }
    else
    {
      return x.equals(y);
    }
  }

  public static List<File> toFiles(List<String> strs)
  {
    List<File> files = new ArrayList<File>();
    for (String s : strs)
    {
      files.add(new File(s));
    }
    return files;
  }

  public static byte[] getBytes(String s, String e)
  {
    try
    {
      return s.getBytes(e);
    }
    catch (UnsupportedEncodingException exp)
    {
      throw new RuntimeException(exp);
    }
  }

  /**
   * This helper function copies the contents of a generic record into a specific record.
   * This function is useful in consuming records from a KafkaConsumer when you know the
   * actual specific record type. This function assumes that the schema to java class
   * mapping is already in the cache of SpecificData.
   * 
   * @param specificRecord
   *          An empty specific record which data is copied into
   * @param genericRecord
   *          The generic record that contains your data
   * @return the specific record
   * @throws AvroRuntimeException
   *           Throws an exception if there is a structural or type mismatch between the
   *           data and the specific record
   */
  public static <SR extends SpecificRecord> SR genericRecordToSpecificRecord(SR specificRecord,
                                                                             GenericData.Record genericRecord) throws AvroRuntimeException
  {
    int size =
        Math.min(genericRecord.getSchema().getFields().size(), specificRecord.getSchema()
                                                                             .getFields()
                                                                             .size());
    for (Schema.Field field : specificRecord.getSchema().getFields())
    {
      // for backwards compatibility. consider the case where there is a version mismatch
      // and a field was newly added
      if (field.pos() >= size)
      {
        continue;
      }
      Object fieldData = genericRecord.get(field.pos());
      Schema fieldSchema = resolveSchema(fieldData, field.schema());
      Object fieldValue = interpretFieldData(fieldData, fieldSchema);
      specificRecord.put(field.pos(), fieldValue);
    }
    return specificRecord;
  }

  // start helper functions for genericRecordToSpecificRecord
  private static Object interpretFieldData(Object fieldData, Schema fieldSchema) throws AvroRuntimeException
  {
    try
    {
      switch (fieldSchema.getType())
      {
      case ARRAY:
        return interpretArrayFieldData(fieldData, fieldSchema);
      case MAP:
        return interpretMapFieldData(fieldData, fieldSchema);
      default:
        return interpretScalarFieldData(fieldData, fieldSchema);
      }
    }
    catch (Exception e)
    {
      return new AvroRuntimeException("error interpreting field", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Object> interpretArrayFieldData(Object data, Schema schema)
  {
    List<Object> array = new ArrayList<Object>();
    for (Object o : ((Collection<Object>) data))
    {
      array.add(interpretFieldData(o, schema.getElementType()));
    }
    return array;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> interpretMapFieldData(Object data, Schema schema)
  {
    Map<String, Object> map = new HashMap<String, Object>();
    for (Map.Entry entry : ((Map<Object, Object>) data).entrySet())
    {
      map.put(entry.getKey().toString(),
              interpretFieldData(entry.getValue(), schema.getValueType()));
    }
    return map;
  }

  private static Object interpretScalarFieldData(Object data, Schema schema) throws Exception
  {
    Schema fieldSchema = resolveSchema(data, schema);
    Class fieldClass = SpecificData.get().getClass(fieldSchema);

    Object fieldValue;
    // if dealing with a null field, don't instantiate it
    if (Void.TYPE.equals(fieldClass))
    {
      fieldValue = null;
    }
    else
    {
      switch (fieldSchema.getType())
      {
      case FIXED:
        SpecificFixed fixed = (SpecificFixed) fieldClass.newInstance();
        fixed.bytes(((GenericData.Fixed) data).bytes());
        fieldValue = fixed;
        break;
      case RECORD:
        SpecificRecord innerSpecificRecord = (SpecificRecord) fieldClass.newInstance();
        fieldValue =
            genericRecordToSpecificRecord(innerSpecificRecord, (GenericData.Record) data);
        break;
      case ENUM:
        fieldValue =
            fieldClass.getDeclaredMethod("valueOf", String.class).invoke(null,
                                                                         data.toString());
        break;
      case STRING:
        fieldValue = (data != null) ? data.toString() : null;
        break;
      default:
        fieldValue = data;
      }
    }
    return fieldValue;
  }

  private static Schema resolveSchema(Object data, Schema schema)
  {
    switch (schema.getType())
    {
    case UNION:
      int schemaIndex = SpecificData.get().resolveUnion(schema, data);
      return schema.getTypes().get(schemaIndex);
    default:
      return schema;
    }
  }
  // end helper functions for genericRecordToSpecificRecord

}
