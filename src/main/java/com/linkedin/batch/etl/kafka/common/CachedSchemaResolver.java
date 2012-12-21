package com.linkedin.batch.etl.kafka.common;

//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.avro.Schema;
//import org.apache.avro.repository.SchemaAndId;
//
//import com.linkedin.batch.etl.kafka.schemaregistry.SchemaRegistryClient;
//
///**
// * Resolve schema from schema registry service. It maintains a local cache for previously
// * accessed schema.
// * 
// * @author lguo
// */
//public class CachedSchemaResolver implements SchemaResolver
//{
//
//  // Schema registry client
//  private SchemaRegistryClient           client       = null;
//  // Local cache so we don't constantly query the registry.
//  private final Map<SchemaAndId, Schema> cache        =
//                                                          new HashMap<SchemaAndId, Schema>();
//  // target schema for the event
//  private Schema                         readerSchema = null;
//
//  /**
//   * Creates a registry resolver that will go to the schema registry and sets the target
//   * registry to the latest found by the topic Name
//   * 
//   * @param url
//   * @param topicName
//   * @throws IOException
//   */
//  public CachedSchemaResolver(String url, String topicName) throws IOException
//  {
//    // Filtering would be required on the kind of SchemaRegistry
//    client = new SchemaRegistryClient(url);
//    String targetSchema = client.getLatestSchemaByName(topicName);
//    readerSchema = Schema.parse(targetSchema);
//  }
//
//  public CachedSchemaResolver(String url, Schema rs)
//  {
//    client = new SchemaRegistryClient(url);
//    readerSchema = rs;
//  }
//
//  public Schema resolve(byte[] bs)
//  {
//    SchemaId id = new SchemaId(bs);
//    Schema schema = cache.get(id);
//    if (schema != null)
//    {
//      return schema;
//    }
//    try
//    {
//      String s = client.getSchemaByID(id.toString());
//      schema = Schema.parse(s);
//      cache.put(id, schema);
//    }
//    catch (Exception e)
//    {
//      throw new RuntimeException("Error while resolving schema id:" + id + " msg:"
//          + e.getMessage());
//    }
//    return schema;
//  }
//
//  public Schema getTargetSchema(byte[] paramArrayOfByte)
//  {
//    // TODO Auto-generated method stub
//    return readerSchema;
//  }
//
// }
