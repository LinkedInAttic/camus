//package com.linkedin.batch.etl.kafka.schemaregistry;
//
//import org.apache.avro.repository.JdbcSchemaRepository;
//import org.apache.avro.repository.SchemaAndId;
//import org.apache.avro.repository.SchemaChangeValidator;
//import org.apache.avro.repository.SchemaIdGenerator;
//import org.apache.avro.repository.SchemaRepository;
//import org.apache.hadoop.mapreduce.JobContext;
//
//import com.linkedin.batch.etl.kafka.mapred.EtlInputFormat;
//
///**
// * Wrapper around the Schema Repository Interface
// * 
// * @author
// * 
// */
//public class SchemaRegistryClient implements SchemaRepository
//{
//  public static SchemaRepository registry = null;
//
//  private SchemaRegistryClient(JobContext context)
//  {
//    // Generate the kind of Schema Repository specified
//    // Currently the support includes for JDBC and HTTP. Implementing support for the JDBC
//    // as of now.
//    if (EtlInputFormat.getSchemaRegistryType(context) == "jdbc")
//    {
//      String validatorClass = EtlInputFormat.getValidatorClassName(context);
//      String idGeneratorClass = EtlInputFormat.getIdGeneratorClassName(context);
//      SchemaChangeValidator validator;
//      SchemaIdGenerator idGenerator;
//      try
//      {
//        validator = (SchemaChangeValidator) Class.forName(validatorClass).newInstance();
//        idGenerator = (SchemaIdGenerator) Class.forName(idGeneratorClass).newInstance();
//        registry =
//            new JdbcSchemaRepository(validator,
//                                     idGenerator,
//                                     EtlInputFormat.getJDBCSchemaRegistryUser(context),
//                                     EtlInputFormat.getJDBCSchemaRegistryPassword(context),
//                                     EtlInputFormat.getEtlSchemaRegistryUrl(context),
//                                     EtlInputFormat.getJDBCSchemaRegistryDriver(context),
//                                     Integer.parseInt(EtlInputFormat.getEtlSchemaRegistryPoolSize(context)));
//      }
//      catch (Exception e)
//      {
//        System.err.println("Error in creating the validator and idGenerator instances for Schema Registry ");
//      }
//    }
//    // TODO: Implement support for other schema repositories
//  }
//
//  /**
//   * Singleton pattern to return the registry connection details
//   * 
//   * @param context
//   * @return
//   */
//  public static SchemaRepository getInstance(JobContext context)
//  {
//    if (registry == null)
//    {
//      return (new SchemaRegistryClient(context)).registry;
//    }
//    else
//    {
//      return registry;
//    }
//  }
//
//  public void close()
//  {
//    // TODO Auto-generated method stub
//
//  }
//
//  public Iterable<String> listIds(String arg0)
//  {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//  public Iterable<String> listSources()
//  {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//  public String lookup(String arg0, String arg1)
//  {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//  public SchemaAndId lookupLatest(String arg0)
//  {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//  public String register(String arg0, String arg1)
//  {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//}
