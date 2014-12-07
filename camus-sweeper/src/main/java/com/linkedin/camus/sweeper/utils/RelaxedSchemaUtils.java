package com.linkedin.camus.sweeper.utils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;


/**
 * Class to parse the schema without name validation
 * 
 * The routines here might try to parse the schema twice.
 * The first time with name validation on, if it failed with SchemaParseException,
 * it will try to parse it again without name validation.
 * 
 * The reason we do this is if the schema was generated/serialized using avro-1.4
 * which has weaker check on field names (e.g. it allowed -, @, ' ').  However, 
 * if we are running avro-1.7 during deserialization and avro 1.7 has stronger name 
 * validation.  Change those field names would mean migration overhead.  
 * We might just have to live with the old schema names during the interim.
 * 
 * @author hcai
 *
 */
public class RelaxedSchemaUtils {
  private static final Log LOG = LogFactory.getLog(RelaxedSchemaUtils.class.getName());

  // This constant is coming from AvroJob, however it's defined as private
  private static final String CONF_INPUT_KEY_SCHEMA = "avro.schema.input.key";

  private static final String CONF_SKIP_NAME_VALIDATION = "camus.sweeper.skip.name.validation";

  public static boolean skipNameValidation(Configuration conf) {
    String validateStr = conf.get(CONF_SKIP_NAME_VALIDATION);
    LOG.info(CONF_SKIP_NAME_VALIDATION + ": " + validateStr);
    if (validateStr != null && Boolean.parseBoolean(validateStr)) {
      return true;
    }
    return false;
  }

  /**
   * This routine might try to parse the schema twice.
   * 
   * @param schemaStr
   * @return
   */
  public static Schema parseSchema(String schemaStr, Configuration conf) {
    Schema schema = null;
    try {
      schema = new Schema.Parser().parse(schemaStr);
    } catch (SchemaParseException ex) {
      boolean skipNameValidation = skipNameValidation(conf);
      if (skipNameValidation) {
        LOG.warn("Cannot parse schema. " + ex);
        LOG.info("Try one more time without name validation.");
        Schema.Parser parser = new Schema.Parser();
        schema = parser.setValidate(false).parse(schemaStr);
      } else {
        throw ex;
      }
    }
    return schema;
  }

  /**
   * Gets the job input key schema.
   * 
   * This is the equivalent code copied from AvroJob
   *
   * @param conf The job configuration.
   * @return The job input key schema, or null if not set.
   */
  public static Schema getInputKeySchema(Configuration conf) {
    String schemaString = conf.get(CONF_INPUT_KEY_SCHEMA);
    return schemaString != null ? parseSchema(schemaString, conf) : null;
  }

}
