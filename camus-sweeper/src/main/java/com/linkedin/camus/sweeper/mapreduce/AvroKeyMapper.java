package com.linkedin.camus.sweeper.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class AvroKeyMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<GenericRecord>, Object>
{
  private AvroKey<GenericRecord> outKey;
  private AvroValue<GenericRecord> outValue;
  private boolean mapOnly = false;
  private Schema keySchema;

  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException
  {
    keySchema = AvroJob.getMapOutputKeySchema(context.getConfiguration());

    outValue = new AvroValue<GenericRecord>();
    outKey = new AvroKey<GenericRecord>();
    outKey.datum(new GenericData.Record(keySchema));

    if (context.getNumReduceTasks() == 0)
      mapOnly = true;
  }

  @Override
  protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException,
      InterruptedException
  {
    if (mapOnly)
    {
      context.write(key, NullWritable.get());
    }
    else
    {
      outValue.datum(key.datum());
      projectData(key.datum(), outKey.datum());
      context.write(outKey, outValue);
    }
  }

  private void projectData(GenericRecord source, GenericRecord target)
  {
    for (Field fld : target.getSchema().getFields())
    {
      if (fld.schema().getType() == Type.UNION)
      {
        Object obj = source.get(fld.name());
        Schema sourceSchema = GenericData.get().induce(obj);
        if (sourceSchema.getType() == Type.RECORD){
        for (Schema type : fld.schema().getTypes()){
          if (type.getFullName().equals(sourceSchema.getFullName())){
            GenericRecord record = new GenericData.Record(type);
            target.put(fld.name(), record);
            projectData((GenericRecord) obj, record);
            break;
          }
        }
        } else {
          target.put(fld.name(), source.get(fld.name()));
        }
      }
      else if (fld.schema().getType() == Type.RECORD)
      {
        GenericRecord record = (GenericRecord) target.get(fld.name());
        
        if (record == null){
          record = new GenericData.Record(fld.schema());
          target.put(fld.name(), record);
        }
        
        projectData((GenericRecord) source.get(fld.name()), record);
      }
      else
      {
        target.put(fld.name(), source.get(fld.name()));
      }
    }
  }

}
