package com.linkedin.camus.sweeper.mapreduce;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class AvroKeyReducer
    extends Reducer<AvroKey<GenericRecord>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable> {

  public static enum EVENT_COUNTER {
    MORE_THAN_1,
    DEDUPED,
    RECORD_COUNT
  };

  private AvroKey<GenericRecord> outKey;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    outKey = new AvroKey<GenericRecord>();
  }

  @Override
  protected void reduce(AvroKey<GenericRecord> key, Iterable<AvroValue<GenericRecord>> values, Context context)
      throws IOException, InterruptedException {
    int numVals = 0;

    for (AvroValue<GenericRecord> av : values) {
      outKey.datum(av.datum());
      numVals++;
    }

    if (numVals > 1) {
      context.getCounter(EVENT_COUNTER.MORE_THAN_1).increment(1);
      context.getCounter(EVENT_COUNTER.DEDUPED).increment(numVals - 1);
    }
    context.getCounter(EVENT_COUNTER.RECORD_COUNT).increment(1);

    context.write(outKey, NullWritable.get());
  }
}
