package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;

import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.mapreduce.Mapper;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.kafka.common.EtlKey;


/**
 * KafkaETL mapper
 * 
 * input -- EtlKey, AvroWrapper
 * 
 * output -- EtlKey, AvroWrapper
 * 
 */
public class EtlMapper extends Mapper<EtlKey, CamusWrapper, EtlKey, CamusWrapper> {

  @Override
  public void map(EtlKey key, CamusWrapper val, Context context) throws IOException, InterruptedException {
    long startTime = System.currentTimeMillis();

    context.write(key, val);

    long endTime = System.currentTimeMillis();
    long mapTime = ((endTime - startTime));
    context.getCounter("total", "mapper-time(ms)").increment(mapTime);
  }
}
