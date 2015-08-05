package com.linkedin.camus.sweeper.mapreduce;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.linkedin.camus.sweeper.utils.RelaxedAvroSerialization;


@SuppressWarnings("rawtypes")
public abstract class CamusSweeperJob {
  protected Logger log;

  public CamusSweeperJob setLogger(Logger log) {
    this.log = log;
    return this;
  }

  public abstract void configureJob(String topic, Job job);

  protected void configureInput(Job job, Class<? extends InputFormat> inputFormat, Class<? extends Mapper> mapper,
      Class<?> mapOutKey, Class<?> mapOutValue) {
    job.setInputFormatClass(inputFormat);
    job.setMapperClass(mapper);
    job.setMapOutputKeyClass(mapOutKey);
    job.setMapOutputValueClass(mapOutValue);
    RelaxedAvroSerialization.addToConfiguration(job.getConfiguration());
  }

  protected void configureOutput(Job job, Class<? extends OutputFormat> outputFormat, Class<? extends Reducer> reducer,
      Class<?> outKey, Class<?> outValue) {
    job.setOutputFormatClass(outputFormat);
    job.setReducerClass(reducer);
    job.setOutputKeyClass(outKey);
    job.setOutputValueClass(outValue);
  }

  protected String getConfValue(Job job, String topic, String key, String defaultStr) {
    return job.getConfiguration().get(topic + "." + key, job.getConfiguration().get(key, defaultStr));
  }

  protected String getConfValue(Job job, String topic, String key) {
    return getConfValue(job, topic, key, null);
  }
}
