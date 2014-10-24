package com.linkedin.camus.workallocater;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

public abstract class WorkAllocator {

  protected Properties props;

  public void init(Properties props){
      this.props = props;
  }

  public abstract List<InputSplit> allocateWork(List<CamusRequest> requests,
      JobContext context) throws IOException ;


}
