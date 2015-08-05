package com.linkedin.camus.workallocater;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import com.linkedin.camus.etl.kafka.mapred.EtlSplit;


public class BaseAllocator extends WorkAllocator {

  protected Properties props;

  public void init(Properties props) {
    this.props = props;
  }

  protected void reverseSortRequests(List<CamusRequest> requests) {
    // Reverse sort by size
    Collections.sort(requests, new Comparator<CamusRequest>() {
      @Override
      public int compare(CamusRequest o1, CamusRequest o2) {
        if (o2.estimateDataSize() == o1.estimateDataSize()) {
          return 0;
        }
        if (o2.estimateDataSize() < o1.estimateDataSize()) {
          return -1;
        } else {
          return 1;
        }
      }
    });
  }

  @Override
  public List<InputSplit> allocateWork(List<CamusRequest> requests, JobContext context) throws IOException {
    int numTasks = context.getConfiguration().getInt("mapred.map.tasks", 30);

    reverseSortRequests(requests);

    List<InputSplit> kafkaETLSplits = new ArrayList<InputSplit>();

    for (int i = 0; i < numTasks; i++) {
      if (requests.size() > 0) {
        kafkaETLSplits.add(new EtlSplit());
      }
    }

    for (CamusRequest r : requests) {
      getSmallestMultiSplit(kafkaETLSplits).addRequest(r);
    }

    return kafkaETLSplits;
  }

  protected EtlSplit getSmallestMultiSplit(List<InputSplit> kafkaETLSplits) throws IOException {
    EtlSplit smallest = (EtlSplit) kafkaETLSplits.get(0);

    for (int i = 1; i < kafkaETLSplits.size(); i++) {
      EtlSplit challenger = (EtlSplit) kafkaETLSplits.get(i);
      if ((smallest.getLength() == challenger.getLength() && smallest.getNumRequests() > challenger.getNumRequests())
          || smallest.getLength() > challenger.getLength()) {
        smallest = challenger;
      }
    }

    return smallest;
  }

}
