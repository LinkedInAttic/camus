package com.linkedin.camus.workallocater;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.mapred.EtlSplit;

public class TopicGroupingAllocator extends BaseAllocator {
  public static final String CAMUS_TOPIC_GROUP_TARGET_PERCENTAGE = "camus.topic.group.target.percentage";

  @Override
  public List<InputSplit> allocateWork(List<CamusRequest> requests, JobContext context) throws IOException {
    int numTasks = context.getConfiguration()
        .getInt("mapred.map.tasks", 30);
    List<InputSplit> kafkaETLSplits = new ArrayList<InputSplit>();
    
    for (int i = 0; i < numTasks; i++) {
      if (requests.size() > 0) {
        kafkaETLSplits.add(new EtlSplit());
      }
    }
    
    long avgSize = getAvgRequestSize(requests);
    
    List<CamusRequest> groupedRequests = groupSmallRequest(requests, avgSize);
    
    reverseSortRequests(groupedRequests);

    for (CamusRequest r : requests) {
      EtlSplit split = getSmallestMultiSplit(kafkaETLSplits);
      
      if (r instanceof GroupedRequest){
        for (CamusRequest r1 : (GroupedRequest)r) {
          split.addRequest(r1);
        }
      } else {
        split.addRequest(r);
      }
    }

    return kafkaETLSplits;
  }
  
  private long getAvgRequestSize(List<CamusRequest> requests){
    long size = 0;
    
    for (CamusRequest cr : requests){
      size += cr.estimateDataSize();
    }
    
    return size / requests.size();
  }
  
  private List<CamusRequest> groupSmallRequest(List<CamusRequest> requests, long avgSize){
    List<CamusRequest> finalRequests = new ArrayList<CamusRequest>();
    
    Map<String, List<CamusRequest>> groupedRequests = new HashMap<String, List<CamusRequest>>();
    
    double targetPercent = Long.valueOf(props.getProperty(CAMUS_TOPIC_GROUP_TARGET_PERCENTAGE, "50")) / 100.0;
    long targetSize = (long) (avgSize * targetPercent);
    
    for (CamusRequest cr : requests){
      if (cr.estimateDataSize() < targetSize){
        if (! groupedRequests.containsKey(cr.getTopic())){
          groupedRequests.put(cr.getTopic(), new ArrayList<CamusRequest>());
        }
        groupedRequests.get(cr.getTopic()).add(cr);
      } else {
        finalRequests.add(cr);
      }
    }
    
    for (List<CamusRequest> group : groupedRequests.values()){
      finalRequests.add(new GroupedRequest(group));
    }
    
    return finalRequests;
  }
  
  class GroupedRequest implements CamusRequest, Iterable<CamusRequest>{
    private List<CamusRequest> requests;
    private long size = -1;
    
    public GroupedRequest(List<CamusRequest> requests) {
      this.requests = requests;
    }
    
    @Override
    public void readFields(DataInput arg0) throws IOException {
      requests = new ArrayList<CamusRequest>();
      int size = arg0.readInt();
      for (int i = 0; i < size; i++){
        
        //TODO: factor out kafka specific request functionality 
        CamusRequest request = new EtlRequest();
        request.readFields(arg0);
        requests.add(request);
      }
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
      arg0.writeInt(requests.size());
      
      for (CamusRequest cr : requests){
        cr.write(arg0);
      } 
    }

    @Override
    public void setLatestOffset(long latestOffset) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setEarliestOffset(long earliestOffset) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setOffset(long offset) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setURI(URI uri) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getTopic() {
      return requests.get(0).getTopic();
    }

    @Override
    public URI getURI() {
      return requests.get(0).getURI();
    }

    @Override
    public int getPartition() {
      return requests.get(0).getPartition();
    }

    @Override
    public long getOffset() {
      return requests.get(0).getOffset();
    }

    @Override
    public boolean isValidOffset() {
      return requests.get(0).isValidOffset();
    }

    @Override
    public long getEarliestOffset() {
      return requests.get(0).getEarliestOffset();
    }

    @Override
    public long getLastOffset() {
      return requests.get(0).getLastOffset();
    }

    @Override
    public long getLastOffset(long time) {
      return requests.get(0).getLastOffset(time);
    }

    @Override
    public long estimateDataSize() {
      if (size == -1){
        for (CamusRequest cr : requests){
          size += cr.estimateDataSize();
        } 
      }
      return estimateDataSize();
    }

    @Override
    public long estimateDataSize(long endTime) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<CamusRequest> iterator() {
      return requests.iterator();
    }
  }
}
