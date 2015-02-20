package com.linkedin.camus.workallocater;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.mapred.EtlSplit;


public class TopicGroupingAllocator extends BaseAllocator {
  public final static String CAMUS_MAX_GROUP_SIZE_REDUCTION_FACTOR = "camus.max.group.size.reduction.factor";

  @Override
  public List<InputSplit> allocateWork(List<CamusRequest> requests, JobContext context) throws IOException {
    int numTasks = context.getConfiguration().getInt("mapred.map.tasks", 30);
    List<InputSplit> kafkaETLSplits = new ArrayList<InputSplit>();

    for (int i = 0; i < numTasks; i++) {
      if (requests.size() > 0) {
        kafkaETLSplits.add(new EtlSplit());
      }
    }

    List<CamusRequest> groupedRequests = groupSmallRequest(requests, context);

    reverseSortRequests(groupedRequests);

    for (CamusRequest r : groupedRequests) {
      EtlSplit split = getSmallestMultiSplit(kafkaETLSplits);
      for (CamusRequest r1 : (GroupedRequest) r) {
        split.addRequest(r1);
      }
    }

    return kafkaETLSplits;
  }

  private List<CamusRequest> groupSmallRequest(List<CamusRequest> requests, JobContext context) {
    List<CamusRequest> finalRequests = new ArrayList<CamusRequest>();

    Map<String, List<CamusRequest>> requestsTopicMap = new HashMap<String, List<CamusRequest>>();
    long totalEstimatedDataSize = 0;

    for (CamusRequest cr : requests) {
      if (!requestsTopicMap.containsKey(cr.getTopic())) {
        requestsTopicMap.put(cr.getTopic(), new ArrayList<CamusRequest>());
      }
      requestsTopicMap.get(cr.getTopic()).add(cr);
      totalEstimatedDataSize += cr.estimateDataSize();
    }

    long maxSize =
        totalEstimatedDataSize / requests.size()
            / context.getConfiguration().getInt(CAMUS_MAX_GROUP_SIZE_REDUCTION_FACTOR, 3);

    for (List<CamusRequest> topic : requestsTopicMap.values()) {
      long size = 0;
      List<CamusRequest> groupedRequests = new ArrayList<CamusRequest>();

      for (CamusRequest cr : topic) {
        if (size + cr.estimateDataSize() >= maxSize) {
          if (groupedRequests.size() > 0)
            finalRequests.add(new GroupedRequest(groupedRequests));

          groupedRequests = new ArrayList<CamusRequest>();
          size = 0;
        }

        groupedRequests.add(cr);
      }

      finalRequests.add(new GroupedRequest(groupedRequests));
    }

    return finalRequests;
  }

  class GroupedRequest implements CamusRequest, Iterable<CamusRequest> {
    private List<CamusRequest> requests;
    private long size = -1;

    public GroupedRequest(List<CamusRequest> requests) {
      this.requests = requests;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
      requests = new ArrayList<CamusRequest>();
      int size = arg0.readInt();
      for (int i = 0; i < size; i++) {

        //TODO: factor out kafka specific request functionality 
        CamusRequest request = new EtlRequest();
        request.readFields(arg0);
        requests.add(request);
      }
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
      arg0.writeInt(requests.size());

      for (CamusRequest cr : requests) {
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
      if (size == -1) {
        for (CamusRequest cr : requests) {
          size += cr.estimateDataSize();
        }
      }
      return size;
    }

    @Override
    public long estimateDataSize(long endTime) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<CamusRequest> iterator() {
      return requests.iterator();
    }

    @Override
    public void setAvgMsgSize(long size) {
      for (CamusRequest cr : requests) {
        cr.setAvgMsgSize(size);
      }
    }
  }
}
