package com.linkedin.camus.workallocater;

import com.google.common.collect.Lists;
import com.linkedin.camus.etl.kafka.mapred.EtlSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.List;

public class RoundRobinAllocator extends BaseAllocator {

    /**
     * Creates splits using round-robin algorithm
     *
     * @param requests sorted by topic requests
     * @param context  camus job context
     * @return splits
     * @throws IOException
     */
    @Override
    public List<InputSplit> allocateWork(List<CamusRequest> requests, JobContext context) throws IOException {
        int numTasks = Math.min(getMapTasksCount(context), requests.size());
        List<InputSplit> kafkaETLSplits = Lists.newArrayListWithCapacity(numTasks);

        for (int i = 0; i < numTasks; i++) {
            if (requests.size() > 0) {
                kafkaETLSplits.add(new EtlSplit());
            }
        }

        int currentSplitIndex = 0;
        for (CamusRequest request : requests) {
            if (currentSplitIndex >= kafkaETLSplits.size()) {
                currentSplitIndex = 0;
            }

            ((EtlSplit) kafkaETLSplits.get(currentSplitIndex)).addRequest(request);
            currentSplitIndex++;
        }
        return kafkaETLSplits;
    }
}
