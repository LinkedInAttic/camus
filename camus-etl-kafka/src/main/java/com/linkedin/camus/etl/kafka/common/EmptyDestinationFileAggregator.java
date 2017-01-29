package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.etl.DestinationFileAggregator;
import org.apache.hadoop.fs.Path;

public class EmptyDestinationFileAggregator implements DestinationFileAggregator {

    @Override
    public void addDestinationFile(Path path) {
        // do nothing
    }

}
