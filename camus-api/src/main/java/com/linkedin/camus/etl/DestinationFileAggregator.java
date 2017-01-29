package com.linkedin.camus.etl;


import org.apache.hadoop.fs.Path;

/**
 * The interface which could be used to get a list of all final files which is created by camus.
 * <p>
 * This opens up the possibility to for example notify other services which new files have been created.
 * Or just list them somewhere in the HDFS to be picked up later by another application.
 */
public interface DestinationFileAggregator {

    void addDestinationFile(Path path);

}
