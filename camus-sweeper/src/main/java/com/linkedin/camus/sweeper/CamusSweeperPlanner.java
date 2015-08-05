package com.linkedin.camus.sweeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;


public abstract class CamusSweeperPlanner {
  protected Properties props;
  protected Set<Properties> outlierProperties = new HashSet<Properties>();
  private Logger log;

  public CamusSweeperPlanner setPropertiesLogger(Properties props, Logger log) {
    this.props = props;
    this.log = log;
    return this;
  }

  public abstract List<Properties> createSweeperJobProps(String topic, Path inputDir, Path outputDir, FileSystem fs)
      throws IOException;

  protected abstract List<Properties> createSweeperJobProps(String topic, Path inputDir, Path outputDir, FileSystem fs,
      CamusSweeperMetrics metrics) throws IOException;

  // Simple check for reprocessing depending on the modified time of the source and destination
  // folder
  protected boolean shouldReprocess(FileSystem fs, List<Path> sources, Path dest) throws IOException {

    log.debug("source:" + sources.toString());
    log.debug("dest:" + dest.toString());

    FileStatus destStatus = fs.getFileStatus(dest);
    long destinationModTime = destStatus.getModificationTime();

    for (Path source : sources) {
      if (shouldReprocess(fs, source, destinationModTime))
        return true;
    }

    return false;
  }

  private boolean shouldReprocess(FileSystem fs, Path source, long destinationModTime) throws IOException {
    FileStatus sourceStatus = fs.getFileStatus(source);

    log.debug("source mod:" + sourceStatus.getModificationTime());
    log.debug("dest mod:" + destinationModTime);

    if (sourceStatus.getModificationTime() > destinationModTime) {
      return true;
    }

    FileStatus[] statuses = fs.globStatus(new Path(source, "*"), new HiddenFilter());
    for (FileStatus status : statuses) {
      if (shouldReprocess(fs, status.getPath(), destinationModTime)) {
        return true;
      }
    }

    return false;
  }

  protected String pathListToCommaSeperated(List<Path> list) {
    ArrayList<Path> tmpList = new ArrayList<Path>();
    tmpList.addAll(list);

    StringBuilder sb = new StringBuilder(tmpList.get(0).toString());
    tmpList.remove(0);

    for (Path p : tmpList) {
      sb.append(",").append(p.toString());
    }

    return sb.toString();
  }

  public Set<Properties> getOutlierProperties() {
    return this.outlierProperties;
  }

  /**
   * Blocks processing of a job until the input is ready.
   * By default, will return immediately and proceed with the job.
   * @param jobProps Job properties.
   * @return true to proceed with the job, false to cancel the job.
   */
  protected boolean waitUntilReadyToProcess(Properties jobProps, FileSystem fs) {
    return true;
  }
}
