package com.linkedin.camus.sweeper;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

public abstract class CamusSweeperPlanner
{
  protected Properties props;
  protected Logger log;

  public CamusSweeperPlanner setPropertiesLogger(Properties props, Logger log)
  {
    this.props = props;
    this.log = log;
    return this;
  }
  
  public abstract List<Properties> createSweeperJobProps(String topic, Path inputDir, Path outputDir, FileSystem fs) throws IOException;
  
  // Simple check for reprocessing depending on the modified time of the source and destination
  // folder
  protected boolean shouldReprocess(FileSystem fs, Path source, Path dest) throws IOException
  {
    FileStatus sourceStatus = fs.getFileStatus(source);
    FileStatus destStatus = fs.getFileStatus(dest);

    long destinationModTime = destStatus.getModificationTime();
    if (sourceStatus.getModificationTime() > destinationModTime)
    {
      return true;
    }

    FileStatus[] statuses = fs.listStatus(source, new HiddenFilter());
    for (FileStatus status : statuses)
    {
      if (status.getModificationTime() > destinationModTime)
      {
        return true;
      }
    }

    return false;
  }
  
  private class HiddenFilter implements PathFilter
  {
    @Override
    public boolean accept(Path path)
    {
      String name = path.getName();
      if (name.startsWith("_") || name.startsWith("."))
      {
        return false;
      }
      
      return true;
    }
  }
}
