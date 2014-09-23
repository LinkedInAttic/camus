package com.linkedin.camus.sweeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.camus.sweeper.utils.DateUtils;

public class CamusSweeperDatePartitionPlanner extends CamusSweeperPlanner
{
  private static final Logger _log = Logger.getLogger(CamusSweeperDatePartitionPlanner.class);
  
  private DateTimeFormatter dayFormatter;
  private DateUtils dUtils;
  
  @Override
  public CamusSweeperPlanner setPropertiesLogger(Properties props, Logger log)
  {
    dUtils = new DateUtils(props);
    dayFormatter = dUtils.getDateTimeFormatter("YYYY/MM/dd");
    return super.setPropertiesLogger(props, log);
  }
  
  @Override
  public List<Properties> createSweeperJobProps(String topic, Path inputDir, Path outputDir, FileSystem fs) throws IOException
  {  
    int daysAgo = Integer.parseInt(props.getProperty("days.ago", "0"));
    int numDays = Integer.parseInt(props.getProperty("num.days", "15"));

    DateTime midnight = dUtils.getMidnight();
    DateTime startDate = midnight.minusDays(daysAgo);

    List<Properties> jobPropsList = new ArrayList<Properties>();
    for (int i = 0; i < numDays; ++i)
    {
      Properties jobProps = new Properties(props);
      jobProps.putAll(props);

      jobProps.put("topic", topic);
      
      DateTime currentDate = startDate.minusDays(i);
      String directory = dayFormatter.print(currentDate);

      Path sourcePath = new Path(inputDir, directory);
      if (!fs.exists(sourcePath))
      {
        _log.info("Source path: " + sourcePath + " does not exist.");
        continue;
      }

      List<Path> sourcePaths = new ArrayList<Path>();
      sourcePaths.add(sourcePath);

      Path destPath = new Path(outputDir, directory);

      jobProps.put("input.paths", sourcePath.toString());
      jobProps.put("dest.path", destPath.toString());
      
      if (!fs.exists(destPath))
      {
        _log.info(topic + " dest dir " + directory + " doesn't exist or . Processing.");
        jobPropsList.add(jobProps);
      }
      else if (shouldReprocess(fs, sourcePaths.get(0), destPath))
      {
        _log.info(topic + " dest dir " + directory + " has a modified time before the source. Reprocessing.");
        jobProps.put("input.paths", sourcePath.toString() + "," + destPath.toString());
        jobPropsList.add(jobProps);
      }
      else
      {
        _log.info(topic + " skipping " + directory);
      }
    }
    
    return jobPropsList;

  }
  
  

}
