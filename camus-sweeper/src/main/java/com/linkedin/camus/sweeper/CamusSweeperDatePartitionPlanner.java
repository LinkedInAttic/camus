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
    dayFormatter = dUtils.getDateTimeFormatter("YYYY/MM/dd/HH");
    return super.setPropertiesLogger(props, log);
  }

  @Override
  public List<Properties> createSweeperJobProps(String topic, Path inputDir, Path outputDir, FileSystem fs) throws IOException
  {
    int daysAgo = Integer.parseInt(props.getProperty("days.ago", "0"));
    int numDays = Integer.parseInt(props.getProperty("num.days", "5"));

    DateTime midnight = dUtils.getMidnight();
    DateTime startDate = midnight.minusDays(daysAgo);

    List<Properties> jobPropsList = new ArrayList<Properties>();
    for (int i = 0; i < numDays; ++i)
    {

      for(int j = 0; j < 24; ++j){

        if (dUtils.getCurrentTime().getHourOfDay() == j ){
          _log.info("Skipping hour: " + String.valueOf(j));
          continue;
        }

        Properties jobProps = new Properties(props);
        jobProps.putAll(props);

        jobProps.put("topic", topic);

        DateTime currentDate = startDate.minusDays(i).plusHours(j);

        String directory = dayFormatter.print(currentDate);
        Path sourcePath = new Path(inputDir, directory);
        if (!fs.exists(sourcePath))
        {
          continue;
        }

        List<Path> sourcePaths = new ArrayList<Path>();
        sourcePaths.add(sourcePath);

        String[] parts = directory.split("/");
        String output = "year=" + parts[0] + "/";
        output += "month=" + parts[1] + "/";
        output += "day=" + parts[2] + "/";
        output += "hour=" + parts[3] + "/";

        Path destPath = new Path(outputDir, output);

        jobProps.put("input.paths", sourcePath.toString());
        jobProps.put("dest.path", destPath.toString());

        if (!fs.exists(destPath))
        {
          _log.info(topic + " dest dir " + directory + " doesn't exist or . Processing.");
          jobPropsList.add(jobProps);
        }
        else
        {
          _log.info(topic + " dest dir " + directory + " exists. Reprocessing.");
          jobProps.put("input.paths", sourcePath.toString() + "," + destPath.toString());
          jobPropsList.add(jobProps);
        }
      }
    }
    return jobPropsList;
  }
}
