package com.linkedin.camus.sweeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.camus.sweeper.utils.DateUtils;


public class CamusSweeperDatePartitionPlanner extends CamusSweeperPlanner {
  private static final Logger _log = Logger.getLogger(CamusSweeperDatePartitionPlanner.class);

  private DateTimeFormatter dayFormatter;
  private DateUtils dUtils;

  @Override
  public CamusSweeperPlanner setPropertiesLogger(Properties props, Logger log) {
    dUtils = new DateUtils(props);
    dayFormatter = dUtils.getDateTimeFormatter("YYYY/MM/dd");
    return super.setPropertiesLogger(props, log);
  }

  private DateTime getDateFromPath(Path datePath, Path inputDir) {
    String datePathStr = datePath.toString();
    String inputDirStr = inputDir.toString();
    String dateStr = datePathStr.substring(datePathStr.indexOf(inputDirStr) + inputDirStr.length());
    return dayFormatter.parseDateTime(dateStr.replaceAll("^/", ""));
  }

  private String pathListToCommaSeperated(List<Path> list) {
    ArrayList<Path> tmpList = new ArrayList<Path>();
    tmpList.addAll(list);

    StringBuilder sb = new StringBuilder(tmpList.get(0).toString());
    tmpList.remove(0);

    for (Path p : tmpList) {
      sb.append(",").append(p.toString());
    }

    return sb.toString();
  }

  @Override
  public List<Properties> createSweeperJobProps(String topic, Path inputDir, Path outputDir, FileSystem fs)
      throws IOException {
    DateTime midnight = dUtils.getMidnight();
    DateTime daysAgo = midnight.minusDays(Integer.parseInt(props.getProperty("camus.sweeper.days.ago", "1")));

    List<Properties> jobPropsList = new ArrayList<Properties>();

    for (FileStatus f : fs.globStatus(new Path(inputDir, "*/*/*"))) {
      Properties jobProps = new Properties(props);
      jobProps.putAll(props);

      jobProps.put("topic", topic);

      DateTime inputDate = getDateFromPath(f.getPath(), inputDir);

      if (inputDate.isBefore(daysAgo) || inputDate.isEqual(daysAgo)) {
        List<Path> sourcePaths = new ArrayList<Path>();
        sourcePaths.add(f.getPath());

        Path destPath = new Path(outputDir, inputDate.toString(dayFormatter));

        jobProps.put("input.paths", pathListToCommaSeperated(sourcePaths));
        jobProps.put("dest.path", destPath.toString());

        if (!fs.exists(destPath)) {
          _log.info(topic + " dest dir " + destPath.toString() + " doesn't exist or . Processing.");
          jobPropsList.add(jobProps);
        } else if (shouldReprocess(fs, sourcePaths, destPath)) {
          _log.info(topic + " dest dir " + destPath.toString()
              + " has a modified time before the source. Reprocessing.");
          sourcePaths.add(destPath);
          jobProps.put("input.paths", pathListToCommaSeperated(sourcePaths));
          jobPropsList.add(jobProps);
        } else {
          _log.info(topic + " skipping " + destPath.toString());
        }
      }
    }

    return jobPropsList;

  }

}
