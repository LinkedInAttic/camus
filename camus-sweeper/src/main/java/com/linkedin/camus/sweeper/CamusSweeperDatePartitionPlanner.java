package com.linkedin.camus.sweeper;

import java.io.IOException;
import java.util.ArrayList;
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
  private static final Logger LOG = Logger.getLogger(CamusSweeperDatePartitionPlanner.class);

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

  @Override
  public List<Properties> createSweeperJobProps(String topic, Path inputDir, Path outputDir, FileSystem fs)
      throws IOException {
    DateTime midnight = dUtils.getMidnight();
    DateTime minDaysAgo = midnight.minusDays(Integer.parseInt(props.getProperty("camus.sweeper.min.days.ago", "1")));
    DateTime maxDaysAgo = midnight.minusDays(Integer.parseInt(props.getProperty("camus.sweeper.max.days.ago", "2")));

    List<Properties> jobPropsList = new ArrayList<Properties>();

    for (FileStatus f : fs.globStatus(new Path(inputDir, "*/*/*"))) {
      Properties jobProps = new Properties(props);
      jobProps.putAll(props);

      jobProps.put("topic", topic);

      DateTime inputDate = getDateFromPath(f.getPath(), inputDir);

      if (shouldProcessDir(f.getPath(), inputDate, minDaysAgo, maxDaysAgo)) {
        List<Path> sourcePaths = new ArrayList<Path>();
        sourcePaths.add(f.getPath());

        Path destPath = new Path(outputDir, inputDate.toString(dayFormatter));

        jobProps.put("input.paths", pathListToCommaSeperated(sourcePaths));
        jobProps.put("dest.path", destPath.toString());

        if (!fs.exists(destPath)) {
          LOG.info(topic + " dest dir " + destPath.toString() + " doesn't exist or . Processing.");
          jobPropsList.add(jobProps);
        } else if (shouldReprocess(fs, sourcePaths, destPath)) {
          LOG.info(
              topic + " dest dir " + destPath.toString() + " has a modified time before the source. Reprocessing.");
          sourcePaths.add(destPath);
          jobProps.put("input.paths", pathListToCommaSeperated(sourcePaths));
          jobPropsList.add(jobProps);
        } else {
          LOG.info(topic + " skipping " + destPath.toString());
        }
      }
    }

    return jobPropsList;

  }

  private boolean shouldProcessDir(Path inputDir, DateTime inputDate, DateTime minDaysAgo, DateTime maxDaysAgo) {
    if (inputDate.isAfter(minDaysAgo)) {
      LOG.info(String.format("folder %s with timestamp %s is later than latest allowed timestamp %s. Skipping",
          inputDir, inputDate, minDaysAgo));
      return false;
    }
    if (inputDate.isBefore(maxDaysAgo)) {
      LOG.info(String.format("folder %s with timestamp %s is earlier than earliest allowed timestamp %s. Skipping",
          inputDir, inputDate, maxDaysAgo));
      return false;
    }
    return true;
  }

  @Override
  protected List<Properties> createSweeperJobProps(String topic, Path inputDir, Path outputDir, FileSystem fs,
      CamusSweeperMetrics metrics) throws IOException {
    return createSweeperJobProps(topic, inputDir, outputDir, fs);
  }

}
