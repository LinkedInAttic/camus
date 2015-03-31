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


public class CamusHourlySweeperPlanner extends CamusSweeperPlanner {
  private static final String CAMUS_HOURLY_SWEEPER_MAX_HOURS_AGO = "camus.hourly.sweeper.max.hours.ago";
  private static final String DEFAULT_CAMUS_HOURLY_SWEEPER_MAX_HOURS_AGO = "1";
  private static final String CAMUS_HOURLY_SWEEPER_MIN_HOURS_AGO = "camus.hourly.sweeper.min.hours.ago";
  private static final String DEFAULT_CAMUS_HOURLY_SWEEPER_MIN_HOURS_AGO = "1";

  private static final Logger LOG = Logger.getLogger(CamusHourlySweeperPlanner.class);

  private DateTimeFormatter hourFormatter;
  private DateUtils dUtils;

  @Override
  public CamusSweeperPlanner setPropertiesLogger(Properties props, Logger log) {
    dUtils = new DateUtils(props);
    hourFormatter = dUtils.getDateTimeFormatter("YYYY/MM/dd/HH");
    return super.setPropertiesLogger(props, log);
  }

  private DateTime getFolderHour(Path datePath, Path inputDir) {
    String datePathStr = datePath.toString();
    String inputDirStr = inputDir.toString();
    String dateStr = datePathStr.substring(datePathStr.indexOf(inputDirStr) + inputDirStr.length());
    return hourFormatter.parseDateTime(dateStr.replaceAll("^/", ""));
  }

  @Override
  public List<Properties> createSweeperJobProps(String topic, Path inputDir, Path outputDir, FileSystem fs)
      throws IOException {
    return createSweeperJobProps(topic, inputDir, outputDir, fs, new CamusSweeperMetrics());
  }

  @Override
  protected List<Properties> createSweeperJobProps(String topic, Path inputDir, Path outputDir, FileSystem fs,
      CamusSweeperMetrics metrics) throws IOException {
    LOG.info("creating hourly sweeper job props: topic=" + topic + ", inputDir=" + inputDir + ", outputDir="
        + outputDir);
    List<Properties> jobPropsList = new ArrayList<Properties>();
    if (!fs.exists(inputDir)) {
      LOG.warn("inputdir " + inputDir + " does not exist. Skipping topic " + topic);
      return jobPropsList;
    }

    for (FileStatus f : fs.globStatus(new Path(inputDir, "*/*/*/*"))) {

      DateTime folderHour = getFolderHour(f.getPath(), inputDir);
      if (shouldProcessHour(folderHour)) {
        Properties jobProps = createJobProps(topic, f.getPath(), folderHour, outputDir, fs, metrics);
        if (jobProps != null) {
          jobPropsList.add(jobProps);
        }
      }
    }

    return jobPropsList;
  }

  private Properties createJobProps(String topic, Path folder, DateTime folderHour, Path outputDir, FileSystem fs,
      CamusSweeperMetrics metrics) throws IOException {

    Properties jobProps = new Properties(props);
    jobProps.putAll(props);

    jobProps.put("topic", topic);

    String topicAndHour = topic + ":" + folderHour.toString(hourFormatter);
    jobProps.put(CamusHourlySweeper.TOPIC_AND_HOUR, topicAndHour);

    long dataSize = fs.getContentSummary(folder).getLength();
    metrics.dataSizeByTopic.put(topicAndHour, dataSize);
    metrics.totalDataSize += dataSize;

    List<Path> sourcePaths = new ArrayList<Path>();
    sourcePaths.add(folder);

    Path destPath = new Path(outputDir, folderHour.toString(hourFormatter));

    jobProps.put(CamusHourlySweeper.INPUT_PATHS, pathListToCommaSeperated(sourcePaths));
    jobProps.put(CamusHourlySweeper.DEST_PATH, destPath.toString());

    if (!fs.exists(destPath)) {
      LOG.info(topic + " dest dir " + destPath.toString() + " doesn't exist. Processing.");
      return jobProps;
    } else if (sourceDirHasOutliers(fs, sourcePaths, destPath)) {
      LOG.info("found outliers for topic " + topic + ". Will add outliers to " + destPath.toString());
      this.outlierProperties.add(jobProps);
      return null;
    } else {
      LOG.info(topic + " dest dir " + destPath.toString() + " already exists. Skipping.");
      return null;
    }
  }

  private boolean sourceDirHasOutliers(FileSystem fs, List<Path> sourcePaths, Path destPath) throws IOException {
    long destinationModTime = fs.getFileStatus(destPath).getModificationTime();
    for (Path source : sourcePaths) {
      for (FileStatus status : fs.globStatus(new Path(source, "*"), new HiddenFilter())) {
        if (status.getModificationTime() > destinationModTime) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean shouldProcessHour(DateTime folderHour) {
    DateTime currentHour = dUtils.getCurrentHour();
    DateTime maxHoursAgo =
        currentHour.minusHours(Integer.parseInt(props.getProperty(CAMUS_HOURLY_SWEEPER_MAX_HOURS_AGO,
            DEFAULT_CAMUS_HOURLY_SWEEPER_MAX_HOURS_AGO)));
    DateTime minHoursAgo =
        currentHour.minusHours(Integer.parseInt(props.getProperty(CAMUS_HOURLY_SWEEPER_MIN_HOURS_AGO,
            DEFAULT_CAMUS_HOURLY_SWEEPER_MIN_HOURS_AGO)));
    return (folderHour.isAfter(maxHoursAgo) || folderHour.isEqual(maxHoursAgo))
        && (folderHour.isBefore(minHoursAgo) || folderHour.isEqual(minHoursAgo));
  }
}
