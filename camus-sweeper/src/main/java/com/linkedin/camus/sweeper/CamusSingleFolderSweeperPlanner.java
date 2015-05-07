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


public class CamusSingleFolderSweeperPlanner extends CamusSweeperPlanner {

  private static final String CAMUS_SINGLE_FOLDER_SWEEPER_TIMEBASED = "camus.single.folder.sweeper.timebased";
  private static final String DEFAULT_CAMUS_SINGLE_FOLDER_SWEEPER_TIMEBASED = Boolean.TRUE.toString();
  private static final String CAMUS_SINGLE_FOLDER_SWEEPER_FOLDER_STRUCTURE =
      "camus.single.folder.sweeper.folder.structure";
  private static final String DEFAULT_CAMUS_SINGLE_FOLDER_SWEEPER_DEFAULT_FOLDER_STRUCTURE = "*/*/*/*";
  private static final String CAMUS_SINGLE_FOLDER_SWEEPER_TIME_FORMAT = "camus.single.folder.sweeper.time.format";
  private static final String DEFAULT_CAMUS_SINGLE_FOLDER_SWEEPER_TIME_FORMAT = "YYYY/MM/dd/HH";
  private static final String CAMUS_SINGLE_FOLDER_SWEEPER_MAX_HOURS_AGO = "camus.single.folder.sweeper.max.hours.ago";
  private static final String DEFAULT_CAMUS_SINGLE_FOLDER_SWEEPER_MAX_HOURS_AGO = "1";
  private static final String CAMUS_SINGLE_FOLDER_SWEEPER_MIN_HOURS_AGO = "camus.single.folder.sweeper.min.hours.ago";
  private static final String DEFAULT_CAMUS_SINGLE_FOLDER_SWEEPER_MIN_HOURS_AGO = "1";

  private static final Logger LOG = Logger.getLogger(CamusSingleFolderSweeperPlanner.class);

  private DateTimeFormatter timeFormatter;
  private DateUtils dUtils;

  @Override
  public CamusSweeperPlanner setPropertiesLogger(Properties props, Logger log) {
    dUtils = new DateUtils(props);
    timeFormatter =
        dUtils.getDateTimeFormatter(props.getProperty(CAMUS_SINGLE_FOLDER_SWEEPER_TIME_FORMAT,
            DEFAULT_CAMUS_SINGLE_FOLDER_SWEEPER_TIME_FORMAT));
    return super.setPropertiesLogger(props, log);
  }

  private DateTime getFolderHour(Path datePath, Path inputDir) {
    String datePathStr = datePath.toString();
    String inputDirStr = inputDir.toString();
    String dateStr = datePathStr.substring(datePathStr.indexOf(inputDirStr) + inputDirStr.length());
    return timeFormatter.parseDateTime(dateStr.replaceAll("^/", ""));
  }

  @Override
  public List<Properties> createSweeperJobProps(String topic, Path inputDir, Path outputDir, FileSystem fs)
      throws IOException {
    return createSweeperJobProps(topic, inputDir, outputDir, fs, new CamusSweeperMetrics());
  }

  /**
   * Create hourly compaction properties for a topic.
   * If a topic has multiple hourly folders that need to be deduped, there will be multiple jobs for this topic.
   */
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

    if (Boolean.valueOf(this.props.getProperty(CAMUS_SINGLE_FOLDER_SWEEPER_TIMEBASED,
        DEFAULT_CAMUS_SINGLE_FOLDER_SWEEPER_TIMEBASED))) {

      // Timebased sweeper. Each input folder is inputDir + folderStructure
      LOG.info("Time-based sweeper");
      String folderStructure =
          props.getProperty(CAMUS_SINGLE_FOLDER_SWEEPER_FOLDER_STRUCTURE,
              DEFAULT_CAMUS_SINGLE_FOLDER_SWEEPER_DEFAULT_FOLDER_STRUCTURE);
      LOG.info("Sweeper folder structure: " + folderStructure);

      for (FileStatus f : fs.globStatus(new Path(inputDir, folderStructure))) {

        DateTime folderHour = getFolderHour(f.getPath(), inputDir);
        if (shouldProcessHour(folderHour, topic)) {
          Properties jobProps = createJobProps(topic, f.getPath(), folderHour, outputDir, fs, metrics);
          if (jobProps != null) {
            jobPropsList.add(jobProps);
          }
        }
      }
    } else {

      // Non-timebased sweeper. Each input folder is simply inputDir
      LOG.info("Non-time-based sweeper");
      Properties jobProps = createJobProps(topic, inputDir, null, outputDir, fs, metrics);
      if (jobProps != null) {
        jobPropsList.add(jobProps);
      }
    }

    return jobPropsList;
  }

  private Properties createJobProps(String topic, Path folder, DateTime folderHour, Path outputDir, FileSystem fs,
      CamusSweeperMetrics metrics) throws IOException {

    Properties jobProps = new Properties();
    jobProps.putAll(props);

    jobProps.put("topic", topic);
    if (folderHour != null) {
      jobProps.setProperty(CamusSingleFolderSweeper.FOLDER_HOUR, Long.toString(folderHour.getMillis()));
    }

    String topicAndHour = topic + ":" + (folderHour != null ? folderHour.toString(timeFormatter) : "");
    jobProps.put(CamusSingleFolderSweeper.TOPIC_AND_HOUR, topicAndHour);

    long dataSize = fs.getContentSummary(folder).getLength();
    metrics.recordDataSizeByTopic(topicAndHour, dataSize);
    metrics.addToTotalDataSize(dataSize);

    List<Path> sourcePaths = new ArrayList<Path>();
    sourcePaths.add(folder);

    Path destPath = (folderHour != null ? new Path(outputDir, folderHour.toString(timeFormatter)) : outputDir);

    jobProps.put(CamusSingleFolderSweeper.INPUT_PATHS, pathListToCommaSeperated(sourcePaths));
    jobProps.put(CamusSingleFolderSweeper.DEST_PATH, destPath.toString());

    if (!fs.exists(destPath)) {
      LOG.info(topic + " dest dir " + destPath.toString() + " doesn't exist. Processing.");
      return jobProps;
    } else if (forceReprocess()) {
      LOG.info(topic + " dest dir " + destPath.toString() + " exists, but force reprocess set to true. Reprocessing.");
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

  private boolean forceReprocess() {
    return Boolean.valueOf(this.props.getProperty("camus.sweeper.always.reprocess", Boolean.FALSE.toString()));
  }

  private boolean sourceDirHasOutliers(FileSystem fs, List<Path> sourcePaths, Path destPath) throws IOException {
    long destinationModTime = CamusSingleFolderSweeper.getDestinationModTime(fs, destPath.toString());
    for (Path source : sourcePaths) {
      for (FileStatus status : fs.globStatus(new Path(source, "*"), new HiddenFilter())) {
        if (status.getModificationTime() > destinationModTime) {
          return true;
        }
      }
    }
    return false;
  }

  protected boolean shouldProcessHour(DateTime folderHour, String topic) {
    DateTime currentHour = dUtils.getCurrentHour();
    DateTime maxHoursAgo =
        currentHour.minusHours(Integer.parseInt(props.getProperty(CAMUS_SINGLE_FOLDER_SWEEPER_MAX_HOURS_AGO,
            DEFAULT_CAMUS_SINGLE_FOLDER_SWEEPER_MAX_HOURS_AGO)));
    DateTime minHoursAgo =
        currentHour.minusHours(Integer.parseInt(props.getProperty(CAMUS_SINGLE_FOLDER_SWEEPER_MIN_HOURS_AGO,
            DEFAULT_CAMUS_SINGLE_FOLDER_SWEEPER_MIN_HOURS_AGO)));
    return (folderHour.isAfter(maxHoursAgo) || folderHour.isEqual(maxHoursAgo))
        && (folderHour.isBefore(minHoursAgo) || folderHour.isEqual(minHoursAgo));
  }
}
