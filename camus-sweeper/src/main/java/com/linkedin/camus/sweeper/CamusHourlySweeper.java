package com.linkedin.camus.sweeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.linkedin.camus.sweeper.mapreduce.CamusSweeperJob;


public class CamusHourlySweeper extends CamusSweeper {

  private static final Logger LOG = Logger.getLogger(CamusHourlySweeper.class);

  private static final String CAMUS_SWEEPER_IO_CONFIGURER_CLASS = "camus.sweeper.io.configurer.class";
  private static final String MAPRED_COMPRESS_MAP_OUTPUT = "mapred.compress.map.output";
  private static final boolean DEFAULT_MAPRED_COMPRESS_MAP_OUTPUT = Boolean.TRUE;

  static final String TOPIC_AND_HOUR = "topic.and.hour";

  private final CamusSweeperMetrics metrics;

  public CamusHourlySweeper() {
    super();
    metrics = new CamusSweeperMetrics();
  }

  public CamusHourlySweeper(Properties props) {
    super(props);
    metrics = new CamusSweeperMetrics();
  }

  @Override
  public Map<FileStatus, String> findAllTopics(Path input, PathFilter filter, String topicSubdir, FileSystem fs)
      throws IOException {
    Map<FileStatus, String> topics = new HashMap<FileStatus, String>();
    for (FileStatus f : fs.listStatus(input)) {
      if (f.isDir() && filter.accept(f.getPath())) {
        LOG.info("found topic: " + f.getPath().getName());
        topics.put(fs.getFileStatus(f.getPath()), f.getPath().getName());
      }
    }
    return topics;
  }

  /**
   * Run the hourly compaction job. It largely reuses the daily compaction code by calling super.run().
   * For each hourly folder that needs to be deduped, it will create a KafkaCollector, and submit it to the thread pool.
   * After compaction, report job metrics, and add outliers to the outlier folder.
   */
  @Override
  public void run() throws Exception {
    metrics.setTimeStart(System.currentTimeMillis());
    super.run();
    reportMetrics();
    addOutliers();
  }

  private void addOutliers() throws IOException {
    createExecutorService();
    List<Future<?>> futures = new ArrayList<Future<?>>();
    for (Properties jobProps : planner.getOutlierProperties()) {
      futures.add(this.executorService.submit(new OutlierCollectorRunner(jobProps, FileSystem.get(getConf()))));
    }
    this.executorService.shutdown();
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void reportMetrics() throws IllegalArgumentException, IOException {
    LOG.info("reporting metrics");
    metrics.reportTotalRunningTime();
    metrics.reportTotalDataSize();
    metrics.reportDataSizeByTopic();
    metrics.reportDurationFromStartToRunnerStart();
    metrics.reportDurationFromRunnerStartToMRSubmitted();
    metrics.reportDurationFromMRSubmittedToMRStarted();
    metrics.reportDurationFromMRStartedToMRFinished();
    LOG.info("finished reporting metrics");
  }

  public static void main(String args[]) throws Exception {
    CamusSweeper job = new CamusHourlySweeper();
    ToolRunner.run(job, args);
  }

  /**
   * This method creates collectors of a topic.
   * A topic may have multiple collectors, if this topic has multiple hourly folders that need to be deduped.
   * After creating the collectors, it passes them to runCollector() to submit them to thread pool.
   */
  @Override
  protected void runCollectorForTopicDir(FileSystem fs, String topic, Path topicSourceDir, Path topicDestDir)
      throws Exception {
    LOG.info("Running collector for topic " + topic + " source:" + topicSourceDir + " dest:" + topicDestDir);
    List<Future<?>> tasksToComplete = new ArrayList<Future<?>>();

    List<Properties> jobPropsList = planner.createSweeperJobProps(topic, topicSourceDir, topicDestDir, fs, metrics);

    for (Properties jobProps : jobPropsList) {
      tasksToComplete.add(runCollector(jobProps, topic));
    }
  }

  /**
   * This method runs a collector of a topic
   * A topic may have multiple collectors, if this topic has multiple hourly folders that need to be deduped.
   */
  @Override
  protected Future<?> runCollector(Properties props, String topic) {
    String jobName = topic + "-" + UUID.randomUUID().toString();
    props
        .put("tmp.path", props.getProperty("camus.sweeper.tmp.dir") + "/" + jobName + "_" + System.currentTimeMillis());

    if (props.containsKey("reduce.count.override." + topic))
      props.put("reducer.count", Integer.parseInt(props.getProperty("reduce.count.override." + topic)));

    LOG.info("Processing " + props.get("input.paths"));

    return executorService.submit(new KafkaCollectorRunner(jobName, props, errorMessages, topic));
  }

  private class KafkaCollectorRunner extends CamusSweeper.KafkaCollectorRunner {

    public KafkaCollectorRunner(String name, Properties props, List<SweeperError> errorQueue, String topic) {
      super(name, props, errorQueue, topic);
    }

    @Override
    public void run() {
      KafkaCollector collector = null;
      try {
        LOG.info("Starting runner for " + this.props.getProperty(TOPIC_AND_HOUR));
        collector = new KafkaCollector(props, name, topic);
        LOG.info("Running " + name + " for input " + props.getProperty(INPUT_PATHS));
        collector.run();
      } catch (Throwable e) { // Sometimes the error is the Throwable, e.g. java.lang.NoClassDefFoundError
        LOG.error("Failed for " + name + " ,job: " + collector == null ? null : collector.getJob() + " failed for "
            + props.getProperty(INPUT_PATHS) + " Exception:" + e.getLocalizedMessage());
        errorQueue.add(new SweeperError(name, props.get(INPUT_PATHS).toString(), e));
      }
    }
  }

  private class KafkaCollector extends CamusSweeper.KafkaCollector {

    private final String topicAndHour;

    public KafkaCollector(Properties props, String jobName, String topicName) throws IOException {
      super(props, jobName, topicName);
      this.topicAndHour = props.getProperty(TOPIC_AND_HOUR);

    }

    /**
     * Runs a collector of a topic. It launches an MR job to dedup an hourly folder of the topic.
     */
    @Override
    public void run() throws Exception {
      CamusHourlySweeper.this.metrics.recordRunnerStartTimeByTopic(this.topicAndHour, System.currentTimeMillis());

      job.getConfiguration().setBoolean(MAPRED_COMPRESS_MAP_OUTPUT, DEFAULT_MAPRED_COMPRESS_MAP_OUTPUT);

      ((CamusSweeperJob) Class.forName(props.getProperty(CAMUS_SWEEPER_IO_CONFIGURER_CLASS)).newInstance()).setLogger(
          LOG).configureJob(topicName, job);

      setNumOfReducersAndSplitSizes();
      submitMrJob();
      moveTmpPathToOutputPath();
    }

    @Override
    protected void submitMrJob() throws IOException, InterruptedException, ClassNotFoundException {
      CamusHourlySweeper.this.metrics.recordMrSubmitTimeByTopic(this.topicAndHour, System.currentTimeMillis());
      job.submit();
      runningJobs.add(job);

      CamusHourlySweeper.this.metrics.recordMrStartRunningTimeByTopic(this.topicAndHour, System.currentTimeMillis());

      LOG.info("job running for: " + props.getProperty(TOPIC_AND_HOUR) + ", url: " + job.getTrackingURL());
      job.waitForCompletion(false);
      CamusHourlySweeper.this.metrics.recordMrFinishTimeByTopic(this.topicAndHour, System.currentTimeMillis());
      if (!job.isSuccessful()) {
        throw new RuntimeException("hadoop job failed.");
      }
    }
  }

  private class OutlierCollectorRunner implements Callable<Void> {

    private final Properties props;
    private final FileSystem fs;

    public OutlierCollectorRunner(Properties props, FileSystem fs) {
      this.props = props;
      this.fs = fs;
    }

    /**
     * Collect outliers for an hourly folder, and put it in the outlier folder.
     */
    @Override
    public Void call() throws IOException {
      String inputPaths = this.props.getProperty(CamusHourlySweeper.INPUT_PATHS);
      String outputPathStr = this.props.getProperty(CamusHourlySweeper.DEST_PATH);
      Path outputPath = new Path(outputPathStr, "outlier");
      fs.mkdirs(outputPath);
      long destinationModTime = fs.getFileStatus(new Path(outputPathStr)).getModificationTime();

      for (String inputPathStr : inputPaths.split(",")) {
        Path inputPath = new Path(inputPathStr);
        for (FileStatus status : fs.globStatus(new Path(inputPath, "*"), new HiddenFilter())) {
          if (status.getModificationTime() > destinationModTime) {
            LOG.info("moving " + status.getPath() + " to " + outputPath);
            fs.rename(status.getPath(), new Path(outputPath, status.getPath().getName()));
          }
        }
      }
      return null;
    }
  }
}
