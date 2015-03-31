package com.linkedin.camus.sweeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.linkedin.camus.sweeper.mapreduce.CamusSweeperJob;
import com.linkedin.camus.sweeper.utils.Utils;


public class CamusHourlySweeper extends CamusSweeper {

  private static final Logger LOG = Logger.getLogger(CamusHourlySweeper.class);

  private static final String REDUCE_COUNT_OVERRIDE = "reduce.count.override.";
  private static final String CAMUS_SWEEPER_TMP_DIR = "camus.sweeper.tmp.dir";
  private static final String MAX_FILES = "max.files";
  private static final int DEFAULT_MAX_FILES = 24;
  private static final String TMP_PATH = "tmp.path";
  private static final String CAMUS_SWEEPER_IO_CONFIGURER_CLASS = "camus.sweeper.io.configurer.class";
  private static final String MAPRED_COMPRESS_MAP_OUTPUT = "mapred.compress.map.output";
  private static final boolean DEFAULT_MAPRED_COMPRESS_MAP_OUTPUT = Boolean.TRUE;
  private static final String REDUCER_COUNT = "reducer.count";
  private static final int DEFAULT_REDUCER_COUNT = 45;
  private static final String MAPRED_MIN_SPLIT_SIZE = "mapred.min.split.size";
  private static final String MAPRED_MAX_SPLIT_SIZE = "mapred.max.split.size";

  static final String TOPIC_AND_HOUR = "topic.and.hour";
  static final String INPUT_PATHS = "input.paths";
  static final String DEST_PATH = "dest.path";

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

  @Override
  public void run() throws Exception {
    metrics.timeStart = System.currentTimeMillis();
    super.run();
    reportMetrics();
    addOutliers();
  }

  private void addOutliers() throws IOException {
    Configuration conf = new Configuration();
    for (Entry<Object, Object> pair : props.entrySet()) {
      String key = (String) pair.getKey();
      conf.set(key, (String) pair.getValue());
    }
    for (Properties jobProps : planner.getOutlierProperties()) {
      this.executorService.submit(new OutlierCollectorRunner(jobProps, FileSystem.get(conf)));
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

  @Override
  protected Future<?> runCollector(Properties props, String topic) {
    String jobName = topic + "-" + UUID.randomUUID().toString();
    props.put("tmp.path", props.getProperty(CAMUS_SWEEPER_TMP_DIR) + "/" + jobName + "_" + System.currentTimeMillis());

    if (props.containsKey(REDUCE_COUNT_OVERRIDE + topic))
      props.put("reducer.count", Integer.parseInt(props.getProperty(REDUCE_COUNT_OVERRIDE + topic)));

    LOG.info("Processing " + props.get(INPUT_PATHS));

    return executorService.submit(new KafkaCollectorRunner(jobName, props, errorMessages, topic));
  }

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
        e.printStackTrace();
        LOG.error("Failed for " + name + " ,job: " + collector == null ? null : collector.getJob() + " failed for "
            + props.getProperty(INPUT_PATHS) + " Exception:" + e.getLocalizedMessage());
        errorQueue.add(new SweeperError(name, props.get(INPUT_PATHS).toString(), e));
      }
    }
  }

  private class KafkaCollector extends CamusSweeper.KafkaCollector {

    private final String topicAndHour;
    private final Path[] inputPaths;
    private final Path tmpPath;
    private final Path outputPath;
    private final FileSystem fs;

    public KafkaCollector(Properties props, String jobName, String topicName) throws IOException {
      super(props, jobName, topicName);
      this.topicAndHour = props.getProperty(TOPIC_AND_HOUR);
      this.fs = FileSystem.get(job.getConfiguration());
      this.inputPaths = getInputPaths();
      this.tmpPath = new Path(job.getConfiguration().get(TMP_PATH));
      this.outputPath = new Path(job.getConfiguration().get(DEST_PATH));
      addInputAndOutputPathsToFileInputFormat();
    }

    private Path[] getInputPaths() {
      List<String> strPaths = Utils.getStringList(props, INPUT_PATHS);
      Path[] inputPaths = new Path[strPaths.size()];
      for (int i = 0; i < strPaths.size(); i++)
        inputPaths[i] = new Path(strPaths.get(i));
      return inputPaths;
    }

    @Override
    public void run() throws Exception {
      CamusHourlySweeper.this.metrics.runnerStartTimeByTopic.put(this.topicAndHour, System.currentTimeMillis());

      job.getConfiguration().setBoolean(MAPRED_COMPRESS_MAP_OUTPUT, DEFAULT_MAPRED_COMPRESS_MAP_OUTPUT);

      ((CamusSweeperJob) Class.forName(props.getProperty(CAMUS_SWEEPER_IO_CONFIGURER_CLASS)).newInstance()).setLogger(
          LOG).configureJob(topicName, job);

      setNumOfReducersAndSplitSizes();
      submitMrJob();
      moveTmpPathToOutputPath();

      CamusHourlySweeper.this.metrics.mrFinishTimeByTopic.put(this.topicAndHour, System.currentTimeMillis());
    }

    private void moveTmpPathToOutputPath() throws IOException {
      Path oldPath = null;
      if (fs.exists(outputPath)) {
        oldPath = new Path("/tmp", "_old_" + job.getJobID());
        moveExistingContentInOutputPathToOldPath(oldPath);
      }

      LOG.info("Moving " + tmpPath + " to " + outputPath);
      mkdirs(fs, outputPath.getParent(), perm);

      if (!fs.rename(tmpPath, outputPath)) {
        fs.rename(oldPath, outputPath);
        fs.delete(tmpPath, true);
        throw new RuntimeException("Error: cannot rename " + tmpPath + " to " + outputPath);
      }
      deleteOldPath(oldPath);
    }

    private void deleteOldPath(Path oldPath) throws IOException {
      if (oldPath != null && fs.exists(oldPath)) {
        LOG.info("Deleting " + oldPath);
        fs.delete(oldPath, true);
      }
    }

    private void moveExistingContentInOutputPathToOldPath(Path oldPath) throws IOException {
      LOG.info("Path " + outputPath + " exists. Overwriting.");
      if (!fs.rename(outputPath, oldPath)) {
        fs.delete(tmpPath, true);
        throw new RuntimeException("Error: cannot rename " + outputPath + " to " + oldPath);
      }
    }

    private void setNumOfReducersAndSplitSizes() throws IOException {
      long inputSize = getInputSize();

      int maxFiles = job.getConfiguration().getInt(MAX_FILES, DEFAULT_MAX_FILES);
      int numTasks = Math.min((int) (inputSize / targetFileSize) + 1, maxFiles);

      if (job.getNumReduceTasks() != 0) {
        determineAndSetNumOfReducers(numTasks);
      } else {
        setSplitSizes(inputSize / numTasks);
      }
    }

    private void submitMrJob() throws IOException, InterruptedException, ClassNotFoundException {
      CamusHourlySweeper.this.metrics.mrSubmitTimeByTopic.put(this.topicAndHour, System.currentTimeMillis());
      job.submit();
      runningJobs.add(job);

      CamusHourlySweeper.this.metrics.mrStartRunningTimeByTopic.put(this.topicAndHour, System.currentTimeMillis());

      LOG.info("job running for: " + job.getConfiguration().get(TOPIC_AND_HOUR) + ", url: " + job.getTrackingURL());
      job.waitForCompletion(false);
      if (!job.isSuccessful()) {
        throw new RuntimeException("hadoop job failed.");
      }
    }

    private void setSplitSizes(long targetSplitSize) {
      LOG.info("Setting target split size " + targetSplitSize);
      job.getConfiguration().setLong(MAPRED_MAX_SPLIT_SIZE, targetSplitSize);
      job.getConfiguration().setLong(MAPRED_MIN_SPLIT_SIZE, targetSplitSize);
    }

    private void determineAndSetNumOfReducers(int numTasks) {
      int numReducers;
      if (job.getConfiguration().get(REDUCER_COUNT) != null) {
        numReducers = job.getConfiguration().getInt(REDUCER_COUNT, DEFAULT_REDUCER_COUNT);
      } else {
        numReducers = numTasks;
      }
      job.setNumReduceTasks(numReducers);
    }

    private long getInputSize() throws IOException {
      long inputSize = 0;
      for (Path p : inputPaths) {
        LOG.info("inputPath: " + p.toString() + ", size=" + fs.getContentSummary(p).getLength());
        inputSize += fs.getContentSummary(p).getLength();
      }
      return inputSize;
    }

    private void addInputAndOutputPathsToFileInputFormat() throws IOException {
      for (Path path : inputPaths) {
        FileInputFormat.addInputPath(job, path);
        FileOutputFormat.setOutputPath(job, tmpPath);
      }
    }
  }

  private class OutlierCollectorRunner implements Runnable {

    private final Properties props;
    private final FileSystem fs;

    public OutlierCollectorRunner(Properties props, FileSystem fs) {
      this.props = props;
      this.fs = fs;
    }

    @Override
    public void run() {
      String inputPaths = this.props.getProperty(CamusHourlySweeper.INPUT_PATHS);
      String outputPathStr = this.props.getProperty(CamusHourlySweeper.DEST_PATH);
      Path outputPath = new Path(outputPathStr, "outlier");

      try {
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
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
