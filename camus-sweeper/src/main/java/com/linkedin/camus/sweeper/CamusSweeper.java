package com.linkedin.camus.sweeper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.mortbay.log.Log;

import com.linkedin.camus.sweeper.mapreduce.CamusSweeperJob;
import com.linkedin.camus.sweeper.utils.PriorityExecutor;
import com.linkedin.camus.sweeper.utils.PriorityExecutor.Important;
import com.linkedin.camus.sweeper.utils.Utils;


public class CamusSweeper extends Configured implements Tool {
  protected static final String DEFAULT_NUM_THREADS = "5";
  protected static final String CAMUS_SWEEPER_PRIORITY_LIST = "camus.sweeper.priority.list";
  private static final String MAX_FILES = "max.files";
  private static final int DEFAULT_MAX_FILES = 24;
  private static final String REDUCER_COUNT = "reducer.count";
  private static final int DEFAULT_REDUCER_COUNT = 45;
  private static final String MAPRED_MIN_SPLIT_SIZE = "mapred.min.split.size";
  private static final String MAPRED_MAX_SPLIT_SIZE = "mapred.max.split.size";
  private static final String TMP_PATH = "tmp.path";
  static final String INPUT_PATHS = "input.paths";
  static final String DEST_PATH = "dest.path";

  protected List<SweeperError> errorMessages;
  protected List<Job> runningJobs;

  protected Properties props;
  protected FileSystem fileSystem;
  protected ExecutorService executorService;
  protected FsPermission perm = new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE);

  protected String destSubdir;
  protected String sourceSubdir;

  private static Logger log = Logger.getLogger(CamusSweeper.class);

  protected CamusSweeperPlanner planner;

  protected Map<String, Integer> priorityTopics = new HashMap<String, Integer>();

  public CamusSweeper() {
    props = new Properties();
  }

  public CamusSweeper(Properties props) {
    this.props = props;
    init();
  }

  private void init() {
    for (String str : props.getProperty(CAMUS_SWEEPER_PRIORITY_LIST, "").split(",")) {
      String[] tokens = str.split("=");
      String topic = tokens[0];
      int priority = tokens.length > 1 ? Integer.parseInt(tokens[1]) : 1;

      priorityTopics.put(topic, priority);
    }

    this.errorMessages = Collections.synchronizedList(new ArrayList<SweeperError>());
    DateTimeZone.setDefault(DateTimeZone.forID(props.getProperty("default.timezone")));
    this.runningJobs = Collections.synchronizedList(new ArrayList<Job>());
    sourceSubdir = props.getProperty("camus.sweeper.source.subdir");
    destSubdir = props.getProperty("camus.sweeper.dest.subdir");

    try {
      planner =
          ((CamusSweeperPlanner) Class.forName(props.getProperty("camus.sweeper.planner.class")).newInstance())
              .setPropertiesLogger(props, log);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  public void cancel() throws Exception {
    executorService.shutdownNow();

    for (Job hadoopJob : runningJobs) {
      if (!hadoopJob.isComplete()) {
        try {
          hadoopJob.killJob();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public Map<FileStatus, String> findAllTopics(Path input, PathFilter filter, String topicSubdir, FileSystem fs)
      throws IOException {
    Map<FileStatus, String> topics = new HashMap<FileStatus, String>();
    for (FileStatus f : fs.listStatus(input)) {
      // skipping first level, in search of the topic subdir
      findAllTopics(f.getPath(), filter, topicSubdir, "", fs, topics);
    }
    return topics;
  }

  private void findAllTopics(Path input, PathFilter filter, String topicSubdir, String topicNameSpace, FileSystem fs,
      Map<FileStatus, String> topics) throws IOException {
    for (FileStatus f : fs.listStatus(input)) {
      if (f.isDir()) {
        String topicFullName =
            (topicNameSpace.isEmpty() ? "" : topicNameSpace + ".") + f.getPath().getParent().getName();
        if (f.getPath().getName().equals(topicSubdir) && filter.accept(f.getPath().getParent())) {
          topics.put(fs.getFileStatus(f.getPath().getParent()), topicFullName);
        } else {
          findAllTopics(f.getPath(), filter, topicSubdir, topicFullName, fs, topics);
        }
      }
    }
  }

  protected void createExecutorService() {
    int numThreads = Integer.parseInt(props.getProperty("num.threads", DEFAULT_NUM_THREADS));
    executorService = new PriorityExecutor(numThreads);
  }

  public void run() throws Exception {
    log.info("Starting kafka sweeper");
    createExecutorService();

    String fromLocation = (String) props.getProperty("camus.sweeper.source.dir");
    String destLocation = (String) props.getProperty("camus.sweeper.dest.dir", "");
    String tmpLocation = (String) props.getProperty("camus.sweeper.tmp.dir", "");

    if (destLocation.isEmpty())
      destLocation = fromLocation;

    if (tmpLocation.isEmpty())
      tmpLocation = "/tmp";

    props.setProperty("camus.sweeper.tmp.dir", tmpLocation);

    log.info("fromLocation: " + fromLocation);
    log.info("destLocation: " + destLocation);

    List<String> blacklist = Utils.getStringList(props, "camus.sweeper.blacklist");
    List<String> whitelist = Utils.getStringList(props, "camus.sweeper.whitelist");
    Configuration conf = new Configuration();

    for (Entry<Object, Object> pair : props.entrySet()) {
      String key = (String) pair.getKey();
      conf.set(key, (String) pair.getValue());
    }

    this.fileSystem = FileSystem.get(conf);

    Path tmpPath = new Path(tmpLocation);

    if (!fileSystem.exists(tmpPath)) {
      fileSystem.mkdirs(tmpPath, perm);
      Log.info("Created tmpPath " + tmpPath + " with permissions " + perm + " and umask " + getUmask(conf));

      if (!fileSystem.getFileStatus(tmpPath).getPermission().equals(perm)) {
        log.error(String.format("Wrong permission for %s. Expects %s, actual %s", tmpPath, perm, fileSystem
            .getFileStatus(tmpPath).getPermission()));
        fileSystem.setPermission(tmpPath, perm);
      }

      String user = UserGroupInformation.getCurrentUser().getUserName();
      fileSystem.setOwner(tmpPath, user, user);
    }

    Path fromLocationPath = new Path(fromLocation);

    Map<FileStatus, String> topics =
        findAllTopics(fromLocationPath,
            new WhiteBlackListPathFilter(whitelist, blacklist, fileSystem.getFileStatus(fromLocationPath).getPath()),
            sourceSubdir, fileSystem);
    for (FileStatus topic : topics.keySet()) {
      String topicFullName = topics.get(topic);

      log.info("Processing topic " + topicFullName);

      Path destinationPath = new Path(destLocation + "/" + topics.get(topic).replace(".", "/") + "/" + destSubdir);
      try {
        runCollectorForTopicDir(fileSystem, topicFullName, new Path(topic.getPath(), sourceSubdir), destinationPath);
      } catch (Exception e) {
        System.err.println("unable to process " + topicFullName + " skipping...");
        e.printStackTrace();
      }
    }

    log.info("Shutting down priority executor");
    executorService.shutdown();
    while (!executorService.isTerminated()) {
      executorService.awaitTermination(30, TimeUnit.SECONDS);
    }

    log.info("Shutting down");

    if (!errorMessages.isEmpty()) {
      for (SweeperError error : errorMessages) {
        System.err.println("Error occurred in " + error.getTopic() + " at " + error.getInputPath().toString()
            + " message " + error.getException().getMessage());
        error.e.printStackTrace();
      }
      throw new RuntimeException("Sweeper Failed");
    }
  }

  private static String getUmask(Configuration conf) {
    if (conf.get(FsPermission.UMASK_LABEL) != null && conf.get(FsPermission.DEPRECATED_UMASK_LABEL) != null) {
      log.warn(String.format("Both umask labels exist: %s=%s, %s=%s", FsPermission.UMASK_LABEL,
          conf.get(FsPermission.UMASK_LABEL), FsPermission.DEPRECATED_UMASK_LABEL,
          conf.get(FsPermission.DEPRECATED_UMASK_LABEL)));
      return conf.get(FsPermission.UMASK_LABEL);

    } else if (conf.get(FsPermission.UMASK_LABEL) != null) {
      log.info(String.format("umask set: %s=%s", FsPermission.UMASK_LABEL, conf.get(FsPermission.UMASK_LABEL)));
      return conf.get(FsPermission.UMASK_LABEL);

    } else if (conf.get(FsPermission.DEPRECATED_UMASK_LABEL) != null) {
      log.info(String.format("umask set: %s=%s", FsPermission.DEPRECATED_UMASK_LABEL,
          conf.get(FsPermission.DEPRECATED_UMASK_LABEL)));
      return conf.get(FsPermission.DEPRECATED_UMASK_LABEL);

    } else {
      log.info("umask unset");
      return "undefined";
    }
  }

  protected void runCollectorForTopicDir(FileSystem fs, String topic, Path topicSourceDir, Path topicDestDir)
      throws Exception {
    log.info("Running collector for topic " + topic + " source:" + topicSourceDir + " dest:" + topicDestDir);
    ArrayList<Future<?>> tasksToComplete = new ArrayList<Future<?>>();

    List<Properties> jobPropsList = planner.createSweeperJobProps(topic, topicSourceDir, topicDestDir, fs);

    for (Properties jobProps : jobPropsList) {
      tasksToComplete.add(runCollector(jobProps, topic));
    }

    log.info("Finishing processing for topic " + topic);
  }

  protected Future<?> runCollector(Properties props, String topic) {
    String jobName = topic + "-" + UUID.randomUUID().toString();
    props
        .put("tmp.path", props.getProperty("camus.sweeper.tmp.dir") + "/" + jobName + "_" + System.currentTimeMillis());

    if (props.containsKey("reduce.count.override." + topic))
      props.put("reducer.count", Integer.parseInt(props.getProperty("reduce.count.override." + topic)));

    log.info("Processing " + props.get("input.paths"));

    return executorService.submit(new KafkaCollectorRunner(jobName, props, errorMessages, topic));
  }

  public class KafkaCollectorRunner implements Runnable, Important {
    protected Properties props;
    protected String name;
    protected List<SweeperError> errorQueue;
    protected String topic;
    protected int priority;

    public KafkaCollectorRunner(String name, Properties props, List<SweeperError> errorQueue, String topic) {
      this.name = name;
      this.props = props;
      this.errorQueue = errorQueue;
      this.topic = topic;

      priority = priorityTopics.containsKey(topic) ? priorityTopics.get(topic) : 0;
    }

    public void run() {
      KafkaCollector collector = null;
      try {
        log.info("Starting runner for " + name);
        collector = new KafkaCollector(props, name, topic);

        log.info("Waiting until input for job " + name + " is ready. Input directories:  "
            + props.getProperty("input.paths"));
        if (!planner.waitUntilReadyToProcess(props, fileSystem)) {
          throw new JobCancelledException("Job has been cancelled by planner while waiting for input to be ready.");
        }

        log.info("Running " + name + " for input " + props.getProperty("input.paths"));
        collector.run();
      } catch (Throwable e) // Sometimes the error is the Throwable, e.g. java.lang.NoClassDefFoundError
      {
        e.printStackTrace();
        log.error("Failed for " + name + " ,job: " + collector == null ? null : collector.getJob() + " failed for "
            + props.getProperty("input.paths") + " Exception:" + e.getLocalizedMessage());
        errorQueue.add(new SweeperError(name, props.get("input.paths").toString(), e));
      }
    }

    @Override
    public int getPriority() {
      return priority;
    }
  }

  protected class KafkaCollector {
    protected static final String TARGET_FILE_SIZE = "camus.sweeper.target.file.size";
    protected static final long TARGET_FILE_SIZE_DEFAULT = 1536l * 1024l * 1024l;
    protected long targetFileSize;
    protected final String jobName;
    protected final Properties props;
    protected final String topicName;
    protected final Path[] inputPaths;
    protected final Path tmpPath;
    protected final Path outputPath;
    protected final FileSystem fs;

    protected Job job;

    public KafkaCollector(Properties props, String jobName, String topicName) throws IOException {
      this.jobName = jobName;
      this.props = props;
      this.topicName = topicName;
      this.targetFileSize =
          props.containsKey(TARGET_FILE_SIZE) ? Long.parseLong(props.getProperty(TARGET_FILE_SIZE))
              : TARGET_FILE_SIZE_DEFAULT;

      job = new Job(getConf());
      job.setJarByClass(CamusSweeper.class);
      job.setJobName(jobName);

      for (Entry<Object, Object> pair : props.entrySet()) {
        String key = (String) pair.getKey();
        job.getConfiguration().set(key, (String) pair.getValue());
      }
      this.fs = FileSystem.get(job.getConfiguration());
      this.inputPaths = getInputPaths();
      this.tmpPath = new Path(job.getConfiguration().get(TMP_PATH));
      this.outputPath = new Path(job.getConfiguration().get(DEST_PATH));
      addInputAndOutputPathsToFileInputFormat();
    }

    private void addInputAndOutputPathsToFileInputFormat() throws IOException {
      for (Path path : inputPaths) {
        FileInputFormat.addInputPath(job, path);
      }
      FileOutputFormat.setOutputPath(job, tmpPath);
    }

    private Path[] getInputPaths() {
      List<String> strPaths = Utils.getStringList(props, INPUT_PATHS);
      Path[] inputPaths = new Path[strPaths.size()];
      for (int i = 0; i < strPaths.size(); i++)
        inputPaths[i] = new Path(strPaths.get(i));
      return inputPaths;
    }

    public void run() throws Exception {
      job.getConfiguration().set("mapred.compress.map.output", "true");
      ((CamusSweeperJob) Class.forName(props.getProperty("camus.sweeper.io.configurer.class")).newInstance())
          .setLogger(log).configureJob(topicName, job);

      setNumOfReducersAndSplitSizes();
      submitMrJob();

      moveTmpPathToOutputPath();
    }

    protected void moveTmpPathToOutputPath() throws IOException {
      Path oldPath = null;
      if (fs.exists(outputPath)) {
        oldPath = new Path("/tmp", "_old_" + job.getJobID());
        moveExistingContentInOutputPathToOldPath(oldPath);
      }

      log.info("Moving " + tmpPath + " to " + outputPath);
      mkdirs(fs, outputPath.getParent(), perm, job.getConfiguration());

      if (!fs.rename(tmpPath, outputPath)) {
        fs.rename(oldPath, outputPath);
        fs.delete(tmpPath, true);
        throw new RuntimeException("Error: cannot rename " + tmpPath + " to " + outputPath);
      }
      deleteOldPath(oldPath);
    }

    private void deleteOldPath(Path oldPath) throws IOException {
      if (oldPath != null && fs.exists(oldPath)) {
        log.info("Deleting " + oldPath);
        fs.delete(oldPath, true);
      }
    }

    private void moveExistingContentInOutputPathToOldPath(Path oldPath) throws IOException {
      log.info("Path " + outputPath + " exists. Overwriting. Existing content will be moved to " + oldPath);
      if (!fs.rename(outputPath, oldPath)) {
        fs.delete(tmpPath, true);
        throw new RuntimeException("Error: cannot rename " + outputPath + " to " + oldPath);
      }
    }

    protected void setNumOfReducersAndSplitSizes() throws IOException {
      long inputSize = getInputSize();

      int maxFiles = job.getConfiguration().getInt(MAX_FILES, DEFAULT_MAX_FILES);
      int numTasks = Math.min((int) (inputSize / targetFileSize) + 1, maxFiles);

      if (job.getNumReduceTasks() != 0) {
        determineAndSetNumOfReducers(numTasks);
      } else {
        setSplitSizes(inputSize / numTasks);
      }
    }

    private void setSplitSizes(long targetSplitSize) {
      log.info("Setting target split size " + targetSplitSize);
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
        log.info("inputPath: " + p.toString() + ", size=" + fs.getContentSummary(p).getLength());
        inputSize += fs.getContentSummary(p).getLength();
      }
      return inputSize;
    }

    protected void submitMrJob() throws IOException, InterruptedException, ClassNotFoundException {
      job.submit();
      runningJobs.add(job);

      log.info("job running for: " + job.getJobName() + ", url: " + job.getTrackingURL());
      job.waitForCompletion(false);
      if (!job.isSuccessful()) {
        throw new RuntimeException("hadoop job failed.");
      }
    }

    protected String getJobName() {
      return jobName;
    }

    protected String getTopicName() {
      return topicName;
    }

    protected Job getJob() {
      return job;
    }

    protected Properties getProps() {
      return this.props;
    }
  }

  protected void mkdirs(FileSystem fs, Path path, FsPermission perm, Configuration conf) throws IOException {
    if (!fs.exists(path.getParent()))
      mkdirs(fs, path.getParent(), perm, conf);
    String msg = "Creating " + path + " with permissions " + perm + " and umask " + getUmask(conf);
    if (!fs.exists(path)) {
      log.info(msg);
    }
    if (!fs.mkdirs(path, perm)) {
      msg = msg + " failed";
      log.error(msg);
      throw new IOException(msg);
    }
    if (!fs.getFileStatus(path).getPermission().equals(perm)) {
      log.error(String.format("Wrong permission for %s. Expects %s, actual %s", path, perm, fs.getFileStatus(path)
          .getPermission()));
      fs.setPermission(path, perm);
    }
  }

  public static class WhiteBlackListPathFilter implements PathFilter {
    private Pattern whitelist;
    private Pattern blacklist;
    private int rootLength;

    public WhiteBlackListPathFilter(Collection<String> whitelist, Collection<String> blacklist, Path qualRootDir) {
      if (whitelist.isEmpty())
        this.whitelist = Pattern.compile(".*"); //whitelist everything
      else
        this.whitelist = compileMultiPattern(whitelist);

      if (blacklist.isEmpty())
        this.blacklist = Pattern.compile("a^"); //blacklist nothing
      else
        this.blacklist = compileMultiPattern(blacklist);
      log.info("whitelist: " + this.whitelist.toString());
      log.info("blacklist: " + this.blacklist.toString());
      this.rootLength = qualRootDir.toString().length() + 1;
    }

    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      String fullName = path.toString().substring(rootLength).replaceAll("/", ".");
      return whitelist.matcher(fullName).matches()
          && !(blacklist.matcher(fullName).matches() || name.startsWith(".") || name.startsWith("_"));
    }

    private Pattern compileMultiPattern(Collection<String> list) {
      String patternStr = "(";

      for (String str : list) {
        patternStr += str + "|";
      }

      patternStr = patternStr.substring(0, patternStr.length() - 1) + ")";
      return Pattern.compile(patternStr);
    }
  }

  protected static class SweeperError {
    protected final String topic;
    protected final String input;
    protected final Throwable e;

    public SweeperError(String topic, String input, Throwable e) {
      this.topic = topic;
      this.input = input;
      this.e = e;
    }

    public String getTopic() {
      return topic;
    }

    public String getInputPath() {
      return input;
    }

    public Throwable getException() {
      return e;
    }
  }

  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption("p", true, "properties filename from the classpath");
    options.addOption("P", true, "external properties filename");

    options.addOption(OptionBuilder.withArgName("property=value").hasArgs(2).withValueSeparator()
        .withDescription("use value for given property").create("D"));

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    if (!(cmd.hasOption('p') || cmd.hasOption('P'))) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("CamusJob.java", options);
      return 1;
    }

    if (cmd.hasOption('p'))
      props.load(this.getClass().getResourceAsStream(cmd.getOptionValue('p')));

    if (cmd.hasOption('P')) {
      File file = new File(cmd.getOptionValue('P'));
      FileInputStream fStream = new FileInputStream(file);
      props.load(fStream);
    }

    props.putAll(cmd.getOptionProperties("D"));

    init();
    run();
    return 0;
  }

  public static void main(String args[]) throws Exception {
    CamusSweeper job = new CamusSweeper();
    ToolRunner.run(job, args);
  }
}
