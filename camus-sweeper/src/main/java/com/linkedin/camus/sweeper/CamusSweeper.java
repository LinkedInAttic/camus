package com.linkedin.camus.sweeper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;

import com.linkedin.camus.sweeper.mapreduce.CamusSweeperJob;
import com.linkedin.camus.sweeper.utils.Utils;

@SuppressWarnings("deprecation")
public class CamusSweeper extends Configured implements Tool
{
  private static final String DEFAULT_NUM_THREADS = "5";

  private List<SweeperError> errorMessages;
  private List<Job> runningJobs;

  private Properties props;
  private ExecutorService executorService;
  private FsPermission perm = new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE);

  private String destSubdir;
  private String sourceSubdir;

  private static Logger log = Logger.getLogger(CamusSweeper.class);

  private CamusSweeperPlanner planner;


  public CamusSweeper()
  {
    props = new Properties();
  }

  public CamusSweeper(Properties props)
  {
    this.props = props;
    init();
  }

  private void init()
  {
    this.errorMessages = Collections.synchronizedList(new ArrayList<SweeperError>());
    DateTimeZone.setDefault(DateTimeZone.forID(props.getProperty("default.timezone")));
    this.runningJobs = Collections.synchronizedList(new ArrayList<Job>());
    sourceSubdir = props.getProperty("camus.sweeper.source.subdir");
    destSubdir = props.getProperty("camus.sweeper.dest.subdir");

    try
    {
      planner = ((CamusSweeperPlanner) Class.forName(props.getProperty("camus.sweeper.planner.class")).newInstance()).setPropertiesLogger(props, log);
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }

  }

  // TODO:
  // Figure out the logic of canceling the jobs... How should the jobs be cancelled?
  // Essentially kill the job.. essentially all the jobs that have been launched should be killed
  // Do we need the cancel : Simply letting them execute depending on the files to be read should
  // solve the problem
  public void cancel() throws Exception
  {
    executorService.shutdownNow();

    for (Job hadoopJob : runningJobs)
    {
      if (!hadoopJob.isComplete())
      {
        try
        {
          hadoopJob.killJob();
        }
        catch (IOException e)
        {
          e.printStackTrace();
        }
      }
    }
  }

  private Map<FileStatus, String> findAllTopics(Path input, PathFilter filter, String topicSubdir, String topicNameSpace, FileSystem fs) throws IOException{
    Map<FileStatus, String> topics = new HashMap<FileStatus, String>();

    for (FileStatus f : fs.listStatus(input, filter)){
      if (fs.exists(new Path(f.getPath() + "/" + topicSubdir))){
        topics.put(f, topicNameSpace);
      } else if (! input.toString().equals(f.getPath().toString())) {
        findAllTopics(f.getPath(), filter, topicSubdir, (topicNameSpace.isEmpty() ? "" : topicNameSpace + "/") + f.getPath().getName(), fs);
      }
    }
    return topics;
  }

  public void runFromAzkaban() throws Exception {
    String camusPropertiesPath = System.getProperty("sun.java.command").split("-P ")[1];
    String[] args = {"-P", camusPropertiesPath};
    ToolRunner.run(this, args);
  }

  public void run() throws Exception
  {
    log.info("Starting kafka sweeper");
    int numThreads = Integer.parseInt(props.getProperty("num.threads", DEFAULT_NUM_THREADS));

    executorService = Executors.newFixedThreadPool(numThreads);

    String fromLocation = (String) props.getProperty("camus.sweeper.source.dir");
    String destLocation = (String) props.getProperty("camus.sweeper.dest.dir", "");

    if (destLocation.isEmpty())
      destLocation = fromLocation;

    log.info("fromLocation: " + fromLocation);
    log.info("destLocation: " + destLocation);

    List<String> blacklist = Utils.getStringList(props, "camus.sweeper.blacklist");
    List<String> whitelist = Utils.getStringList(props, "camus.sweeper.whitelist");
    Configuration conf = new Configuration();

    for (Entry<Object, Object> pair : props.entrySet())
    {
      String key = (String) pair.getKey();
      conf.set(key, (String) pair.getValue());
    }

    FileSystem fs = FileSystem.get(conf);
    Map<FileStatus, String> topics = findAllTopics(new Path(fromLocation), new BlackListPathFilter(whitelist, blacklist), sourceSubdir, "", fs);

    for (FileStatus topic : topics.keySet())
    {
      String topicName = topic.getPath().getName();
      log.info("Processing topic " + topicName);

      Path destinationPath = new Path(destLocation + "/" + topics.get(topic) + "/" + topic.getPath().getName() + "/" + destSubdir);
      try
      {
        runCollectorForTopicDir(fs, topicName, new Path(topic.getPath() + "/" + sourceSubdir), destinationPath);
      }
      catch (Exception e)
      {
        System.err.println("unable to process " + topicName + " skipping...");
        e.printStackTrace();
      }
    }

    log.info("Shutting down executor");
    executorService.shutdown();
    while (!executorService.isTerminated())
    {
      executorService.awaitTermination(30, TimeUnit.SECONDS);
    }

    log.info("Shutting down");

    if (!errorMessages.isEmpty())
    {
      for (SweeperError error : errorMessages)
      {
        System.err.println("Error occurred in " + error.getTopic() + " at " + error.getInputPath().toString()
            + " message " + error.getException().getMessage());
        error.e.printStackTrace();
      }
      throw new RuntimeException("Sweeper Failed");
    }
  }

  private void runCollectorForTopicDir(FileSystem fs, String topic, Path topicSourceDir, Path topicDestDir) throws Exception
  {
    log.info("Running collector for topic " + topic + " source:" + topicSourceDir + " dest:" + topicDestDir);
    ArrayList<Future<?>> tasksToComplete = new ArrayList<Future<?>>();

    List<Properties> jobPropsList = planner.createSweeperJobProps(topic, topicSourceDir, topicDestDir, fs);

    for (Properties jobProps : jobPropsList){
      tasksToComplete.add(runCollector(jobProps, topic));
    }

    log.info("Finishing processing for topic " + topic);
  }

  @SuppressWarnings("unchecked")
  private Future runCollector(Properties props, String topic)
  {
    String jobName = topic + "-" + UUID.randomUUID().toString();
    props.put("tmp.path", "/tmp/" + jobName + "_" + System.currentTimeMillis());

    if (props.containsKey("reduce.count.override." + topic))
      props.put("reducer.count", Integer.parseInt(props.getProperty("reduce.count.override." + topic)));

    log.info("Processing " + props.get("input.paths"));
    return executorService.submit(new KafkaCollectorRunner(jobName, props, errorMessages, topic));
  }

  public class KafkaCollectorRunner implements Runnable
  {
    private Properties props;
    private String name;
    private List<SweeperError> errorQueue;
    private String topic;

    public KafkaCollectorRunner(String name, Properties props, List<SweeperError> errorQueue, String topic)
    {
      this.name = name;
      this.props = props;
      this.errorQueue = errorQueue;
      this.topic = topic;
    }

    public void run()
    {
      KafkaCollector collector = null;
      try
      {
        log.info("Starting runner for " + name);
        collector = new KafkaCollector("test", props, name, topic);
        log.info("Running " + name + " for input " + props.getProperty("input.paths"));
        collector.run();
      }
      catch (Throwable e) // Sometimes the error is the Throwable, e.g. java.lang.NoClassDefFoundError
      {
        e.printStackTrace();
        log.error("Failed for " + name
                  + " ,job: " + collector == null ? null : collector.getJob()
                  + " failed for " + props.getProperty("input.paths") + " Exception:"
                  + e.getLocalizedMessage());
        errorQueue.add(new SweeperError(name, props.get("input.paths").toString(), e));
      }
    }
  }

  private class KafkaCollector
  {

    // About 1.5 gig
    private static final long AVERAGE_FILE_SIZE = 1536l * 1024l * 1024l;
    private final String jobName;
    private final Properties props;
    private final String topicName;
    private final List VALID_FILE_EXTENSIONS = Arrays.asList(new String[] {"gz", "avro", "snappy", "deflate", "bz2", "lzo"});

    private Job job;

    public KafkaCollector(String name, Properties props, String jobName, String topicName)
    {
      this.jobName = jobName;
      this.props = props;
      this.topicName = topicName;
    }

    private void addInputPath(Job job, Path path, FileSystem fs) throws IOException
    {
      for (FileStatus stat : fs.listStatus(path))
      {
        String name = stat.getPath().getName();
        String[] parts = name.split("\\.");
        if (stat.isDir())
        {
          addInputPath(job, stat.getPath(), fs);
        }
        else if (VALID_FILE_EXTENSIONS.contains(parts[parts.length-1]))
        {
          FileInputFormat.addInputPath(job, stat.getPath());
        }
      }
    }

    public void run() throws Exception
    {
      job = new Job(getConf());
      job.setJarByClass(CamusSweeper.class);
      job.setJobName(jobName);

      for (Entry<Object, Object> pair : props.entrySet())
      {
        String key = (String) pair.getKey();
        job.getConfiguration().set(key, (String) pair.getValue());
      }

      FileSystem fs = FileSystem.get(job.getConfiguration());
      List<String> strPaths = Utils.getStringList(props, "input.paths");
      Path[] inputPaths = new Path[strPaths.size()];

      for (int i = 0; i < strPaths.size(); i++)
        inputPaths[i] = new Path(strPaths.get(i));

      for (Path path : inputPaths)
      {
        addInputPath(job, path, fs);
      }

      int numReducers;

      if (job.getConfiguration().get("reducer.count") != null)
      {
        numReducers = job.getConfiguration().getInt("reducer.count", 45);
      }
      else
      {
        // Get Average file size

        long dus = 0;
        for (Path p : inputPaths)
          dus += duDirectory(fs, p);
        int maxFiles = job.getConfiguration().getInt("max.files", 24);
        numReducers = Math.min((int) (dus / AVERAGE_FILE_SIZE) + 1, maxFiles);
      }

      log.info("Setting reducer " + numReducers);
      job.setNumReduceTasks(numReducers);
      job.getConfiguration().set("mapred.compress.map.output", "false");

      Path tmpPath = new Path(job.getConfiguration().get("tmp.path"));
      Path outputPath = new Path(job.getConfiguration().get("dest.path"));

      FileOutputFormat.setOutputPath(job, tmpPath);
      ((CamusSweeperJob) Class.forName(props.getProperty("camus.sweeper.io.configurer.class"))
          .newInstance()).setLogger(log).configureJob(topicName, job);
      job.submit();
      runningJobs.add(job);
      log.info("job running: " + job.getTrackingURL() + " for: " + jobName);
      job.waitForCompletion(false);

      if (!job.isSuccessful())
      {
        System.err.println("hadoop job failed");
        throw new RuntimeException("hadoop job failed.");
      }

      Path oldPath = null;
      if (fs.exists(outputPath))
      {
        oldPath = new Path("/tmp", "_old_" + job.getJobID());
        log.info("Path " + outputPath + " exists. Overwriting.");
        if (!fs.rename(outputPath, oldPath))
        {
          fs.delete(tmpPath, true);
          throw new RuntimeException("Error: cannot rename " + outputPath + " to " + outputPath);
        }
      }

      log.info("Swapping " + tmpPath + " to " + outputPath);
      mkdirs(fs, outputPath.getParent(), perm);
      fs.mkdirs(outputPath.getParent(), perm);
      if (!fs.rename(tmpPath, outputPath))
      {
        fs.rename(oldPath, outputPath);
        fs.delete(tmpPath, true);
        throw new RuntimeException("Error: cannot rename " + tmpPath + " to " + outputPath);
      }

      if (oldPath != null && fs.exists(oldPath))
      {
        log.info("Deleting " + oldPath);
        fs.delete(oldPath, true);
      }
    }

    protected Job getJob() {
      return job;
    }

    private long duDirectory(FileSystem fs, Path dir) throws IOException
    {
      String name = dir.getName();
      if (name.startsWith("_") || name.startsWith("."))
      {
        return 0;
      }
      FileStatus status = fs.getFileStatus(dir);
      long sum = 0;
      if (status.isDir())
      {
        FileStatus[] subStatuses = fs.listStatus(dir);
        for (FileStatus subStatus : subStatuses)
        {
          sum += duDirectory(fs, subStatus.getPath());
        }
      }
      else
      {
        return status.getLen();
      }

      return sum;
    }
  }

  private void mkdirs(FileSystem fs, Path path, FsPermission perm) throws IOException{
    log.info("mkdir: " + path);
    if (! fs.exists(path.getParent()))
      mkdirs(fs, path.getParent(), perm);
    fs.mkdirs(path, perm);
  }

  private Pattern compileMultiPattern(Collection<String> list){
    String patternStr = "(";

    for (String str : list){
      patternStr += str + "|";
    }

    patternStr = patternStr.substring(0, patternStr.length() - 1) + ")";
    return Pattern.compile(patternStr);
  }

  private class BlackListPathFilter implements PathFilter
  {
    private Pattern whitelist;
    private Pattern blacklist;

    public BlackListPathFilter(Collection<String> whitelist, Collection<String> blacklist)
    {
      if (whitelist.isEmpty())
        this.whitelist = Pattern.compile(".*");  //whitelist everything
      else
        this.whitelist = compileMultiPattern(whitelist);

      if (blacklist.isEmpty())
        this.blacklist = Pattern.compile("a^");  //blacklist nothing
      else
        this.blacklist = compileMultiPattern(blacklist);
      log.info("whitelist: " + whitelist);
      log.info("blacklist: " + blacklist);
    }

    @Override
    public boolean accept(Path path)
    {
      String name = path.getName();
      return whitelist.matcher(name).matches() && ! ( blacklist.matcher(name).matches()
          || name.startsWith(".") || name.startsWith("_"));
    }
  }

  private static class SweeperError
  {
    private final String topic;
    private final String input;
    private final Throwable e;

    public SweeperError(String topic, String input, Throwable e)
    {
      this.topic = topic;
      this.input = input;
      this.e = e;
    }

    public String getTopic()
    {
      return topic;
    }

    public String getInputPath()
    {
      return input;
    }

    public Throwable getException()
    {
      return e;
    }
  }

  public int run(String[] args) throws Exception
  {
    Options options = new Options();

    options.addOption("p", true, "properties filename from the classpath");
    options.addOption("P", true, "external properties filename");

    options.addOption(OptionBuilder.withArgName("property=value")
                                   .hasArgs(2)
                                   .withValueSeparator()
                                   .withDescription("use value for given property")
                                   .create("D"));

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    if (!(cmd.hasOption('p') || cmd.hasOption('P')))
    {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("CamusJob.java", options);
      return 1;
    }

    if (cmd.hasOption('p'))
      props.load(this.getClass().getResourceAsStream(cmd.getOptionValue('p')));

    if (cmd.hasOption('P'))
    {
      File file = new File(cmd.getOptionValue('P'));
      FileInputStream fStream = new FileInputStream(file);
      props.load(fStream);
    }

    props.putAll(cmd.getOptionProperties("D"));

    init();
    run();
    return 0;
  }

  public static void main(String args[]) throws Exception
  {
    CamusSweeper job = new CamusSweeper();
    ToolRunner.run(job, args);
  }
}
