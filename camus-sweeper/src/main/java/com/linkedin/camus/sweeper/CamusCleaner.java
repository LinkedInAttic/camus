package com.linkedin.camus.sweeper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.camus.sweeper.CamusSweeper.WhiteBlackListPathFilter;
import com.linkedin.camus.sweeper.utils.DateUtils;
import com.linkedin.camus.sweeper.utils.Utils;

/**
 * 
 * Responsible for cleaning out the daily files based on the retention value set in the config
 * 
 */

public class CamusCleaner extends Configured implements Tool
{

  public static final String SIMULATE = "camus.sweeper.clean.simulate";
  public static final String FORCE = "camus.sweeper.clean.force";
  public static final String RETENTION_TOPIC_PREFIX = "camus.sweeper.clean.retention.days.topic.";
  public static final String OUTPUT_DAILY_FORMAT_STR = "YYYY/MM/dd";

  private DateUtils dUtils;
  private DateTimeFormatter outputDailyFormat;
  private DateTimeFormatter outputMonthFormat;
  private DateTimeFormatter outputYearFormat;

  private final Properties props;

  private Path sourcePath;
  private String sourceSubDir;
  private String destSubDir;
  private FileSystem fs;
  private boolean simulate = false;
  private boolean force = false;
  private static Logger log = Logger.getLogger(CamusCleaner.class);

  public CamusCleaner()
  {
    this.props = new Properties();
  }

  public CamusCleaner(Properties props)
  {
    this.props = props;
    dUtils = new DateUtils(props);
    outputDailyFormat = dUtils.getDateTimeFormatter(OUTPUT_DAILY_FORMAT_STR);
    outputMonthFormat = dUtils.getDateTimeFormatter("YYYY/MM");
    outputYearFormat = dUtils.getDateTimeFormatter("YYYY");
  
    sourceSubDir = props.getProperty("camus.sweeper.source.subdir");
    destSubDir = props.getProperty("camus.sweeper.dest.subdir", "");
  }

  public static void main(String args[]) throws Exception
  {
    CamusCleaner job = new CamusCleaner();
    ToolRunner.run(job, args);

  }

  public void run() throws Exception
  {

    log.info("Starting the Camus - Daily Cleaner");
    Configuration conf = new Configuration();
    fs = FileSystem.get(conf);
    
    List<String> blacklist = Utils.getStringList(props, "camus.sweeper.blacklist");
    List<String> whitelist = Utils.getStringList(props, "camus.sweeper.whitelist");
    
    String fromLocation = (String) props.getProperty("camus.sweeper.source.dir");
    String destLocation = (String) props.getProperty("camus.sweeper.dest.dir", "");
    
    if (destLocation.isEmpty())
      destLocation = fromLocation;
    
    sourcePath = fs.getFileStatus(new Path(destLocation)).getPath();
    
    log.debug("Daily Path : " + sourcePath);
    simulate = Boolean.parseBoolean(props.getProperty(SIMULATE, "false"));
    force = Boolean.parseBoolean(props.getProperty(FORCE, "false"));

    // Topic-specific retention
    Map<String, String> map = Utils.getMapByPrefix(props, RETENTION_TOPIC_PREFIX);

    int regularRetention = Integer.parseInt((String) props.getProperty("camus.sweeper.clean.retention.days.global", "-1"));

    if (regularRetention != -1)
      System.out.println("Global retention set to " + regularRetention);
    else
      System.out.println("Global retention set to infinity, will not delete unspecified topics");
    
    WhiteBlackListPathFilter filter = new WhiteBlackListPathFilter(whitelist, blacklist, sourcePath);

    Map<FileStatus, String> topics = CamusSweeper.findAllTopics(sourcePath, filter, sourceSubDir, fs);
    
    for (FileStatus status : topics.keySet())
    {
      String name = status.getPath().getName();
      if (name.startsWith(".") || name.startsWith("_"))
      {
        continue;
      }
      
      String fullname = topics.get(status);

      if (map.containsKey(fullname) && Integer.parseInt(map.get(fullname)) != -1)
      {
        enforceRetention(fullname, status, sourceSubDir, destSubDir, Integer.parseInt(map.get(fullname)));
      }
      else if (regularRetention != -1)
      {
        enforceRetention(fullname, status, sourceSubDir, destSubDir, regularRetention);
      }
    }
  }

  private void enforceRetention(String topicName, FileStatus topicDir, String topicSourceSubdir, String topicDestSubdir, int numDays) throws Exception
  {
    System.out.println("Running retention for " + topicName + " and for days " + numDays);
    DateTime time = new DateTime(dUtils.zone);
    DateTime daysAgo = time.minusDays(numDays);

    Path sourceDailyGlob = new Path(topicDir.getPath() + "/" + topicSourceSubdir + "/*/*/*");

    for (FileStatus f : fs.globStatus(sourceDailyGlob))
    {
      DateTime dirDateTime =
          outputDailyFormat.parseDateTime(f.getPath()
                                           .toString()
                                           .substring(f.getPath().toString().length()
                                               - OUTPUT_DAILY_FORMAT_STR.length()));
      if (dirDateTime.isBefore(daysAgo)) {
        if (! (force || topicDestSubdir.isEmpty())){
          Path destPath = new Path(topicDir.getPath(), topicDestSubdir + "/" + dirDateTime.toString(outputDailyFormat));
          
          if (! fs.exists(destPath))
            throw new RuntimeException("rollup does not exist for input: " + f.getPath());
        }
        deleteDay(f.getPath());
      }
    }
  }

  private void deleteDay(Path dayPath) throws Exception
  {
    Path monthPath = dayPath.getParent();
    Path yearPath = monthPath.getParent();

    if (fs.exists(dayPath))
    {
      System.out.println(" Deleting day " + yearPath.getName() + "/" + monthPath.getName() + "/" + dayPath.getName());
      deleteFileDir(fs, dayPath);

      if (fs.listStatus(monthPath).length == 0)
      {
        System.out.println(" Deleting month " + yearPath.getName() + "/" + monthPath.getName());
        deleteFileDir(fs, monthPath);

        if (fs.listStatus(yearPath).length == 0)
        {
          System.out.println(" Deleting year " + yearPath.getName());
          deleteFileDir(fs, yearPath);
        }
      }
    }
  }

  private void deleteFileDir(FileSystem fs, Path deletePath) throws IOException
  {
    if (!simulate)
    {
      System.out.println("Deleting " + deletePath);
      if (fs.delete(deletePath, true))
      {
        return;
      }
      else
      {
        throw new IOException("Path " + deletePath + " couldn't be deleted.");
      }
    }
    else
    {
      System.out.println("Simulating delete " + deletePath);
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
      props.load(ClassLoader.getSystemClassLoader().getResourceAsStream(cmd.getOptionValue('p')));

    if (cmd.hasOption('P'))
    {
      File file = new File(cmd.getOptionValue('P'));
      FileInputStream fStream = new FileInputStream(file);
      props.load(fStream);
    }

    props.putAll(cmd.getOptionProperties("D"));

    dUtils = new DateUtils(props);
    outputDailyFormat = dUtils.getDateTimeFormatter(OUTPUT_DAILY_FORMAT_STR);
    outputMonthFormat = dUtils.getDateTimeFormatter("YYYY/MM");
    outputYearFormat = dUtils.getDateTimeFormatter("YYYY");

    run();
    return 0;
  }
}
