package com.linkedin.camus.sweeper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
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

import com.linkedin.camus.sweeper.utils.DateUtils;
import com.linkedin.camus.sweeper.utils.Utils;

/**
 * 
 * Responsible for cleaning out the daily files based on the retention value set in the config
 * 
 */

public class CamusDailyCleaner extends Configured implements Tool
{

  public static final String SIMULATE = "camus.sweeper.clean.simulate";
  public static final String RETENTION_TOPIC_PREFIX = "camus.sweeper.clean.retention.days.topic.";
  public static final String OUTPUT_DAILY_FORMAT_STR = "YYYY/MM/dd";

  private DateUtils dUtils;
  private DateTimeFormatter outputDailyFormat;
  private DateTimeFormatter outputMonthFormat;
  private DateTimeFormatter outputYearFormat;

  private final Properties props;

  private String dailyPath;
  private FileSystem fs;
  private boolean simulate = false;
  private static Logger log = Logger.getLogger(CamusDailyCleaner.class);

  public CamusDailyCleaner()
  {
    this.props = new Properties();
  }

  public CamusDailyCleaner(Properties props)
  {
    this.props = props;
    DateUtils dUtils = new DateUtils(props);
    outputDailyFormat = dUtils.getDateTimeFormatter(OUTPUT_DAILY_FORMAT_STR);
    outputMonthFormat = dUtils.getDateTimeFormatter("YYYY/MM");
    outputYearFormat = dUtils.getDateTimeFormatter("YYYY");
  }

  public static void main(String args[]) throws Exception
  {
    CamusDailyCleaner job = new CamusDailyCleaner();
    ToolRunner.run(job, args);

  }

  public void run() throws Exception
  {

    log.info("Starting the Camus - Daily Cleaner");
    
    String fromLocation = (String) props.getProperty("camus.sweeper.source.dir");
    String destLocation = (String) props.getProperty("camus.sweeper.dest.dir", "");
    
    if (destLocation.isEmpty())
      destLocation = fromLocation;
    
    dailyPath = destLocation;
    
    log.debug("Daily Path : " + dailyPath);
    simulate = Boolean.parseBoolean(props.getProperty(SIMULATE, "false"));

    Configuration conf = new Configuration();
    fs = FileSystem.get(conf);

    // Topic-specific retention
    Map<String, String> map = Utils.getMapByPrefix(props, RETENTION_TOPIC_PREFIX);

    int regularRetention = Integer.parseInt((String) props.getProperty("camus.sweeper.clean.retention.days.global", "-1"));

    if (regularRetention != -1)
      System.out.println("Global retention set to " + regularRetention);
    else
      System.out.println("Global retention set to infinity, will not delete unspecified topics");

    FileStatus[] statuses = fs.listStatus(new Path(dailyPath));
    for (FileStatus status : statuses)
    {
      String name = status.getPath().getName();
      if (name.startsWith(".") || name.startsWith("_"))
      {
        continue;
      }

      if (map.containsKey(name))
      {
        enforceRetention(name, Integer.parseInt(map.get(name)));
      }
      else if (regularRetention != -1)
      {
        enforceRetention(name, regularRetention);
      }
    }
  }

  private void enforceRetention(String topic, int numDays) throws Exception
  {
    System.out.println("Running retention for " + topic + " and for days " + numDays);
    DateTime time = new DateTime(dUtils.zone);
    DateTime daysAgo = time.minusDays(numDays);

    Path sourceDailyGlob = new Path(dailyPath, topic + "/daily/*/*/*");

    for (FileStatus f : fs.globStatus(sourceDailyGlob))
    {
      DateTime dirDateTime =
          outputDailyFormat.parseDateTime(f.getPath()
                                           .toString()
                                           .substring(f.getPath().toString().length()
                                               - OUTPUT_DAILY_FORMAT_STR.length()));
      if (dirDateTime.isBefore(daysAgo))
        deleteDay(topic, f.getPath());
    }
  }

  private void deleteDay(String topic, Path dayPath) throws Exception
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
