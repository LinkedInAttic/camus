package com.linkedin.camus.sweeper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
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
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.camus.sweeper.utils.DateUtils;
import com.linkedin.camus.sweeper.utils.Utils;

//import azkaban.common.jobs.AbstractJob;
//import azkaban.common.utils.Props;

public class CamusHourlyCleaner extends Configured implements Tool
{
  
  public static final String SIMULATE = "camus.sweeper.clean.simulate";
  public static final String KAFKA_FORCE_DELETE = "kafka.force.delete";

  private Properties props;
  private DateUtils dUtils;
  private DateTimeFormatter outputDailyFormat;
  private DateTimeFormatter outputMonthFormat;

  private FileSystem sourceFS;

  private Path sourcePath;

  private int numDays = 14;
  private int lookbackDays = 30;

  private boolean simulate = false;
  private boolean forceDelete;

  public CamusHourlyCleaner()
  {
    // TODO Auto-generated constructor stub
  }

  public CamusHourlyCleaner(Properties props)
  {
    dUtils = new DateUtils(props);
    outputDailyFormat = dUtils.getDateTimeFormatter("YYYY/MM/dd");
    outputMonthFormat = dUtils.getDateTimeFormatter("YYYY/MM");
  }

  public void run() throws Exception
  {
    System.out.println("Cleaning");


    Configuration srcConf = new Configuration();

    sourceFS = FileSystem.get(srcConf);

    String fromLocation = (String) props.getProperty("camus.sweeper.source.dir");
    String destLocation = (String) props.getProperty("camus.sweeper.dest.dir", "");
    
    if (destLocation.isEmpty())
      destLocation = fromLocation;
    
    sourcePath = new Path(destLocation);

    numDays = Integer.parseInt((String) props.getProperty("camus.sweeper.clean.retention.hourly.num.days", "14"));
    lookbackDays = Integer.parseInt((String) props.getProperty("camus.sweeper.clean.hourly.lookback.days", "30"));

    simulate = Boolean.parseBoolean(props.getProperty(SIMULATE, "false"));
    forceDelete = Boolean.parseBoolean(props.getProperty(KAFKA_FORCE_DELETE, "false"));

    for (FileStatus status : sourceFS.listStatus(sourcePath))
    {
      String name = status.getPath().getName();
      if (name.startsWith(".") || name.startsWith("_"))
      {
        continue;
      }

      iterateTopic(name);
    }
  }

  private void iterateTopic(String topic) throws IOException
  {
    System.out.println("Cleaning up topic " + topic);

    DateTime time = new DateTime(dUtils.zone);
    DateTime daysAgo = time.minusDays(numDays);

    DateTime currentTime = time.minusDays(lookbackDays);
    int currentMonth = currentTime.getMonthOfYear();

    while (currentTime.isBefore(daysAgo))
    {
      String dateString = outputDailyFormat.print(currentTime);
      Path sourceHourlyDate = new Path(sourcePath, topic + "/hourly/" + dateString);
      Path sourceDailyDate = new Path(sourcePath, topic + "/daily/" + dateString);

      if (sourceFS.exists(sourceHourlyDate))
      {
        System.out.println("Hourly data exists for " + sourceHourlyDate.toString());
        if (sourceFS.exists(sourceDailyDate) || forceDelete)
        {
          System.out.println("Deleting " + sourceHourlyDate);
          // We should be sure that if this source is deleted that the destinations were also
          // cleared out too.
          if (!simulate && !sourceFS.delete(sourceHourlyDate, true))
          {
            throw new IOException("Error deleting " + sourceHourlyDate + " on " + sourceFS.getUri());
          }
        }
        else
        {
          throw new IOException("Daily data for " + sourceHourlyDate + " doesn't exist!");
        }
      }

      DateTime newTime = currentTime.plusDays(1);
      if (newTime.getMonthOfYear() != currentMonth)
      {
        System.out.println("Checking month to see if we need to clean up");
        Path monthPath = new Path(sourcePath, topic + "/hourly/" + outputMonthFormat.print(currentTime));

        FileStatus[] status = sourceFS.listStatus(monthPath);
        if (!simulate && status != null && status.length == 0)
        {
          System.out.println("Deleting " + monthPath);
          sourceFS.delete(monthPath, true);
        }

        currentMonth = newTime.getMonthOfYear();
      }

      currentTime = newTime;
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
    outputDailyFormat = dUtils.getDateTimeFormatter("YYYY/MM/dd");
    outputMonthFormat = dUtils.getDateTimeFormatter("YYYY/MM");

    run();
    return 0;
  }
}
