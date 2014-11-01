package com.linkedin.camus.shopify;

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
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.apache.log4j.Logger;

import com.linkedin.camus.sweeper.utils.DateUtils;
import com.linkedin.camus.sweeper.utils.Utils;

public class CamusCleaner extends Configured implements Tool
{
  private Properties props;
  private DateUtils dUtils;
  private DateTimeFormatter outputHourlyFormat;
  private DateTimeFormatter outputDailyFormat;
  private DateTimeFormatter outputMonthFormat;

  private FileSystem sourceFS;
  private Path sourcePath;

  private int maxIteration = 7; // days

  private static Logger log = Logger.getLogger(CamusCleaner.class);

  public CamusCleaner()
  {
    props = new Properties();
  }

  public CamusCleaner(Properties props)
  {
    this.props = props;
    dUtils = new DateUtils(props);
    outputHourlyFormat = dUtils.getDateTimeFormatter("YYYY/MM/dd/HH");
    outputDailyFormat = dUtils.getDateTimeFormatter("YYYY/MM/dd");
    outputMonthFormat = dUtils.getDateTimeFormatter("YYYY/MM");
  }

  public static void main(String args[]) throws Exception
  {
    CamusCleaner job = new CamusCleaner();
    ToolRunner.run(job, args);
  }

  public void runFromAzkaban() throws Exception {
    String camusPropertiesPath = System.getProperty("sun.java.command").split("-P ")[1];
    String[] args = {"-P" ,camusPropertiesPath};
    ToolRunner.run(this, args);
  }

  public void run() throws Exception
  {
    Configuration srcConf = new Configuration();

    sourceFS = FileSystem.get(srcConf);
    String fromLocation = (String) props.getProperty("camus.sweeper.source.dir");
    log.info("Cleaning directory" + fromLocation);
    sourcePath = new Path(fromLocation);

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
    log.info("Cleaning up topic " + topic);

    DateTime currentTime = (new DateTime(dUtils.zone)).minusHours(1);
    DateTime daysAgo = currentTime.minusDays(maxIteration);

    while (currentTime.isAfter(daysAgo)) {
      String hourString = outputHourlyFormat.print(currentTime);
      String dailyString = outputDailyFormat.print(currentTime);
      String monthlyString = outputMonthFormat.print(currentTime);


      Path sourceHourlyPath = new Path(sourcePath, topic + "/" + hourString);
      Path sourceDailyPath = new Path(sourcePath, topic + "/" + dailyString);
      Path sourceMonthlyPath = new Path(sourcePath, topic + "/" + monthlyString);

      if (sourceFS.exists(sourceHourlyPath)){
        deleteFolder(sourceFS, sourceHourlyPath);

        if(sourceFS.listStatus(sourceDailyPath).length == 0){
          deleteFolder(sourceFS, sourceDailyPath);

          if(sourceFS.listStatus(sourceMonthlyPath).length == 0){
            deleteFolder(sourceFS, sourceMonthlyPath);
          }
        }
      }
      currentTime = currentTime.minusHours(1);
    }
  }

  private void deleteFolder(FileSystem fs, Path path) throws IOException {
    log.info("Deleting " + path.toString());
    if(!fs.delete(path, true)){
      throw new IOException("Error deleting " + path + " on " + fs.getUri());
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
    outputHourlyFormat = dUtils.getDateTimeFormatter("YYYY/MM/dd/HH");
    outputDailyFormat = dUtils.getDateTimeFormatter("YYYY/MM/dd");
    outputMonthFormat = dUtils.getDateTimeFormatter("YYYY/MM");

    run();
    return 0;
  }
}
