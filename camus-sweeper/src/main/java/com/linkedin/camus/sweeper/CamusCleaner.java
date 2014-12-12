package com.linkedin.camus.sweeper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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

public class CamusCleaner extends Configured implements Tool {

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
  private String topicExceptionString = null;

  public CamusCleaner() {
    this.props = new Properties();
  }

  public CamusCleaner(Properties props) {
    this.props = props;
    dUtils = new DateUtils(props);
    outputDailyFormat = dUtils.getDateTimeFormatter(OUTPUT_DAILY_FORMAT_STR);
    outputMonthFormat = dUtils.getDateTimeFormatter("YYYY/MM");
    outputYearFormat = dUtils.getDateTimeFormatter("YYYY");

    sourceSubDir = props.getProperty("camus.sweeper.source.subdir");
    destSubDir = props.getProperty("camus.sweeper.dest.subdir", "");
  }

  public static void main(String args[]) throws Exception {
    CamusCleaner job = new CamusCleaner();
    ToolRunner.run(job, args);

  }

  public void run() throws Exception {

    log.info("Starting the Camus - Daily Cleaner");
    Configuration conf = new Configuration();
    fs = FileSystem.get(conf);

    List<String> blacklist = Utils.getStringList(props, "camus.sweeper.blacklist");
    List<String> whitelist = Utils.getStringList(props, "camus.sweeper.whitelist");

    // usually means hourly, but fromLocation can be daily subdir since the same
    // code is used for daily retention
    String fromLocation = (String) props.getProperty("camus.sweeper.source.dir");

    sourcePath = fs.getFileStatus(new Path(fromLocation)).getPath();

    log.debug("Path : " + sourcePath);
    simulate = Boolean.parseBoolean(props.getProperty(SIMULATE, "false"));
    force = Boolean.parseBoolean(props.getProperty(FORCE, "false"));

    // Topic-specific retention
    Map<String, String> map = Utils.getMapByPrefix(props, RETENTION_TOPIC_PREFIX);

    int regularRetention =
        Integer.parseInt((String) props.getProperty("camus.sweeper.clean.retention.days.global", "-1"));

    if (regularRetention != -1)
      log.info("Global retention set to " + regularRetention);
    else
      log.info("Global retention set to infinity, will not delete unspecified topics");

    WhiteBlackListPathFilter filter = new WhiteBlackListPathFilter(whitelist, blacklist, sourcePath);

    Map<FileStatus, String> topics = CamusSweeper.findAllTopics(sourcePath, filter, sourceSubDir, fs);

    for (FileStatus status : topics.keySet()) {
      String name = status.getPath().getName();
      if (name.startsWith(".") || name.startsWith("_")) {
        continue;
      }

      String fullname = topics.get(status);
      int topicRetention = map.containsKey(fullname) ? Integer.parseInt(map.get(fullname)) : regularRetention;
      enforceRetention(fullname, status, sourceSubDir, destSubDir, topicRetention);
    }

    if (topicExceptionString != null) {
      throw new RuntimeException(topicExceptionString);
    }
  }

  private void enforceRetention(String topicName, FileStatus topicDir, String topicSourceSubdir,
      String topicDestSubdir, int numDays) throws Exception {
    log.info("Running retention for " + topicName + " using " + numDays + " days");

    if (numDays != -1) {
      DateTime time = new DateTime(dUtils.zone);
      DateTime daysAgo = time.minusDays(numDays);
      Path sourceDailyGlob = new Path(topicDir.getPath() + "/" + topicSourceSubdir + "/*/*/*");
      for (FileStatus f : fs.globStatus(sourceDailyGlob)) {
        DateTime dirDateTime =
            outputDailyFormat.parseDateTime(f.getPath().toString()
                .substring(f.getPath().toString().length() - OUTPUT_DAILY_FORMAT_STR.length()));
        if (dirDateTime.isBefore(daysAgo)) {
          if (!(force || topicDestSubdir.isEmpty())) {
            Path destPath =
                new Path(topicDir.getPath(), topicDestSubdir + "/" + dirDateTime.toString(outputDailyFormat));

            if (!fs.exists(destPath)) {
              topicExceptionString =
                  topicExceptionString == null ? ("rollups do not exist for inputs: " + f.getPath()) : (", " + f
                      .getPath());
              continue;
            } else {
              FileStatus dest = fs.getFileStatus(destPath);

              for (FileStatus sourceFile : fs.listStatus(f.getPath())) {
                if (dest.getModificationTime() < sourceFile.getModificationTime()) {
                  topicExceptionString =
                      topicExceptionString == null ? ("source is older than rollup for inputs: " + f.getPath())
                          : (", " + f.getPath());
                }
              }
            }
          }
          deleteFileDir(fs, f.getPath());
        }
      }
    }

    Path sourceMonthlyGlob = new Path(topicDir.getPath() + "/" + topicSourceSubdir + "/*/*");
    for (FileStatus f : fs.globStatus(sourceMonthlyGlob)) {
      if (fs.listStatus(f.getPath()).length == 0)
        deleteFileDir(fs, f.getPath());
    }

    Path sourceYearGlob = new Path(topicDir.getPath() + "/" + topicSourceSubdir + "/*");
    for (FileStatus f : fs.globStatus(sourceYearGlob)) {
      if (fs.listStatus(f.getPath()).length == 0)
        deleteFileDir(fs, f.getPath());
    }

    Path sourceWithSubdir = new Path(topicDir.getPath() + "/" + topicSourceSubdir);
    if (fs.listStatus(sourceWithSubdir).length == 0)
      deleteFileDir(fs, sourceWithSubdir);

    if (fs.listStatus(topicDir.getPath()).length == 0)
      deleteFileDir(fs, topicDir.getPath());
  }

  private void deleteFileDir(FileSystem fs, Path deletePath) throws IOException {
    if (!simulate) {
      log.info("Deleting " + deletePath);
      if (fs.delete(deletePath, true)) {
        return;
      } else {
        throw new IOException("Path " + deletePath + " couldn't be deleted.");
      }
    } else {
      log.info("Simulating delete " + deletePath);
    }
  }

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
      props.load(ClassLoader.getSystemClassLoader().getResourceAsStream(cmd.getOptionValue('p')));

    if (cmd.hasOption('P')) {
      String pathname = cmd.getOptionValue('P');

      InputStream fStream;
      if (pathname.startsWith("hdfs:")) {
        Path pt = new Path(pathname);
        FileSystem fs = FileSystem.get(new Configuration());
        fStream = fs.open(pt);
      } else {
        File file = new File(pathname);
        fStream = new FileInputStream(file);
      }

      props.load(fStream);
      fStream.close();
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
