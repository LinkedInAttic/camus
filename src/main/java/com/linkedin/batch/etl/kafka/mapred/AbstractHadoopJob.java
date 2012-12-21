package com.linkedin.batch.etl.kafka.mapred;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import azkaban.common.jobs.AbstractJob;
import azkaban.common.utils.Props;

/**
 * An abstract Base class for Hadoop Jobs
 * 
 * @author bbansal
 * 
 */
public abstract class AbstractHadoopJob extends AbstractJob
{
  public static String       COMMON_FILE_DATE_PATTERN = "yyyy-MM-dd-HH-mm";
  public static final String HADOOP_PREFIX            = "hadoop-conf.";
  public static final String LATEST_SUFFIX            = "#LATEST";
  public static final String CURRENT_SUFFIX           = "#CURRENT";
  private final Props        _props;
  private Job                _job;

  public AbstractHadoopJob(String name, Props props)
  {
    super(name);
    this._props = props;
  }

  public void run(Job job) throws Exception
  {
    run(job, true);
  }

  public void run(Job job, boolean throwErrorOnFail) throws Exception
  {
    _job = job;
    _job.submit();
    info("See " + _job.getTrackingURL() + " for details.");
    _job.waitForCompletion(true);

    if (throwErrorOnFail && !_job.isSuccessful())
    {
      throw new Exception("Hadoop job:" + getId() + " failed!");
    }

    // dump all counters
    Counters counters = _job.getCounters();
    for (String groupName : counters.getGroupNames())
    {
      CounterGroup group = counters.getGroup(groupName);
      info("Group: " + group.getDisplayName());
      for (Counter counter : group)
      {
        info(counter.getDisplayName() + ":\t" + counter.getValue());
      }
    }
  }

  public Job createJobConf(Class<? extends Mapper> mapperClass) throws IOException,
      URISyntaxException
  {
    Job job = createJobConf(mapperClass, Reducer.class);
    job.setNumReduceTasks(0);

    return job;
  }

  public Job createJobConf(Class<? extends Mapper> mapperClass,
                           Class<? extends Reducer> reducerClass,
                           Class<? extends Reducer> combinerClass) throws IOException,
      URISyntaxException
  {
    Job job = createJobConf(mapperClass, reducerClass);
    job.setCombinerClass(combinerClass);

    return job;
  }

  public Job createJobConf(Class<? extends Mapper> mapperClass,
                           Class<? extends Reducer> reducerClass) throws IOException,
      URISyntaxException
  {
    Job job = new Job();
    // set custom class loader with custom find resource strategy.

    job.setJobName(getId());
    job.setMapperClass(mapperClass);
    job.setReducerClass(reducerClass);

    String hadoop_ugi = _props.getString("hadoop.job.ugi", null);
    if (hadoop_ugi != null)
    {
      job.getConfiguration().set("hadoop.job.ugi", hadoop_ugi);
    }

    if (_props.getBoolean("is.local", false))
    {
      job.getConfiguration().set("mapred.job.tracker", "local");
      job.getConfiguration().set("fs.default.name", "file:///");
      job.getConfiguration().set("mapred.local.dir", "/tmp/map-red");

      info("Running locally, no hadoop jar set.");
    }
    else
    {
      setClassLoaderAndJar(job, getClass());
      info("Setting hadoop jar file for class:" + getClass() + "  to " + job.getJar());
      info("*************************************************************************");
      info("          Running on Real Hadoop Cluster("
          + job.getConfiguration().get("mapred.job.tracker") + ")           ");
      info("*************************************************************************");
    }

    // set JVM options if present
    if (_props.containsKey("mapred.child.java.opts"))
    {
      job.getConfiguration().set("mapred.child.java.opts",
                                 _props.getString("mapred.child.java.opts"));
      info("mapred.child.java.opts set to " + _props.getString("mapred.child.java.opts"));
    }

    // set input and output paths if they are present
    if (_props.containsKey("input.paths"))
    {
      List<String> inputPaths = _props.getStringList("input.paths");
      if (inputPaths.size() == 0)
      {
        throw new IllegalArgumentException("Must specify at least one value for property 'input.paths'");
      }
      for (String path : inputPaths)
      {
        // Implied stuff, but good implied stuff
        if (path.endsWith(LATEST_SUFFIX))
        {
          FileSystem fs = FileSystem.get(job.getConfiguration());

          PathFilter filter = new PathFilter()
          {
            @Override
            public boolean accept(Path arg0)
            {
              return !arg0.getName().startsWith("_") && !arg0.getName().startsWith(".");
            }
          };

          String latestPath = path.substring(0, path.length() - LATEST_SUFFIX.length());
          FileStatus[] statuses = fs.listStatus(new Path(latestPath), filter);

          Arrays.sort(statuses);

          path = statuses[statuses.length - 1].getPath().toString();
          System.out.println("Using latest folder: " + path);
        }
        FileInputFormat.addInputPath(job, new Path(path));
      }
    }

    if (_props.containsKey("output.path"))
    {
      String location = _props.get("output.path");
      if (location.endsWith("#CURRENT"))
      {
        DateTimeFormatter format = DateTimeFormat.forPattern(COMMON_FILE_DATE_PATTERN);
        String destPath = format.print(new DateTime());
        location =
            location.substring(0, location.length() - "#CURRENT".length()) + destPath;
        System.out.println("Store location set to " + location);
      }

      FileOutputFormat.setOutputPath(job, new Path(location));
      // For testing purpose only remove output file if exists
      if (_props.getBoolean("force.output.overwrite", false))
      {
        FileSystem fs =
            FileOutputFormat.getOutputPath(job).getFileSystem(job.getConfiguration());
        fs.delete(FileOutputFormat.getOutputPath(job), true);
      }
    }

    // Adds External jars to hadoop classpath
    String externalJarList = _props.getString("hadoop.external.jarFiles", null);
    if (externalJarList != null)
    {
      String[] jarFiles = externalJarList.split(",");
      for (String jarFile : jarFiles)
      {
        info("Adding extenral jar File:" + jarFile);
        DistributedCache.addFileToClassPath(new Path(jarFile), job.getConfiguration());
      }
    }

    // Adds distributed cache files
    String cacheFileList = _props.getString("hadoop.cache.files", null);
    if (cacheFileList != null)
    {
      String[] cacheFiles = cacheFileList.split(",");
      for (String cacheFile : cacheFiles)
      {
        info("Adding Distributed Cache File:" + cacheFile);
        DistributedCache.addCacheFile(new URI(cacheFile), job.getConfiguration());
      }
    }

    // Adds distributed cache files
    String archiveFileList = _props.getString("hadoop.cache.archives", null);
    if (archiveFileList != null)
    {
      String[] archiveFiles = archiveFileList.split(",");
      for (String archiveFile : archiveFiles)
      {
        info("Adding Distributed Cache Archive File:" + archiveFile);
        DistributedCache.addCacheArchive(new URI(archiveFile), job.getConfiguration());
      }
    }

    String hadoopCacheJarDir = _props.getString("hdfs.default.classpath.dir", null);
    if (hadoopCacheJarDir != null)
    {
      FileSystem fs = FileSystem.get(job.getConfiguration());
      if (fs != null)
      {
        FileStatus[] status = fs.listStatus(new Path(hadoopCacheJarDir));

        if (status != null)
        {
          for (int i = 0; i < status.length; ++i)
          {
            if (!status[i].isDir())
            {
              Path path = new Path(hadoopCacheJarDir, status[i].getPath().getName());
              info("Adding Jar to Distributed Cache Archive File:" + path);

              DistributedCache.addFileToClassPath(path, job.getConfiguration());
            }
          }
        }
        else
        {
          info("hdfs.default.classpath.dir " + hadoopCacheJarDir + " is empty.");
        }
      }
      else
      {
        info("hdfs.default.classpath.dir " + hadoopCacheJarDir
            + " filesystem doesn't exist");
      }
    }

    // May want to add this to HadoopUtils, but will await refactoring
    for (String key : getProps().keySet())
    {
      String lowerCase = key.toLowerCase();
      if (lowerCase.startsWith(HADOOP_PREFIX))
      {
        String newKey = key.substring(HADOOP_PREFIX.length());
        job.getConfiguration().set(newKey, getProps().get(key));
      }
    }

    // HadoopUtils.setPropsInJob(job.getConfiguration(), getProps());
    return job;
  }

  public Props getProps()
  {
    return this._props;
  }

  @Override
  public void cancel() throws Exception
  {
    if (_job != null)
    {
      _job.killJob();
    }
  }

  @Override
  public double getProgress() throws IOException
  {
    if (_job == null)
    {
      return 0.0;
    }
    else
    {
      return (_job.mapProgress() + _job.reduceProgress()) / 2.0d;
    }
  }

  public Counters getCounters() throws IOException
  {
    return _job.getCounters();
  }

  public JobID getJobID() throws IOException
  {
    return JobID.forName(_job.getTrackingURL().substring(_job.getTrackingURL()
                                                             .lastIndexOf("=") + 1));
  }

  public static void setClassLoaderAndJar(Job job, Class jobClass)
  {
    job.getConfiguration().setClassLoader(Thread.currentThread().getContextClassLoader());
    job.setJarByClass(jobClass);
  }
}
