package com.linkedin.camus.etl.kafka.mapred;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.EtlCounts;
import com.linkedin.camus.etl.kafka.common.EtlKey;


public class EtlMultiOutputCommitter extends FileOutputCommitter {
  public static final String ETL_COMMIT_HOOKS = "etl.commit.hooks";
  private final List<CommitHook> commitHooks = new ArrayList<CommitHook>();
  private Pattern workingFileMetadataPattern;

  private HashMap<String, EtlCounts> counts = new HashMap<String, EtlCounts>();
  private HashMap<String, EtlKey> offsets = new HashMap<String, EtlKey>();
  private HashMap<String, Long> eventCounts = new HashMap<String, Long>();

  private TaskAttemptContext context;
  private final RecordWriterProvider recordWriterProvider;
  private Logger log;

  private void addCommitHooks(TaskAttemptContext context, Path outputPath) throws IOException {
    try {
      Path workPath = super.getWorkPath();
      log.debug("Adding offset writer hook: " + SequenceFileOffsetCommitter.class.getCanonicalName());
      commitHooks.add(new SequenceFileOffsetCommitter());

      for (Class<?> klass : context.getConfiguration().getClasses(ETL_COMMIT_HOOKS)) {
        log.debug("Adding offset writer hook: " + klass.getCanonicalName());
        Constructor constructor = klass.getConstructor();
        CommitHook commitHook = (CommitHook) constructor.newInstance();
        commitHooks.add(commitHook);
      }
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void mkdirs(FileSystem fs, Path path) throws IOException {
    if (! fs.exists(path.getParent())) {
      mkdirs(fs, path.getParent());
    }
    fs.mkdirs(path);
  }

  @Override
  public void setupTask(org.apache.hadoop.mapreduce.TaskAttemptContext context) throws java.io.IOException {
    this.context = context;
  }

  public void addCounts(EtlKey key) throws IOException {
    String workingFileName = EtlMultiOutputFormat.getWorkingFileName(context, key);
    if (!counts.containsKey(workingFileName))
      counts.put(workingFileName,
          new EtlCounts(key.getTopic(), EtlMultiOutputFormat.getMonitorTimeGranularityMs(context)));
    counts.get(workingFileName).incrementMonitorCount(key);
    addOffset(key);
  }

  public void addOffset(EtlKey key) {
    String topicPart = key.getTopic() + "-" + key.getLeaderId() + "-" + key.getPartition();
    EtlKey offsetKey = new EtlKey(key);

    if (offsets.containsKey(topicPart)) {
      long avgSize = offsets.get(topicPart).getMessageSize() * eventCounts.get(topicPart) + key.getMessageSize();
      avgSize /= eventCounts.get(topicPart) + 1;
      offsetKey.setMessageSize(avgSize);
    } else {
      eventCounts.put(topicPart, 0l);
    }
    eventCounts.put(topicPart, eventCounts.get(topicPart) + 1);
    offsets.put(topicPart, offsetKey);
  }

  public EtlMultiOutputCommitter(Path outputPath, TaskAttemptContext context, Logger log)
          throws IOException {
    super(outputPath, context);
    this.context = context;
    try {
      //recordWriterProvider = EtlMultiOutputFormat.getRecordWriterProviderClass(context).newInstance();
      Class<RecordWriterProvider> rwp = EtlMultiOutputFormat.getRecordWriterProviderClass(context);
      Constructor<RecordWriterProvider> crwp = rwp.getConstructor(TaskAttemptContext.class);
      recordWriterProvider = crwp.newInstance(context);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    workingFileMetadataPattern =
            Pattern.compile("data\\.([^\\.]+)\\.([\\d_]+)\\.(\\d+)\\.([^\\.]+)-m-\\d+"
                    + recordWriterProvider.getFilenameExtension());
    this.log = log;

    addCommitHooks(context, outputPath);
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {

    ArrayList<Map<String, Object>> allCountObject = new ArrayList<Map<String, Object>>();
    FileSystem fs = FileSystem.get(context.getConfiguration());
    Path workPath = super.getWorkPath();
    log.info("work path: " + workPath);
    if (EtlMultiOutputFormat.isRunMoveData(context)) {
      Path baseOutDir = EtlMultiOutputFormat.getDestinationPath(context);
      log.info("Destination base path: " + baseOutDir);
      for (FileStatus f : fs.listStatus(workPath)) {
        String file = f.getPath().getName();
        log.info("work file: " + file);
        if (file.startsWith("data")) {
          String workingFileName = file.substring(0, file.lastIndexOf("-m"));
          EtlCounts count = counts.get(workingFileName);

          String partitionedFile =
              getPartitionedPath(context, file, count.getEventCount(), count.getLastKey().getOffset());

          Path dest = new Path(baseOutDir, partitionedFile);

          if (!fs.exists(dest.getParent())) {
            mkdirs(fs, dest.getParent());
          }

          commitFile(context, f.getPath(), dest);
          log.info("Moved file from: " + f.getPath() + " to: " + dest);

          if (EtlMultiOutputFormat.isRunTrackingPost(context)) {
            count.writeCountsToMap(allCountObject, fs, new Path(workPath, EtlMultiOutputFormat.COUNTS_PREFIX + "."
                + dest.getName().replace(recordWriterProvider.getFilenameExtension(), "")));
          }
        }
      }

      if (EtlMultiOutputFormat.isRunTrackingPost(context)) {
        Path tempPath = new Path(workPath, "counts." + context.getConfiguration().get("mapred.task.id"));
        OutputStream outputStream = new BufferedOutputStream(fs.create(tempPath));
        ObjectMapper mapper = new ObjectMapper();
        log.info("Writing counts to : " + tempPath.toString());
        long time = System.currentTimeMillis();
        mapper.writeValue(outputStream, allCountObject);
        log.debug("Time taken : " + (System.currentTimeMillis() - time) / 1000);
      }
    } else {
      log.info("Not moving run data.");
    }

    for (CommitHook commitHook : commitHooks) {
      try {
        commitHook.commit(context, offsets, workPath);
      } catch (Exception ex) {
        log.error("Failed to execute commit hook: " + commitHook.getClass().getCanonicalName(), ex);
        throw new IOException(ex);
      }
    }
    log.debug("Completed commit hooks");

    super.commitTask(context);
  }

  protected void commitFile(JobContext job, Path source, Path target) throws IOException {
    FileSystem.get(job.getConfiguration()).rename(source, target);
  }

  public String getPartitionedPath(JobContext context, String file, int count, long offset) throws IOException {
    Matcher m = workingFileMetadataPattern.matcher(file);
    if (!m.find()) {
      throw new IOException("Could not extract metadata from working filename '" + file + "'");
    }
    String topic = m.group(1);
    String leaderId = m.group(2);
    String partition = m.group(3);
    String encodedPartition = m.group(4);

    String partitionedPath =
        EtlMultiOutputFormat.getPartitioner(context, topic).generatePartitionedPath(context, topic, encodedPartition);

    partitionedPath +=
        "/"
            + EtlMultiOutputFormat.getPartitioner(context, topic).generateFileName(context, topic, leaderId,
                Integer.parseInt(partition), count, offset, encodedPartition);

    return partitionedPath + recordWriterProvider.getFilenameExtension();
  }
}
