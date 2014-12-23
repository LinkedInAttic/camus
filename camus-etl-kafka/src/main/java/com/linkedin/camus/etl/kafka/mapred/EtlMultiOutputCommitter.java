package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.etl.Partitioner;
import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.EtlCounts;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class EtlMultiOutputCommitter extends FileOutputCommitter {
  public static final String ETL_COMMIT_HOOKS = "etl.commit.hooks";
  private final List<CommitHook> commitHooks = new ArrayList<CommitHook>();
  private Pattern workingFileMetadataPattern;

  private HashMap<String, EtlCounts> counts = new HashMap<String, EtlCounts>();
  private HashMap<String, EtlKey> offsets = new HashMap<String, EtlKey>();
  private HashMap<String, Long> eventCounts = new HashMap<String, Long>();
  private Map<String, Partitioner> partitionersByTopic;

  private TaskAttemptContext context;
  private final RecordWriterProvider recordWriterProvider;
  private Logger log;

  public EtlMultiOutputCommitter(Path outputPath, TaskAttemptContext context, Logger log, Map<String, Partitioner> partitionersByTopic)
          throws IOException {
    super(outputPath, context);
    this.context = context;
    this.partitionersByTopic = partitionersByTopic;
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

    try {
      addCommitHooks(context, outputPath);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void addCommitHooks(TaskAttemptContext context, Path outputPath) throws IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
    Path workPath = super.getWorkPath();
    log.debug("Attaching commit hooks");
    log.debug("Attaching sequence file offset writer commit hook");
    commitHooks.add(new SequenceFileOffsetCommitter(context, workPath, outputPath));

    log.debug("Adding user-defined commit hooks");
    for (String hook : context.getConfiguration().getStringCollection(ETL_COMMIT_HOOKS)) {
      log.info("Adding commit hook: " + hook);
      Class<CommitHook> klass = (Class<CommitHook>) Class.forName(hook);
      Constructor<CommitHook> constructor = klass.getConstructor(TaskAttemptContext.class, Path.class, Path.class, Logger.class);
      CommitHook commitHook = constructor.newInstance(context, workPath, outputPath, log);
      commitHooks.add(commitHook);
    }

    log.debug("All commit hooks added");
  }

  private void mkdirs(FileSystem fs, Path path) throws IOException {
    if (!fs.exists(path.getParent())) {
      mkdirs(fs, path.getParent());
    }
    fs.mkdirs(path);
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


  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {

    ArrayList<Map<String, Object>> allCountObject = new ArrayList<Map<String, Object>>();
    FileSystem fs = FileSystem.get(context.getConfiguration());
    if (EtlMultiOutputFormat.isRunMoveData(context)) {
      Path workPath = super.getWorkPath();
      log.info("work path: " + workPath);
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
        commitHook.commit(offsets);
      } catch (Exception ex) {
        log.error("Failed to execute commit hook: " + commitHook.getClass().getCanonicalName(), ex);
        throw new IOException(ex);
      }
    }
    log.info("Completed commit hooks");

    recordProcessedPartitions(context);

    super.commitTask(context);
  }

  private void recordProcessedPartitions(TaskAttemptContext context) throws IOException {
    for (Partitioner p : partitionersByTopic.values()) {
      p.recordProcessedPartitions(context, super.getWorkPath());
    }
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
