package com.linkedin.camus.etl.kafka.mapred;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.camus.etl.RecordWriterProvider;
import com.linkedin.camus.etl.kafka.common.EtlCounts;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.utils.RetryExhausted;
import com.linkedin.camus.etl.kafka.utils.RetryLogic;
import com.linkedin.camus.etl.kafka.utils.io.FileCommitUtil;
import com.linkedin.camus.etl.kafka.utils.io.FileExistsTask;
import com.linkedin.camus.etl.kafka.utils.io.FileRenameTask;


public class EtlMultiOutputCommitter extends FileOutputCommitter {
	
  private Pattern workingFileMetadataPattern;

  private HashMap<String, EtlCounts> counts = new HashMap<String, EtlCounts>();
  private HashMap<String, EtlKey> offsets = new HashMap<String, EtlKey>();
  private HashMap<String, Long> eventCounts = new HashMap<String, Long>();

  private TaskAttemptContext context;
  private final RecordWriterProvider recordWriterProvider;
  private Logger log;
  
  ///TODO make this configurable..
  private static final int MAX_RETRY = 3 ;
  private static final int MAX_WAIT_IN_SEC =1 ;
  
  private void mkdirs(FileSystem fs, Path path) throws IOException {
    if (!fs.exists(path)) {
    	// removed the recursive call here and upon failure throw IOException (Count retry exists and mkdirs with retry..)
	    boolean result = fs.mkdirs(path);
	    if(!result){
	    	String msg = "Unable to create directory: " + path.toString();
	    	log.error(msg);
	    	throw new IOException(msg);
	    }
    }
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

  public EtlMultiOutputCommitter(Path outputPath, TaskAttemptContext context, Logger log) throws IOException {
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
          count.setEndTime(System.currentTimeMillis());

          String partitionedFile =
              getPartitionedPath(context, file, count.getEventCount(), count.getLastKey().getOffset());

          Path dest = new Path(baseOutDir, partitionedFile);
          
          // TODO Fix up the here as well why Re-cursive calls...
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

    SequenceFile.Writer offsetWriter =
        SequenceFile.createWriter(
            fs,
            context.getConfiguration(),
            new Path(super.getWorkPath(), EtlMultiOutputFormat.getUniqueFile(context,
                EtlMultiOutputFormat.OFFSET_PREFIX, "")), EtlKey.class, NullWritable.class);
    for (String s : offsets.keySet()) {
      offsetWriter.append(offsets.get(s), NullWritable.get());
    }
    offsetWriter.close();
    super.commitTask(context);
  }

	protected void commitFile(final JobContext job, final Path source, final Path target) throws IOException{
		
		// This is new approch...
		if(EtlMultiOutputFormat.isCopyAndRenameFinalDestination(job)){
			FileCommitUtil.commitWithCopyAndRenameFile(job, source, target, log);
		}else {
			// This is default behavior out of box adding more 
			// should we retry if rename fails here and remove the data...
			
			/**
			 * Ideally, this should check if target file is there or not if it is there than we should move or not based on 
			 * configuration otherwise we have major problem data overridden by this command.
			 */
			final FileSystem fs = FileSystem.get(job.getConfiguration());
			final RetryLogic<Boolean> retryWithTrue = new RetryLogic<Boolean>( MAX_RETRY, MAX_WAIT_IN_SEC,TimeUnit.SECONDS, Boolean.TRUE);
			final FileRenameTask renameTask = new FileRenameTask(fs, source, target);
			try{
				// check if file can be overridden if the destination path is there or not..
				if(!EtlMultiOutputFormat.isFinalDestinationFileOverwriteOn(job)){
					final FileExistsTask fileExists = new FileExistsTask(fs, target);
					final RetryLogic<Boolean> retryWithFalse = new RetryLogic<Boolean>( MAX_RETRY, MAX_WAIT_IN_SEC,TimeUnit.SECONDS, Boolean.FALSE);
					try {
						boolean isFileFound  = retryWithFalse.getResultWithRetry(fileExists);
						if(isFileFound){
							throw new IOException("Target File="+target.toString() +" already exists and " + EtlMultiOutputFormat.ETL_FINAL_DESTINATION_FILE_OVERWRITE +" is OFF. Hence, the erorr message thrown to prevent data loss and overwrite.");
						}
					} catch (RetryExhausted e) {
						String msg = "Target File="+target.toString() +" exists could failed and " + EtlMultiOutputFormat.ETL_FINAL_DESTINATION_FILE_OVERWRITE +" is OFF. "
									+ "Hence, the erorr message thrown to prevent data loss and overwrite.";
						throw new IOException(msg,e);
					}
				}
				
				boolean result = retryWithTrue.getResultWithRetry(renameTask);
				
				if(!result){
				    /*
				     * We do not want silent failure...:  should we do the Md5 or CRC checksum ..?
				     *  Based on feedback please refer to https://github.com/linkedin/camus/issues/189
				     *  We do not throw exception data will be lost..
				     */					
					String msg = "Could not rename a File during Commit Phase due to underlying File System reutrn false result: soruce=" + source.toString() +" to target=" + target.toString();
					log.error(msg);
					throw new IOException(msg);
				}
			}catch(RetryExhausted e){
				String msg = "File Rename Failed from= " + source.getName() +" to=" + target.getName() + " because it failed " +MAX_RETRY+" times";
				log.error(msg, e);
				throw new IOException(msg,e);
			}
			catch(Throwable e){
				log.error("File Rename Failed from= " + source.getName() +" to=" + target.getName() , e);
				if(e instanceof IOException){
					throw (IOException)e;
				}else {
					throw new RuntimeException("Default Behavior file commit failed due to fatal problem",e);
				}
			}
		}
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
