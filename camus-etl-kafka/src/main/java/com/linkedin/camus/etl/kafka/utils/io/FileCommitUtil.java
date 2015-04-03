package com.linkedin.camus.etl.kafka.utils.io;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import com.linkedin.camus.etl.kafka.utils.RetryExhausted;
import com.linkedin.camus.etl.kafka.utils.RetryLogic;

/**
 * Custom code to commit the file across volumes as suggested by the Peter Newcomb...
 *  We also wanted to preserve the so we need attempted task id as well... hence this will be only called when task that is needs to be committed.
 *  mapred.map.tasks.speculative.execution	true	If true, then multiple instances of some map tasks may be executed in parallel for safe reason add "taskid to temp"
 * 
 *  user volume/tmp  (volume 1)  to /raw/logmon (volume 2) and then rename file to final target location (/raw/logmon/...)
 *  can not remove /_temporary as other camus job may write to it so delete it till taskid.
 *  
 *  This is very inefficient solution (Kakfa ->copy to temp user volume-> copy to temp day volume ->rename to final destination.
 *  This will increase the latency of Camus job, Hence, lags is expected to go high.
 *  
 */
public final class FileCommitUtil {

	//TODO have this configurable 
	private static final int MAX_RETRY = 3;
	private static final int WAIT_SECONDS = 1;
	
	public static void commitWithCopyAndRenameFile(final JobContext job,
			final Path source, final Path target, Logger log) throws IOException{

		CamusJob.startTiming("copy-rename-commit");
		
		log.info("Using the Two Steps Process Copy to Temp and then rename to target.");
		TaskAttemptContext context = (TaskAttemptContext) job;
		long startTime = System.currentTimeMillis();
		final FileSystem fs = FileSystem.get(job.getConfiguration());
	    // now create a temp directory for this task file to copy..
		Path parentpath = target.getParent();
		// lets create a temp directory....
		final TaskAttemptContext taskContext = (TaskAttemptContext) job;
		final String tempLocation = parentpath.toString() + "/_temporary_"+taskContext.getTaskAttemptID().toString() +"/";
		final Path tempPath = new Path(tempLocation);
		// create retry logic for all file command 
		RetryLogic<Boolean> retryWithTrueExpected = new RetryLogic<Boolean>(MAX_RETRY, WAIT_SECONDS, TimeUnit.SECONDS,Boolean.TRUE);
		
		boolean shouldCleanupTemp = false;
		try{
				log.info("Starting copy-rename-commit at " +  new Date());	
				/**
				 * Ideally, this should check if target file is there or not if it is there than we should move or not based on 
				 * configuration otherwise we have major problem data overridden by this command.
				 */
				// check if file can be overridden if the destination path is there or not..
				if(!EtlMultiOutputFormat.isFinalDestinationFileOverwriteOn(job)){
					final FileExistsTask fileExists = new FileExistsTask(fs, target);
					final RetryLogic<Boolean> retryWithFalse = new RetryLogic<Boolean>( MAX_RETRY, WAIT_SECONDS,TimeUnit.SECONDS, Boolean.FALSE);
					try {							
						boolean isFileFound  = retryWithFalse.getResultWithRetry(fileExists);
						if(isFileFound){
							throw new IOException("Target File="+target.toString() +" already exists and " + EtlMultiOutputFormat.ETL_FINAL_DESTINATION_FILE_OVERWRITE +" is OFF. Hence, the erorr message thrown to prevent data loss and overwrite file.");
						}
					} catch (RetryExhausted e) {
						String msg = "Target File="+target.toString() +" exists check failed and " + EtlMultiOutputFormat.ETL_FINAL_DESTINATION_FILE_OVERWRITE +" is OFF. "
									+ "Hence, the erorr message thrown to prevent data loss and overwrite.";
						throw new IOException(msg,e);
					}
				}
				
				FileMakeDirTask makeTempDir = new FileMakeDirTask(fs,tempPath);
					
				boolean tempDirectoryCreated = false;
				try {
					tempDirectoryCreated = retryWithTrueExpected.getResultWithRetry(makeTempDir);
				} catch (Exception e) {
					log.error("Could not commit file due to error exit....",e);
					// this will cause entire Map task to fail so we can start from last offset again  three for no data loss ???
					throw new RuntimeException("Temp Folder Creation failed...",e);
				}
				
				if(tempDirectoryCreated){
					shouldCleanupTemp = tempDirectoryCreated;
					//now try to move this file to temp folder...
					
					try {
						final FileCopyToTempTask copyToTempDir = new FileCopyToTempTask(tempLocation, source, fs);
						// now we have copied to respective volume so let try to rename it....this should not fail at all ....
						final boolean copyToTempFolderFile = retryWithTrueExpected.getResultWithRetry(copyToTempDir);
						if(copyToTempFolderFile && copyToTempDir.getTempPath() !=  null){					
							FileRenameTask renameFromTempToFinalDestination =  new FileRenameTask(fs, copyToTempDir.getTempPath(), target);
							boolean  finalResult = retryWithTrueExpected.getResultWithRetry(renameFromTempToFinalDestination);
							if(finalResult){
								// do not do retry here....
								// it is ok if it does not get deleted since we have directory clean up will take care of it...
								boolean delte = fs.delete(source,true);
								if(!delte){
									// last try to delete it with Hadoop M/R framework...
									fs.deleteOnExit(source);
								}
							}else{
								String message =  "File rename failed at this movement due to underlying FileSystem rename return false. Error is thrown so offset does not advance, hence no data loss.\n"+
										  "source=" + source.toString() + " \n" +
										  "final target= " + target.toString() + "\n";
						
								log.error(message);
								throw new RuntimeException(message);
							}
						}
					} catch (Exception e) {
						if(e instanceof InterruptedException){
							log.error("Giving up commit rename since this tread was inturupted: " + source.toString() +" and to=" + tempLocation,e);
							Thread.currentThread().interrupt();
						}else if(e instanceof RuntimeException){
							log.error("Giving up commit rename since the copy failed: " + source.toString() +" and to=" + tempLocation,e);
							throw (RuntimeException)e;
						// any this expected result or IOException we must throw exception..
						}else if (e instanceof RetryExhausted){
							String message =  "File can not be commited at this movement so lets try next time due to underlying FileSystem erorr. Error is thrown so offset does not advance, hence no data loss.\n"+
									  "source=" + source.toString() + " \n" +
									  "final target= " + target.toString() + "\n";
					
							log.error(message);
							throw new RuntimeException(message,e); 
						}else{
							log.error("Giving up commit rename since the copy failed: " + source.toString() +" and to=" + tempLocation,e);
							throw new RuntimeException("Giving up commit since the copy failed" + source.toString() +" and to=" + tempLocation,e);
						}
					}
				}
		
		}finally{
			// finally delete the temp location....
			 //partition_month_utc=2015-03/partition_day_utc=2015-03-11/partition_minute_bucket=2015-03-11-02-09/_temporary_attempt_201503102148_0007_m_000017_0
			//if temp dir creation was successful then try to delete it..
			if(shouldCleanupTemp){
				try {
					DirDeleteTask task = new DirDeleteTask(fs,tempPath);
					boolean result = retryWithTrueExpected.getResultWithRetry(task);
					if(!result){
						boolean tempLocationDel =  fs.delete(tempPath, true);
						if(!tempLocationDel){
							// give second chance for the deleting the tempDir...by the hadoop framework if it failed......
							log.error("Filed to clean up temp dir=" + tempPath.toString());
							fs.deleteOnExit(tempPath);
						}
					}
				} catch (RetryExhausted e) {
					log.error("Filed to clean up temp directory in finally block tempdir=" + tempPath.toString());
				}catch(Throwable th){
					log.error("FATAL ERORR while delete finally block tempdir=" + tempPath.toString());
				}
			}
			CamusJob.stopTiming("copy-rename-commit");
			long duration = System.currentTimeMillis() - startTime;
			log.info("Finish copy-rename-commit at " +  new Date() + " and duration (ms) is " + duration );	
			updateCommitCounter(context,log,duration);
		}		
	}
	
	/**
	 * Added Counter for this Copy-Task for Hadoop 1.0 and Hadoop 2.0 task.
	 * @param context
	 * @param log
	 * @param value
	 */
	private static void updateCommitCounter(TaskAttemptContext context, Logger log, long value) {
	    try {
	      //In Hadoop 2, TaskAttemptContext.getCounter() is available
	      Method getCounterMethod = context.getClass().getMethod("getCounter", String.class, String.class);
	      Counter counter = ((Counter) getCounterMethod.invoke(context, "total", "copy-rename-commit"));
	      counter.increment(value);
	    } catch (NoSuchMethodException e) {
	      //In Hadoop 1, TaskAttemptContext.getCounter() is not available
	      //Have to cast context to TaskAttemptContext in the mapred package, then get a StatusReporter instance
	      org.apache.hadoop.mapred.TaskAttemptContext mapredContext = (org.apache.hadoop.mapred.TaskAttemptContext) context;
	      org.apache.hadoop.mapreduce.Counter counter = ((StatusReporter) mapredContext.getProgressible()).getCounter("total", "copy-rename-commit");
	      counter.increment(value);
	    } catch (IllegalArgumentException e) {
	      log.error("IllegalArgumentException while obtaining counter 'total:copy-rename-commit': " + e.getMessage());
	      throw new RuntimeException(e);
	    } catch (IllegalAccessException e) {
	      log.error("IllegalAccessException while obtaining counter 'total:copy-rename-commit': " + e.getMessage());
	      throw new RuntimeException(e);
	    } catch (InvocationTargetException e) {
	      log.error("InvocationTargetException obtaining counter 'total:copy-rename-commit': " + e.getMessage());
	      throw new RuntimeException(e);
	    }
	  }	
}