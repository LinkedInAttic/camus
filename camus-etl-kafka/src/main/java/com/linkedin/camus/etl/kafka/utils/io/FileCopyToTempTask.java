package com.linkedin.camus.etl.kafka.utils.io;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.linkedin.camus.etl.kafka.utils.RetryLogic;

public final class FileCopyToTempTask implements RetryLogic.Delegate<Boolean> {
	
	private static final Logger log = Logger.getLogger(FileCopyToTempTask.class);
	
	private String tempLocation;
	private Path source;
	private FileSystem fs;
	private Path tempFilePath;

	FileCopyToTempTask(String tempLocation, Path source, FileSystem fs){
		this.tempLocation = tempLocation;
		this.source = source;
		this.fs = fs;
	}
	
	@Override
	public Boolean call(){
		Path destinationTemp = new Path(tempLocation + "/" + source.getName());
		boolean result = false;
		int retry = 3;
		// for ANY fatal underlying FS error try 3 more times... 
		// try to create a temp directory try 3 times if there is any failure then do not try...
		while(retry > 0 && !result){
			Path origDestinationTemp = new Path(destinationTemp.toString());
			try{
				result = FileUtil.copy(fs, source, fs,
						destinationTemp, false, fs.getConf());
				tempFilePath = destinationTemp;
				
				if(!result){
					log.error("Failed rename to temp destination Path so delete it : " + destinationTemp.toString());
					// if result is failure then lets delete it now...
					boolean deleted = fs.delete(destinationTemp,true);
					if(!deleted){
						// let see if hadoop framework can delete this..otherwise this will be garbage sitting here for ever and ever...TODO ????
						fs.deleteOnExit(destinationTemp);
						// try new path...
						destinationTemp = new Path(destinationTemp.toString() + "-"+retry);
					}
				}
			}catch(IOException e){
				log.error("Error while copy to temp location from=" + source.toString() +" and to=" + origDestinationTemp.toString(),e);
				try{
					// try to close file and delete it... TODO Review this....
					fs.create(origDestinationTemp,true).close();
					fs.delete(origDestinationTemp, true);
				}catch(Throwable th){
					log.error("Error delete while copy to temp location from=" + source.toString() +" and to=" + origDestinationTemp.toString(),e);								}
				
			}
		}
		return result;
	}
	
	public Path getTempPath(){
		return tempFilePath;
	}

}
