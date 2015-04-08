package com.linkedin.camus.etl.kafka.utils.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.linkedin.camus.etl.kafka.utils.RetryLogic;

public class DirDeleteTask implements RetryLogic.Delegate<Boolean> {

	private static final Logger log = Logger.getLogger(DirDeleteTask.class);

	
	private FileSystem fs;
	private Path path;
	
	DirDeleteTask(FileSystem fs,Path path ){
		if(fs == null || path == null){
			throw new NullPointerException("FileSystem object fs or Path path cannot be null!");
		} 
		this.fs = fs ;
		this.path = path;
	}
	
	@Override
	public Boolean call() throws Exception {		
		boolean result = false;
		boolean dirExists = fs.exists(path);
		if(dirExists){
			log.info("Path :" + path.toString() + "is marked for deletion.");
			result = fs.delete(path, true);
		}else {
			// file does not exits delete it..
			result = true;
		}
		return result;
	}

}
