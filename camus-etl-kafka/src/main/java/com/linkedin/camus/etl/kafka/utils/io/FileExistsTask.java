package com.linkedin.camus.etl.kafka.utils.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.linkedin.camus.etl.kafka.utils.RetryLogic;

public final class FileExistsTask implements RetryLogic.Delegate<Boolean> {
	
	private static final Logger log = Logger.getLogger(FileExistsTask.class);

	
	private FileSystem fs;
	private Path path;
	
	public FileExistsTask(FileSystem fs,Path path ){
		if(fs == null || path == null){
			throw new NullPointerException("FileSystem object fs or Path path cannot be null!");
		} 
		this.fs = fs ;
		this.path = path;
	}

	@Override
	public Boolean call() throws Exception {
		boolean result = fs.exists(path);
		log.info("Checking file if file Path exists["+result+"]:" + path.toString());
		return result;
	}

}
