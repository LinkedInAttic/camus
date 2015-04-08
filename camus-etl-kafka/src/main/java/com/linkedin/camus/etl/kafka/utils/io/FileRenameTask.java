package com.linkedin.camus.etl.kafka.utils.io;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.linkedin.camus.etl.kafka.utils.RetryLogic;

public class FileRenameTask implements RetryLogic.Delegate<Boolean>{
	
	
	private Logger log = Logger.getLogger(FileRenameTask.class);
	
	private FileSystem fs;
	private Path soruce;
	private Path target;
	
	
	
	private boolean isFirstCall = true;
	private boolean fileAlreadyExits = false;

	public FileRenameTask(FileSystem fs,Path source, Path target ){
		if(fs == null){
			throw new NullPointerException("FileSystem  fs cannot be null");
		} 
		if(source == null){
			throw new NullPointerException("source cannot be null");
		}
		if(target == null){
			throw new NullPointerException("target cannot be null");
		}		
		this.fs = fs ;
		this.soruce = source;
		this.target = target;
	}
	
	
	/**
	 * Any IO Exception retry will happen...
	 */
	@Override
	public Boolean call() throws Exception {
		boolean result = false;
        // check if file exits...
		if(fs.exists(soruce)){
			boolean fileExists = fs.exists(target);
			if( fileExists && isFirstCall){
				isFirstCall = false;
				String message = "Cannot rename File target File["+target.toString()+"] already exits final destination path."; 
				fileAlreadyExits = true;
				throw new IOException(message);
			}
			// not first call any more...
			isFirstCall = false;
			// first time file does not exist but may be it does exist after FS failure while renaming...
			if(!fileAlreadyExits){
				//try to rename it ...
				result = fs.rename(soruce,target);
				if(!result){
					String msg = "Failed to commit the file to final destination ="+target.toString() + " and temp path="+soruce.toString();
					log.error(msg);
					result =false;
					throw new IOException(msg);
				}
			}			
		}
		return result;
	}

}
