package com.linkedin.camus.etl;

import org.apache.hadoop.mapreduce.RecordWriter;


/**
 * This is writer interface added to check if underlying writer is closed or not. 
 *
 * @param <K>  key 
 * @param <V>  value
 */
public abstract class RecordWriterWithCloseStatus<K, V> extends RecordWriter<K, V>{
	/**
	 * Give Ability to check if close has been called on the writer or File has been closed on not..
	 * @return
	 */
	public abstract boolean isClose();
	
}