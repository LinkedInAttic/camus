package com.linkedin.camus.etl.kafka.utils;

import java.util.ArrayList;
import java.util.List;

public class RetryExhausted extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5120068860833652540L;
	
	private transient List<Throwable> listofThrowables = new ArrayList<Throwable>();
	
	public RetryExhausted(String message) {
	}
	
	public void addException(Throwable th){
		listofThrowables.add(th);
	}
	
	public boolean hasException(){
		return !listofThrowables.isEmpty();
	}
	
}
