package com.linkedin.camus.etl.kafka.utils;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * http://stackoverflow.com/questions/9539845/java-how-to-handle-retries-without-copy-paste-code
 * 
 * Generic Retry with Any exception try again and again.
 * 
 */
public class RetryLogic<T> {
	
	public static interface Delegate<T> {
		T call() throws Exception;
	}
   
	private static final Logger logger = Logger.getLogger(RetryLogic.class);
	
	private int maxAttempts;
	private long retryWaitInMs;
	private T expectedResult;

	public RetryLogic(int maxAttempts, long retryWait,TimeUnit timeUnit, T expectedResult) {
		this.maxAttempts = maxAttempts;
		if(TimeUnit.MILLISECONDS != timeUnit){
			this.retryWaitInMs = timeUnit.toMillis(retryWait);
		}else{
			this.retryWaitInMs = retryWait;
		}
		this.expectedResult = expectedResult;
	}

	public T getResultWithRetry(Delegate<T> caller) throws RetryExhausted {
		T result = null;
		int remainingAttempts = maxAttempts;
		RetryExhausted retryExcep = new RetryExhausted(caller.getClass() + " Failed After " + maxAttempts + " retry.");
		while(true){
			try {
				remainingAttempts--;
				result = caller.call();
				// covers the null case....
				if(result == expectedResult){
					return result;
				// is what you expect result to be...
				}else if(expectedResult != null && expectedResult.equals(result)){
					return result;
				}else if(  (expectedResult == null && result != null) || (expectedResult != null && !expectedResult.equals(result))){
					// retry when except result does not match up...
					logger.warn("WARN Retrying again because expected result does not match actual result after calling so wait for retryWaitInMs="+retryWaitInMs +" retryLeft="+remainingAttempts);
				}else{
					String msg = "Expected and result condition missing expectedResult=" + (expectedResult == null?"null":expectedResult.toString()) +" result="+(result == null?"null":result.toString()); 
					logger.warn(msg);
					throw new RuntimeException(msg);
				}
			}catch (Throwable e) {
				// what should be do if we get interupt signal...
				if(e instanceof InterruptedException){
					logger.warn("Thread="+Thread.currentThread().getName() +" was interupted while retry...so exiting passing on the signal...");
					Thread.interrupted();
				}else{
					logger.warn("WARN Retrying again due to fatal erorr",e);
					retryExcep.addException(e);
				}
			}			
			if (remainingAttempts <= 0) {
				if(!retryExcep.hasException()){
					logger.warn("Could not get expected result after "+maxAttempts+ " attempt(s) ");
					//return last...
					return result;
				}else{				
					logger.error("ERROR cannot recover from the fatal error after " +maxAttempts+ " attempt(s) hasException="+retryExcep.hasException());
					throw retryExcep;
				}
			} else {
				// else retry immediately 
				if(retryWaitInMs > 0){
					try {
						Thread.sleep(retryWaitInMs);
					} catch (InterruptedException ie) {
						logger.error("while we are waiting this thread was interupted...so passing on the interupt..");
						Thread.interrupted();
					}
				}
			}
		}
	}
	
}