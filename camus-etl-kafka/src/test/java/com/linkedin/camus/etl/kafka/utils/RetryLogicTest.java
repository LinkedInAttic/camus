package com.linkedin.camus.etl.kafka.utils;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class RetryLogicTest {
	

	@Test
	public void testGetResultWithRetryFailureRetryExhausted() {
		
		RetryLogic<Boolean> retryLogic = new 
				RetryLogic<Boolean>(3, 1, TimeUnit.SECONDS, Boolean.TRUE);
		boolean gotRetryExhausted = false;
		long start = System.currentTimeMillis();
		long end = 0;
		TestFailureDueToException test  = new TestFailureDueToException();
		CounterWrapper<Boolean> counter = new CounterWrapper<Boolean>(test);
		try {
			retryLogic.getResultWithRetry(counter);
		} catch (RetryExhausted e) {
			end = System.currentTimeMillis();
			gotRetryExhausted = true;
		}
		Assert.assertTrue(gotRetryExhausted);	
		Assert.assertTrue(end > 0);	
		//must wait for 2 sec total  between 3 retry ....
		Assert.assertTrue(TimeUnit.MILLISECONDS.toSeconds(end-start) >= 2 );
		Assert.assertTrue(counter.getCount() == 3);
	}

	@Test
	public void testGetResultWithRetryFailureRetryExhaustedDueToUnexpectedResult() {
		
		RetryLogic<Boolean> retryLogic = new 
				RetryLogic<Boolean>(3, 1, TimeUnit.SECONDS, Boolean.TRUE);
		boolean gotRetryExhausted = false;
		long start = System.currentTimeMillis();
		long end = 0;
		TestFailureDueToExceptionUnExpectedResult test  = new TestFailureDueToExceptionUnExpectedResult();

		CounterWrapper<Boolean> counter = new CounterWrapper<Boolean>(test);
		Boolean result = null;
		try {
			result = retryLogic.getResultWithRetry(counter);
			end = System.currentTimeMillis();
		} catch (RetryExhausted e) {
			gotRetryExhausted = true;
			Assert.assertTrue(!e.hasException());
		}
		Assert.assertTrue(!gotRetryExhausted);	
		Assert.assertFalse(result);	
		Assert.assertTrue(end > 0);	
		//must wait for 2 sec total  between 3 retry ....
		Assert.assertTrue(TimeUnit.MILLISECONDS.toSeconds(end-start) >= 2 );
	}
	

	@Test
	public void testGetResultWithRetryHappyCases() {
		
		RetryLogic<Boolean> retryLogic = new 
				RetryLogic<Boolean>(3, 1, TimeUnit.SECONDS, Boolean.TRUE);
		boolean gotRetryExhausted = false;
		long start = System.currentTimeMillis();
		long end = 0;
		TestHappyResult test  = new TestHappyResult();

		CounterWrapper<Boolean> counter = new CounterWrapper<Boolean>(test);
		try {
			retryLogic.getResultWithRetry(counter);
			end = System.currentTimeMillis();
		} catch (RetryExhausted e) {
			gotRetryExhausted = true;
		}
		Assert.assertFalse(gotRetryExhausted);	
		Assert.assertTrue(end > 0);	
		//must wait for 2 sec total  between 3 retry ....
		Assert.assertTrue(TimeUnit.MILLISECONDS.toSeconds(end-start) < 2 );
		Assert.assertTrue(counter.getCount() == 1);
		
		TestHappyResultAfterFailure test2  = new TestHappyResultAfterFailure();
		CounterWrapper<Boolean> counter2 = new CounterWrapper<Boolean>(test2);
		try {
			start = System.currentTimeMillis();
			retryLogic.getResultWithRetry(counter2);
			end = System.currentTimeMillis();
		} catch (RetryExhausted e) {
			gotRetryExhausted = true;
		}
		Assert.assertFalse(gotRetryExhausted);	
		Assert.assertTrue(end > 0);	
		//must wait for 2 sec total  between 3 retry ....
		Assert.assertTrue(TimeUnit.MILLISECONDS.toSeconds(end-start) <= 2 );
		Assert.assertTrue(counter2.getCount() == 2);		
	}
	
	
	
	static class CounterWrapper<T> implements RetryLogic.Delegate<T> {
		int count = 0;
		RetryLogic.Delegate<T> test;
		CounterWrapper(RetryLogic.Delegate<T> test){
			this.test = test;
		}
		
		@Override
		public T call() throws Exception {
			count++;
			return test.call();
		}
		
		public int getCount(){
			return count;
		}
	}	
	
	
	
	static class TestFailureDueToException implements RetryLogic.Delegate<Boolean> {
		@Override
		public Boolean call() {
			throw new RuntimeException("Failed Operation..all time");
		}
		
	}
	static class TestFailureDueToExceptionUnExpectedResult implements RetryLogic.Delegate<Boolean> {
		@Override
		public Boolean call() {
			return Boolean.FALSE;
		}
	}
	
	static class TestHappyResult implements RetryLogic.Delegate<Boolean> {
		@Override
		public Boolean call() {
			return Boolean.TRUE;
		}
	}
	static class TestHappyResultAfterFailure implements RetryLogic.Delegate<Boolean> {
		private boolean isFirstOne = true;
		@Override
		public Boolean call() {
			
			if(isFirstOne){
				isFirstOne = false;
				throw new RuntimeException("First TIme Failure..");
			}
			return Boolean.TRUE;
		}
	}
}
