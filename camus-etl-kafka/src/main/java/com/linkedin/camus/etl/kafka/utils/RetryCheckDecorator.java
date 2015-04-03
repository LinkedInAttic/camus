package com.linkedin.camus.etl.kafka.utils;

public class RetryCheckDecorator<T> implements RetryLogic.Delegate<T>{

	private int counter = 0;
	private RetryLogic.Delegate<T> target;

	public RetryCheckDecorator(RetryLogic.Delegate<T> target){
		if(target == null){
			throw new NullPointerException("Target Invocation Object cannot be null");
		}
		this.target = target;
	}
	
	@Override
	public T call() throws Exception {
		counter++;
		return target.call();
	}
	
	public int invokeCount(){
		return counter;
	}
}
