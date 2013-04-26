package com.linkedin.camus.etl.kafka.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.linkedin.camus.etl.kafka.common.EtlRequest;

public class EtlSplit extends InputSplit implements Writable {
	private List<EtlRequest> requests = new ArrayList<EtlRequest>();
	private long length = 0;

	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		for (int i = 0; i < size; i++) {
			EtlRequest r = new EtlRequest();
			r.readFields(in);
			requests.add(r);
			length += r.estimateDataSize();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(requests.size());
		for (EtlRequest r : requests)
			r.write(out);
	}

	@Override
	public long getLength() throws IOException {
		return length;
	}

	public int getNumRequests() {
		return requests.size();
	}

	@Override
	public String[] getLocations() throws IOException {
		return new String[] {};
	}

	public void addRequest(EtlRequest request) {
		requests.add(request);
		length += request.estimateDataSize();
	}

	public EtlRequest popRequest() {
		if (requests.size() > 0)
			return requests.remove(0);
		else
			return null;
	}
}
