package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;

import kafka.api.PartitionFetchInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.linkedin.camus.etl.kafka.CamusJob;

/**
 * Poorly named class that handles kafka pull events within each
 * KafkaRecordReader.
 * 
 * @author Richard Park
 */
public class KafkaReader {
	// index of context
	private EtlRequest kafkaRequest = null;
	private SimpleConsumer simpleConsumer = null;

	private long beginOffset;
	private long currentOffset;
	private long lastOffset;
	private long currentCount;

	private TaskAttemptContext context;

	private Iterator<MessageAndOffset> messageIter = null;

	private long totalFetchTime = 0;
	private long lastFetchTime = 0;

	private int fetchBufferSize;

	/**
	 * Construct using the json represention of the kafka request
	 */
	public KafkaReader(TaskAttemptContext context, EtlRequest request,
			int clientTimeout, int fetchBufferSize) throws Exception {
		this.fetchBufferSize = fetchBufferSize;
		this.context = context;

		System.out.println("bufferSize=" + fetchBufferSize);
		System.out.println("timeout=" + clientTimeout);

		// Create the kafka request from the json

		kafkaRequest = request;

		beginOffset = request.getOffset();
		currentOffset = request.getOffset();
		lastOffset = request.getLastOffset();
		currentCount = 0;
		totalFetchTime = 0;

		// read data from queue

		URI uri = kafkaRequest.getURI();
		simpleConsumer = new SimpleConsumer(uri.getHost(), uri.getPort(),
				CamusJob.getKafkaTimeoutValue(context),
				CamusJob.getKafkaBufferSize(context),
				CamusJob.getKafkaClientName(context));
		System.out.println("Connected to leader " + uri
				+ " beginning reading at offset " + beginOffset
				+ " latest offset=" + lastOffset);
		fetch();
	}

	public boolean hasNext() throws IOException {
		if (messageIter != null && messageIter.hasNext())
			return true;
		else
			return fetch();

	}

	/**
	 * Fetches the next Kafka message and stuffs the results into the key and
	 * value
	 * 
	 * @param key
	 * @param value
	 * @return true if there exists more events
	 * @throws IOException
	 */
	public boolean getNext(EtlKey key, BytesWritable value) throws IOException {
		if (hasNext()) {

			MessageAndOffset msgAndOffset = messageIter.next();
			Message message = msgAndOffset.message();

			ByteBuffer buf = message.payload();
			int origSize = buf.remaining();
			byte[] bytes = new byte[origSize];
			buf.get(bytes, buf.position(), origSize);
			value.set(bytes, 0, origSize);

			key.clear();
			key.set(kafkaRequest.getTopic(), kafkaRequest.getLeaderId(),
					kafkaRequest.getPartition(), currentOffset,
					msgAndOffset.offset(), message.checksum());

			currentOffset = msgAndOffset.offset(); // increase offset
			currentCount++; // increase count

			return true;
		} else {
			return false;
		}
	}

	/**
	 * Creates a fetch request.
	 * 
	 * @return false if there's no more fetches
	 * @throws IOException
	 */

	public boolean fetch() throws IOException {
		if (currentOffset +1 >= lastOffset) {
			return false;
		}
		long tempTime = System.currentTimeMillis();
		TopicAndPartition topicAndPartition = new TopicAndPartition(
				kafkaRequest.getTopic(), kafkaRequest.getPartition());
		System.out.println("\nAsking for offset : " + (currentOffset + 1));
		PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(
				currentOffset + 1, fetchBufferSize);

		HashMap<TopicAndPartition, PartitionFetchInfo> fetchInfo = new HashMap<TopicAndPartition, PartitionFetchInfo>();
		fetchInfo.put(topicAndPartition, partitionFetchInfo);

		FetchRequest fetchRequest = new FetchRequest(
				CamusJob.getKafkaFetchRequestCorrelationId(context),
				CamusJob.getKafkaClientName(context),
				CamusJob.getKafkaFetchRequestMaxWait(context),
				CamusJob.getKafkaFetchRequestMinBytes(context), fetchInfo);

		FetchResponse fetchResponse = null;
		try {
			fetchResponse = simpleConsumer.fetch(fetchRequest);
			if (fetchResponse.hasError()) {
				System.out
						.println("Error encountered during a fetch request from Kafka");
				System.out.println("Error Code generated : "
						+ fetchResponse.errorCode(kafkaRequest.getTopic(),
								kafkaRequest.getPartition()));
				return false;
			} else {
				ByteBufferMessageSet messageBuffer = fetchResponse.messageSet(
						kafkaRequest.getTopic(), kafkaRequest.getPartition());
				lastFetchTime = (System.currentTimeMillis() - tempTime);
				System.out.println("Time taken to fetch : "
						+ (lastFetchTime / 1000) + " seconds");
				int skipped = 0;
				totalFetchTime += lastFetchTime;
				messageIter = messageBuffer.iterator();
				boolean flag = false;
				Iterator<MessageAndOffset> messageIter2 = messageBuffer
						.iterator();
				MessageAndOffset message = null;
				while (messageIter2.hasNext()) {
					message = messageIter2.next();
					if (message.offset() < currentOffset) {
						flag = true;
						skipped++;
					} else {
						System.out.println("Skipped offsets till : "
								+ message.offset());
						break;
					}
				}
				System.out.println("Number of skipped offsets : " + skipped);
				if (!messageIter2.hasNext()) {
					System.out
							.println("No more data left to process. Returning false");
					messageIter = null;
					return false;
				}
				if (flag) {
					messageIter = messageIter2;
				}
				return true;
			}
		} catch (Exception e) {
			System.out.println("Exception generated during fetch");
			e.printStackTrace();
			return false;
		}

	}

	/**
	 * Closes this context
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		if (simpleConsumer != null) {
			simpleConsumer.close();
		}
	}

	/**
	 * Returns the total bytes that will be fetched. This is calculated by
	 * taking the diffs of the offsets
	 * 
	 * @return
	 */
	public long getTotalBytes() {
		return (lastOffset > beginOffset) ? lastOffset - beginOffset : 0;
	}

	/**
	 * Returns the total bytes that have been fetched so far
	 * 
	 * @return
	 */
	public long getReadBytes() {
		return currentOffset - beginOffset;
	}

	/**
	 * Returns the number of events that have been read r
	 * 
	 * @return
	 */
	public long getCount() {
		return currentCount;
	}

	/**
	 * Returns the fetch time of the last fetch in ms
	 * 
	 * @return
	 */
	public long getFetchTime() {
		return lastFetchTime;
	}

	/**
	 * Returns the totalFetchTime in ms
	 * 
	 * @return
	 */
	public long getTotalFetchTime() {
		return totalFetchTime;
	}
}
