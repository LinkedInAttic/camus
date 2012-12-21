package com.linkedin.batch.etl.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.batch.etl.kafka.common.DateUtils;
import com.linkedin.batch.etl.kafka.common.EtlKey;
import com.linkedin.batch.etl.kafka.common.KafkaETLKey;

public class ConvertOldExecutions {

	private static final String TOPIC_KEY = "topic";
	private static final String NODE_KEY = "node";
	private static final String PARTITION_KEY = "partition";
	private static final String OFFSET_KEY = "offset";
	private static KafkaETLKey key = new KafkaETLKey();
	private static BytesWritable offsetBW = new BytesWritable();
	private static ArrayList<EtlKey> offsets = new ArrayList<EtlKey>();

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("hadoop.job.ugi", "kafkaetl,dfsload");
		conf.set("dfs.umask", "002");

		FileSystem fs = FileSystem.get(conf);

		for (FileStatus f : fs.listStatus(new Path("/data/tracking"))) {
			FileStatus[] stats = fs.globStatus(new Path(f.getPath(), "/hourly/2012/04/02/*"));
			System.out.println("checking " + f.getPath());
			if (stats.length > 0) {


				for (FileStatus g : stats){
					String filename = g.getPath().getName();
					String[] parts = filename.split("\\.");
					//AYNSearchEvent.527.0.1333414800000.1.23631586
					
					EtlKey etlKey = new EtlKey(parts[0], parts[1], Integer.parseInt(parts[2]), -1L, Long.parseLong(parts[5]));
					if (offsets.contains(etlKey)) {
						EtlKey oldKey = offsets.get(offsets.indexOf(etlKey));
						if (oldKey.getOffset() < etlKey.getOffset()) {
							offsets.remove(oldKey);
						}
					}
					offsets.add(etlKey);
					
				}
			}
		}
		DateTimeFormatter dateFmt = DateUtils.getDateTimeFormatter("YYYY-MM-dd-HH-mm-ss");
		SequenceFile.Writer offsetWriter = SequenceFile.createWriter(fs, conf, new Path("/jobs/kafkaetl/", new DateTime().toString(dateFmt)
				+ "/offsets-old"), EtlKey.class, NullWritable.class);
		for (EtlKey s : offsets) {
			offsetWriter.append(s, NullWritable.get());
		}
		offsetWriter.close();

	}

	private static void readRequestSetupFile(Configuration conf, Path requestFile) throws IOException {
		FileSystem fs = FileSystem.get(conf);

		SequenceFile.Reader reader = new SequenceFile.Reader(fs, requestFile, conf);
		while (reader.next(key, offsetBW)) {
			String offsetJson = new String(offsetBW.getBytes(), "UTF-8");
			String trimRequest = offsetJson.trim();

			EtlKey etlKey = createKeyFromJson(trimRequest);
			if (offsets.contains(etlKey)) {
				EtlKey oldKey = offsets.get(offsets.indexOf(etlKey));
				if (oldKey.getOffset() < etlKey.getOffset()) {
					offsets.remove(oldKey);
				}
			}
			offsets.add(etlKey);
		}

		reader.close();
	}

	private static EtlKey createKeyFromJson(String json) throws IOException {
		ObjectMapper m = new ObjectMapper();
		ObjectNode node = (ObjectNode) m.readTree(json);

		String topic = node.get(TOPIC_KEY).getValueAsText();
		String nodeId = node.get(NODE_KEY).getValueAsText();
		int partition = node.get(PARTITION_KEY).getValueAsInt();
		long offset = node.get(OFFSET_KEY).getValueAsLong();

		return new EtlKey(topic, nodeId, partition, 0, offset);
	}
}
