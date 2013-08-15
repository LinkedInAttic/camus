package com.linkedin.camus.example.sample;  

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import kafka.producer.ProducerConfig;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;

import com.linkedin.camus.example.sample.ExampleData;
import com.linkedin.camus.example.sample.ExampleDataSerializer;

public class WriteExampleDataToKafka {

    public static void main(String[] args) throws IOException {

	// Get ZooKeeper connection string
	if (args.length != 2) {
	    System.err.println("Usage: WriteToKafka <ZK String eg. zk.example.com:2181> <Kafka Topic eg. EXAMPLE_LOG>");
	    System.exit(1);
	}

	String zkString = args[0];
	String kTopic = args[1];

	// prepare the record
	Date dt = new Date();
	Map<CharSequence, CharSequence> map = new HashMap<CharSequence, CharSequence>();
	map.put("date", dt.toString());
	ExampleData ed1 = new ExampleData(new Long(dt.getTime()), map);

	// kafka faffing
	Properties props = new Properties();

	// props.put("zk.connect", "127.0.0.1:2181");
	// props.put("zk.connect", "200-01-05.sc1.verticloud.com:2181");
	props.put("zk.connect", zkString);

	// props.put("serializer.class", "kafka.serializer.StringEncoder");
	props.put("serializer.class", "com.linkedin.camus.example.sample.ExampleDataSerializer");
	ProducerConfig config = new ProducerConfig(props);
	Producer<Integer, ExampleData> producer = new Producer<Integer, ExampleData>(config);
	ProducerData<Integer, ExampleData> data = new ProducerData<Integer, ExampleData>(kTopic, new Integer(1), Arrays.asList(ed1));
        
	System.out.println("Sending " + ed1.toString() + " to kafka @ topic " + kTopic);
	producer.send(data);
	System.out.println("done!");
	producer.close();
	
    }

}
