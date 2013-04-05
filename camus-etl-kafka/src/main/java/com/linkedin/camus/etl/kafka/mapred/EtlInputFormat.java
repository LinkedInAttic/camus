package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import kafka.common.ErrorMapping;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageDecoder;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;

/**
 * Input format for a Kafka pull job.
 */
public class EtlInputFormat extends InputFormat<EtlKey, AvroWrapper<Object>> {
    public static final String ZK_HOSTS = "zookeeper.hosts";
    public static final String ZK_TOPIC_PATH = "zookeeper.broker.topics";
    public static final String ZK_BROKER_PATH = "zookeeper.broker.nodes";
    public static final String ZK_SESSION_TIMEOUT = "zookeeper.session.timeout";
    public static final String ZK_CONNECTION_TIMEOUT = "zookeeper.connection.timeout";

    public static final String KAFKA_BLACKLIST_TOPIC = "kafka.blacklist.topics";
    public static final String KAFKA_WHITELIST_TOPIC = "kafka.whitelist.topics";

    public static final String KAFKA_MOVE_TO_LAST_OFFSET_LIST = "kafka.move.to.last.offset.list";

    public static final String KAFKA_CLIENT_BUFFER_SIZE = "kafka.client.buffer.size";
    public static final String KAFKA_CLIENT_SO_TIMEOUT = "kafka.client.so.timeout";

    public static final String KAFKA_MAX_PULL_HRS = "kafka.max.pull.hrs";
    public static final String KAFKA_MAX_PULL_MINUTES_PER_TASK = "kafka.max.pull.minutes.per.task";
    public static final String KAFKA_MAX_HISTORICAL_DAYS = "kafka.max.historical.days";

    public static final String CAMUS_MESSAGE_DECODER_CLASS = "camus.message.decoder.class";
    public static final String ETL_IGNORE_SCHEMA_ERRORS = "etl.ignore.schema.errors";
    public static final String ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST = "etl.audit.ignore.service.topic.list";

    private final Logger log = Logger.getLogger(getClass());

    @Override
    public RecordReader<EtlKey, AvroWrapper<Object>> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new EtlRecordReader(split, context);
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        CamusJob.startTiming("getSplits");

        List<EtlRequest> requests;
        try {
            // Create the zkClient for kafka.
            ArrayList<String> topicList = new ArrayList<String>();
            List<String> whiteListTopicsList = Arrays.asList(getKafkaWhitelistTopic(context));
            List<String> blackListTopicsList = Arrays.asList(getKafkaBlacklistTopic(context));
            Set<String> whiteListTopics = new HashSet<String>(whiteListTopicsList);
            Set<String> blackListTopics = new HashSet<String>(blackListTopicsList);
            System.out.println("Whitelist topics : " + whiteListTopicsList);
            System.out.println("Blacklist Topics : " + blackListTopicsList);

            if (!whiteListTopics.isEmpty()) {
                topicList.addAll(whiteListTopics);
            }
            CamusJob.startTiming("kafkaSetupTime");
            List<TopicMetadata> topicMetadataList = null;//KafkaClient.getMetadata(topicList);
            
            
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(CamusJob.getKafkaHostUrl(context), CamusJob.getKafkaHostPort(context),
                        CamusJob.getKafkaTimeoutValue(context), CamusJob.getKafkaBufferSize(context), CamusJob.getKafkaClientName(context));
                topicMetadataList = (consumer.send(new TopicMetadataRequest(topicList)))
                        .topicsMetadata();
            } catch (Exception e) {
                System.out.println("Error while querying Kafka for Metadata");
                System.out.println("Kafka Host Details : " + "\nHost : "
                        + CamusJob.getKafkaHostUrl(context) + "\nPort : " + CamusJob.getKafkaHostPort(context)
                        + "\n");
                throw new RuntimeException("Unable to fetch metadata from Kafka", e);
            } finally {
                if (consumer != null)
                    consumer.close();
            }
            CamusJob.stopTiming("kafkaSetupTime");

            ArrayList<String> topicsToDiscard = new ArrayList<String>();
            for (TopicMetadata topicMetadata : topicMetadataList) {
                if (blackListTopics.contains(topicMetadata.topic())
                        || !createMessageDecoder(context, topicMetadata.topic())) {
                    topicsToDiscard.add(topicMetadata.topic());
                }
            }

            // Initialize a decoder for each topic so we can discard the topics
            // for which we cannot construct a decoder
            for (String topic : topicList) {
                try {
                    MessageDecoderFactory.createMessageDecoder(context, topic);
                } catch (Exception e) {
                    log.debug("We cound not construct a decoder for topic '" + topic
                            + "', so that topic will be discarded.", e);
                    topicsToDiscard.add(topic);
                }
            }

            if (topicsToDiscard.isEmpty()) {
                log.info("All topics seem valid! None will be discarded :)");
            } else {
                log.error("We could not construct decoders for the following topics, so they will be discarded: "
                        + topicsToDiscard);
                topicList.removeAll(topicsToDiscard);
            }

            log.info("The following topicList will be pulled from Kafka and written to HDFS: "
                    + topicList);

            requests = new ArrayList<EtlRequest>();

            HashMap<String, List<EtlRequest>> partitionInfo = new HashMap<String, List<EtlRequest>>();//KafkaClient.createEtlRequests(
                    //topicMetadataList, topicsToDiscard);
            
            
            for (TopicMetadata topicMetadata : topicMetadataList) {
                if (topicsToDiscard.contains(topicMetadata.topic())) {
                    continue;
                }

                List<PartitionMetadata> partitionsMetadata = topicMetadata.partitionsMetadata();
                List<EtlRequest> tempEtlRequests = new ArrayList<EtlRequest>();

                for (PartitionMetadata partitionMetadata : partitionsMetadata) {
                    if (partitionMetadata.errorCode() != ErrorMapping.NoError()) {
                        System.out.println("Skipping the creation of ETL request for Topic : "
                                + topicMetadata.topic() + " and Partition : "
                                + partitionMetadata.partitionId() + " Exception : "
                                + ErrorMapping.exceptionFor(partitionMetadata.errorCode()));
                        // In case of a temporary error, next run should process
                        // this
                        continue;
                    } else {
                        try {
                            EtlRequest etlRequest = new EtlRequest(context, topicMetadata.topic(),
                                    Integer.toString(partitionMetadata.leader().id()),
                                    partitionMetadata.partitionId(), new URI("tcp://"
                                            + partitionMetadata.leader().getConnectionString()));
                            tempEtlRequests.add(etlRequest);
                        } catch (URISyntaxException e) {
                            System.out.println("Skipping the creation of ETL request for Topic : "
                                    + topicMetadata.topic() + " and Partition : "
                                    + partitionMetadata.partitionId() + " Exception : "
                                    + e.getMessage());
                            continue;
                        }
                    }
                }
                if (tempEtlRequests.size() != 0) {
                    
                    partitionInfo.put(topicMetadata.topic(), tempEtlRequests);
                }
            }
            requests = new ArrayList<EtlRequest>();
            for (String topic : partitionInfo.keySet()) {
                requests.addAll(partitionInfo.get(topic));
            }
        } catch (Exception e) {
            log.error("Unable to pull requests from zookeeper, using previous requests", e);
            requests = getPreviousRequests(context);
        }

        // writing request to output directory so next Camus run can use them if
        // needed
        writeRequests(requests, context);

        Map<EtlRequest, EtlKey> offsetKeys = getPreviousOffsets(
                FileInputFormat.getInputPaths(context), context);
        Set<String> moveLatest = getMoveToLatestTopicsSet(context);

        for (EtlRequest request : requests) {
            if (moveLatest.contains(request.getTopic()) || moveLatest.contains("all")) {
                offsetKeys.put(request,
                        new EtlKey(request.getTopic(), request.getNodeId(), request.getPartition(),
                                0, request.getLastOffset()));
            }

            EtlKey key = offsetKeys.get(request);
            if (key != null) {
                request.setOffset(key.getOffset());
            }

            if (request.getEarliestOffset() > request.getOffset()) {
                request.setOffset(request.getEarliestOffset());
            }

            log.info(request);
        }

        writePrevious(offsetKeys.values(), context);

        CamusJob.stopTiming("getSplits");
        CamusJob.startTiming("hadoop");
        CamusJob.setTime("hadoop_start");

        return allocateWork(requests, context);
    }

    private Set<String> getMoveToLatestTopicsSet(JobContext context) {
        Set<String> topics = new HashSet<String>();

        String[] arr = getMoveToLatestTopics(context);

        if (arr != null) {
            for (String topic : arr) {
                topics.add(topic);
            }
        }

        return topics;
    }

    private boolean createMessageDecoder(JobContext context, String topic) {

        try {
            MessageDecoderFactory.createMessageDecoder(context, topic);
            return true;
        } catch (Exception e) {
            log.debug("We cound not construct a decoder for topic '" + topic
                    + "', so that topic will be discarded.", e);
            return false;
        }
    }

    private List<InputSplit> allocateWork(List<EtlRequest> requests, JobContext context)
            throws IOException {
        int numTasks = context.getConfiguration().getInt("mapred.map.tasks", 30);
        // Reverse sort by size
        Collections.sort(requests, new Comparator<EtlRequest>() {
            @Override
            public int compare(EtlRequest o1, EtlRequest o2) {
                if (o2.estimateDataSize() == o1.estimateDataSize()) {
                    return 0;
                }
                if (o2.estimateDataSize() < o1.estimateDataSize()) {
                    return -1;
                } else {
                    return 1;
                }
            }
        });

        List<InputSplit> kafkaETLSplits = new ArrayList<InputSplit>();

        for (int i = 0; i < numTasks; i++) {
            EtlSplit split = new EtlSplit();

            if (requests.size() > 0) {
                split.addRequest(requests.get(0));
                kafkaETLSplits.add(split);
                requests.remove(0);
            }
        }

        for (EtlRequest r : requests) {
            getSmallestMultiSplit(kafkaETLSplits).addRequest(r);
        }

        return kafkaETLSplits;
    }

    private EtlSplit getSmallestMultiSplit(List<InputSplit> kafkaETLSplits) throws IOException {
        EtlSplit smallest = (EtlSplit) kafkaETLSplits.get(0);

        for (int i = 1; i < kafkaETLSplits.size(); i++) {
            EtlSplit challenger = (EtlSplit) kafkaETLSplits.get(i);
            if ((smallest.getLength() == challenger.getLength() && smallest.getNumRequests() > challenger
                    .getNumRequests()) || smallest.getLength() > challenger.getLength()) {
                smallest = challenger;
            }
        }

        return smallest;
    }

    private void writePrevious(Collection<EtlKey> missedKeys, JobContext context)
            throws IOException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path output = FileOutputFormat.getOutputPath(context);

        if (fs.exists(output)) {
            fs.mkdirs(output);
        }

        output = new Path(output, EtlMultiOutputFormat.OFFSET_PREFIX + "-previous");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, context.getConfiguration(),
                output, EtlKey.class, NullWritable.class);

        for (EtlKey key : missedKeys) {
            writer.append(key, NullWritable.get());
        }

        writer.close();
    }

    private void writeRequests(List<EtlRequest> requests, JobContext context) throws IOException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path output = FileOutputFormat.getOutputPath(context);

        if (fs.exists(output)) {
            fs.mkdirs(output);
        }

        output = new Path(output, EtlMultiOutputFormat.REQUESTS_FILE);
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, context.getConfiguration(),
                output, EtlRequest.class, NullWritable.class);

        for (EtlRequest r : requests) {
            writer.append(r, NullWritable.get());
        }
        writer.close();
    }

    private List<EtlRequest> getPreviousRequests(JobContext context) throws IOException {
        List<EtlRequest> requests = new ArrayList<EtlRequest>();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path input = FileInputFormat.getInputPaths(context)[0];

        input = new Path(input, EtlMultiOutputFormat.REQUESTS_FILE);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, input, context.getConfiguration());

        EtlRequest request = new EtlRequest();
        while (reader.next(request, NullWritable.get())) {
            requests.add(new EtlRequest(request));
        }

        reader.close();
        return requests;
    }

    private Map<EtlRequest, EtlKey> getPreviousOffsets(Path[] inputs, JobContext context)
            throws IOException {
        Map<EtlRequest, EtlKey> offsetKeysMap = new HashMap<EtlRequest, EtlKey>();
        for (Path input : inputs) {
            FileSystem fs = input.getFileSystem(context.getConfiguration());
            for (FileStatus f : fs.listStatus(input, new OffsetFileFilter())) {
                log.info("previous offset file:" + f.getPath().toString());
                SequenceFile.Reader reader = new SequenceFile.Reader(fs, f.getPath(),
                        context.getConfiguration());
                EtlKey key = new EtlKey();
                while (reader.next(key, NullWritable.get())) {
                    EtlRequest request = new EtlRequest(context, key.getTopic(), key.getNodeId(),
                            key.getPartition());

                    if (offsetKeysMap.containsKey(request)) {
                        EtlKey oldKey = offsetKeysMap.get(request);
                        if (oldKey.getOffset() < key.getOffset()) {
                            offsetKeysMap.put(request, key);
                        }
                    } else {
                        offsetKeysMap.put(request, key);
                    }
                    key = new EtlKey();
                }
                reader.close();
            }
        }

        return offsetKeysMap;
    }

    public static void setMoveToLatestTopics(JobContext job, String val) {
        job.getConfiguration().set(KAFKA_MOVE_TO_LAST_OFFSET_LIST, val);
    }

    public static String[] getMoveToLatestTopics(JobContext job) {
        return job.getConfiguration().getStrings(KAFKA_MOVE_TO_LAST_OFFSET_LIST);
    }

    public static void setKafkaClientBufferSize(JobContext job, int val) {
        job.getConfiguration().setInt(KAFKA_CLIENT_BUFFER_SIZE, val);
    }

    public static int getKafkaClientBufferSize(JobContext job) {
        return job.getConfiguration().getInt(KAFKA_CLIENT_BUFFER_SIZE, 2 * 1024 * 1024);
    }

    public static void setKafkaClientTimeout(JobContext job, int val) {
        job.getConfiguration().setInt(KAFKA_CLIENT_SO_TIMEOUT, val);
    }

    public static int getKafkaClientTimeout(JobContext job) {
        return job.getConfiguration().getInt(KAFKA_CLIENT_SO_TIMEOUT, 60000);
    }

    public static void setKafkaMaxPullHrs(JobContext job, int val) {
        job.getConfiguration().setInt(KAFKA_MAX_PULL_HRS, val);
    }

    public static int getKafkaMaxPullHrs(JobContext job) {
        return job.getConfiguration().getInt(KAFKA_MAX_PULL_HRS, -1);
    }

    public static void setKafkaMaxPullMinutesPerTask(JobContext job, int val) {
        job.getConfiguration().setInt(KAFKA_MAX_PULL_MINUTES_PER_TASK, val);
    }

    public static int getKafkaMaxPullMinutesPerTask(JobContext job) {
        return job.getConfiguration().getInt(KAFKA_MAX_PULL_MINUTES_PER_TASK, -1);
    }

    public static void setKafkaMaxHistoricalDays(JobContext job, int val) {
        job.getConfiguration().setInt(KAFKA_MAX_HISTORICAL_DAYS, val);
    }

    public static int getKafkaMaxHistoricalDays(JobContext job) {
        return job.getConfiguration().getInt(KAFKA_MAX_HISTORICAL_DAYS, -1);
    }

    public static void setKafkaBlacklistTopic(JobContext job, String val) {
        job.getConfiguration().set(KAFKA_BLACKLIST_TOPIC, val);
    }

    public static String[] getKafkaBlacklistTopic(JobContext job) {
        if (job.getConfiguration().get(KAFKA_BLACKLIST_TOPIC) != null
                && !job.getConfiguration().get(KAFKA_BLACKLIST_TOPIC).isEmpty()) {
            return job.getConfiguration().getStrings(KAFKA_BLACKLIST_TOPIC);
        } else {
            return new String[] {};
        }
    }

    public static void setKafkaWhitelistTopic(JobContext job, String val) {
        job.getConfiguration().set(KAFKA_WHITELIST_TOPIC, val);
    }

    public static String[] getKafkaWhitelistTopic(JobContext job) {
        if (job.getConfiguration().get(KAFKA_WHITELIST_TOPIC) != null
                && !job.getConfiguration().get(KAFKA_WHITELIST_TOPIC).isEmpty()) {
            return job.getConfiguration().getStrings(KAFKA_WHITELIST_TOPIC);
        } else {
            return new String[] {};
        }
    }

    public static void setZkHosts(JobContext job, String val) {
        job.getConfiguration().set(ZK_HOSTS, val);
    }

    public static String getZkHosts(JobContext job) {
        return job.getConfiguration().get(ZK_HOSTS);
    }

    public static void setZkTopicPath(JobContext job, String val) {
        job.getConfiguration().set(ZK_TOPIC_PATH, val);
    }

    public static String getZkTopicPath(JobContext job) {
        return job.getConfiguration().get(ZK_TOPIC_PATH);
    }

    public static void setZkBrokerPath(JobContext job, String val) {
        job.getConfiguration().set(ZK_BROKER_PATH, val);
    }

    public static String getZkBrokerPath(JobContext job) {
        return job.getConfiguration().get(ZK_BROKER_PATH);
    }

    public static void setZkSessionTimeout(JobContext job, int val) {
        job.getConfiguration().setInt(ZK_SESSION_TIMEOUT, val);
    }

    public static void setZkConnectionTimeout(JobContext job, int val) {
        job.getConfiguration().setInt(ZK_CONNECTION_TIMEOUT, val);
    }

    public static void setEtlIgnoreSchemaErrors(JobContext job, boolean val) {
        job.getConfiguration().setBoolean(ETL_IGNORE_SCHEMA_ERRORS, val);
    }

    public static boolean getEtlIgnoreSchemaErrors(JobContext job) {
        return job.getConfiguration().getBoolean(ETL_IGNORE_SCHEMA_ERRORS, false);
    }

    public static void setEtlAuditIgnoreServiceTopicList(JobContext job, String topics) {
        job.getConfiguration().set(ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST, topics);
    }

    public static String[] getEtlAuditIgnoreServiceTopicList(JobContext job) {
        return job.getConfiguration().getStrings(ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST);
    }

    public static void setMessageDecoderClass(JobContext job, Class<KafkaAvroMessageDecoder> cls) {
        job.getConfiguration().setClass(CAMUS_MESSAGE_DECODER_CLASS, cls,
                KafkaAvroMessageDecoder.class);
    }

    public static Class<KafkaAvroMessageDecoder> getMessageDecoderClass(JobContext job) {
        return (Class<KafkaAvroMessageDecoder>) job.getConfiguration().getClass(
                CAMUS_MESSAGE_DECODER_CLASS, KafkaAvroMessageDecoder.class);
    }

    private class OffsetFileFilter implements PathFilter {

        @Override
        public boolean accept(Path arg0) {
            return arg0.getName().startsWith(EtlMultiOutputFormat.OFFSET_PREFIX);
        }
    }
}
