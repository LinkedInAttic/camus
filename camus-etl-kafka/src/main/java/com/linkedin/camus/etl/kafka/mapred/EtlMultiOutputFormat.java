package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.linkedin.camus.coders.Partitioner;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.common.EtlCounts;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;

/**
 * MultipleAvroOutputFormat.
 * 
 * File names are determined by output keys.
 */

public class EtlMultiOutputFormat extends FileOutputFormat<EtlKey, Object> {
    public static final String ETL_DESTINATION_PATH = "etl.destination.path";
    public static final String ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY = "etl.destination.path.topic.sub.dir";
    public static final String ETL_RUN_MOVE_DATA = "etl.run.move.data";
    public static final String ETL_RUN_TRACKING_POST = "etl.run.tracking.post";

    public static final String ETL_DEFAULT_TIMEZONE = "etl.default.timezone";
    public static final String ETL_DEFLATE_LEVEL = "etl.deflate.level";
    public static final String ETL_AVRO_WRITER_SYNC_INTERVAL = "etl.avro.writer.sync.interval";
    public static final String ETL_OUTPUT_FILE_TIME_PARTITION_MINS = "etl.output.file.time.partition.mins";

    public static final String KAFKA_MONITOR_TIME_GRANULARITY_MS = "kafka.monitor.time.granularity";
    public static final String ETL_DEFAULT_PARTITIONER_CLASS = "etl.partitioner.class";
    public static final String ETL_OUTPUT_CODEC = "etl.output.codec";
    public static final String ETL_DEFAULT_OUTPUT_CODEC = "deflate";

    public static final DateTimeFormatter FILE_DATE_FORMATTER = DateUtils
            .getDateTimeFormatter("YYYYMMddHH");
    public final static String EXT = ".avro";
    public static final String OFFSET_PREFIX = "offsets";
    public static final String ERRORS_PREFIX = "errors";
    public static final String COUNTS_PREFIX = "counts";

    public static final String REQUESTS_FILE = "requests.previous";
    private static EtlMultiOutputCommitter committer = null;
    private static Map<String, Partitioner> partitionersByTopic = new HashMap<String, Partitioner>();

    private long granularityMs;

    @Override
    public RecordWriter<EtlKey, Object> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        if (committer == null)
            committer = new EtlMultiOutputCommitter(getOutputPath(context), context);
        granularityMs = getMonitorTimeGranularityMins(context) * 60000L;
        return new MultiEtlRecordWriter(context);
    }

    private RecordWriter<EtlKey, AvroWrapper<Object>> getDataRecordWriter(
            TaskAttemptContext context, String name, String schema) throws IOException,
            InterruptedException {
        final DataFileWriter<Object> writer = new DataFileWriter<Object>(
                new SpecificDatumWriter<Object>());

        if (FileOutputFormat.getCompressOutput(context)) {
            if ("snappy".equals(getEtlOutputCodec(context))) {
                writer.setCodec(CodecFactory.snappyCodec());
            } else {
                int level = getEtlDeflateLevel(context);
                writer.setCodec(CodecFactory.deflateCodec(level));
            }
        }

        Path path = committer.getWorkPath();
        path = new Path(path, getUniqueFile(context, name, EXT));
        writer.create(Schema.parse(schema),
                path.getFileSystem(context.getConfiguration()).create(path));

        writer.setSyncInterval(getEtlAvroWriterSyncInterval(context));

        return new RecordWriter<EtlKey, AvroWrapper<Object>>() {
            @Override
            public void write(EtlKey ignore, AvroWrapper<Object> data) throws IOException {
                writer.append(data.datum());
            }

            @Override
            public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
                writer.close();
            }
        };
    }

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException {
        if (committer == null)
            committer = new EtlMultiOutputCommitter(getOutputPath(context), context);
        granularityMs = getMonitorTimeGranularityMins(context) * 60 * 1000L;
        return committer;
    }

    public static void setDefaultTimeZone(JobContext job, String tz) {
        job.getConfiguration().set(ETL_DEFAULT_TIMEZONE, tz);
    }

    public static String getDefaultTimeZone(JobContext job) {
        return job.getConfiguration().get(ETL_DEFAULT_TIMEZONE, "America/Los_Angeles");
    }
    
    public static void setDestinationPath(JobContext job, Path dest) {
        job.getConfiguration().set(ETL_DESTINATION_PATH, dest.toString());
    }

    public static Path getDestinationPath(JobContext job) {
        return new Path(job.getConfiguration().get(ETL_DESTINATION_PATH));
    }

    public static void setDestPathTopicSubDir(JobContext job, String subPath) {
        job.getConfiguration().set(ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY, subPath);
    }

    public static Path getDestPathTopicSubDir(JobContext job) {
        return new Path(job.getConfiguration().get(ETL_DESTINATION_PATH_TOPIC_SUBDIRECTORY, "hourly"));
    }

    public static void setMonitorTimeGranularityMins(JobContext job, int mins) {
        job.getConfiguration().setInt(KAFKA_MONITOR_TIME_GRANULARITY_MS, mins);
    }

    public static int getMonitorTimeGranularityMins(JobContext job) {
        return job.getConfiguration().getInt(KAFKA_MONITOR_TIME_GRANULARITY_MS, 10);
    }

    public static void setEtlAvroWriterSyncInterval(JobContext job, int val) {
        job.getConfiguration().setInt(ETL_AVRO_WRITER_SYNC_INTERVAL, val);
    }

    public static int getEtlAvroWriterSyncInterval(JobContext job) {
        return job.getConfiguration().getInt(ETL_AVRO_WRITER_SYNC_INTERVAL, 16000);
    }

    public static void setEtlDeflateLevel(JobContext job, int val) {
        job.getConfiguration().setInt(ETL_DEFLATE_LEVEL, val);
    }
    
    public static void setEtlOutputCodec(JobContext job, String codec) {
        job.getConfiguration().set(ETL_OUTPUT_CODEC, codec);
    }

    public static String getEtlOutputCodec(JobContext job) {
        return job.getConfiguration().get(ETL_OUTPUT_CODEC, ETL_DEFAULT_OUTPUT_CODEC);

    }
    public static int getEtlDeflateLevel(JobContext job) {
        return job.getConfiguration().getInt(ETL_DEFLATE_LEVEL, 6);
    }

    public static int getEtlOutputFileTimePartitionMins(JobContext job) {
        return job.getConfiguration().getInt(ETL_OUTPUT_FILE_TIME_PARTITION_MINS, 60);
    }

    public static void setEtlOutputFileTimePartitionMins(JobContext job, int val) {
        job.getConfiguration().setInt(ETL_OUTPUT_FILE_TIME_PARTITION_MINS, val);
    }

    public static boolean isRunMoveData(JobContext job) {
        return job.getConfiguration().getBoolean(ETL_RUN_MOVE_DATA, true);
    }

    public static void setRunMoveData(JobContext job, boolean value) {
        job.getConfiguration().setBoolean(ETL_RUN_MOVE_DATA, value);
    }

    public static boolean isRunTrackingPost(JobContext job) {
        return job.getConfiguration().getBoolean(ETL_RUN_TRACKING_POST, false);
    }

    public static void setRunTrackingPost(JobContext job, boolean value) {
        job.getConfiguration().setBoolean(ETL_RUN_TRACKING_POST, value);
    }

    public String getWorkingFileName(JobContext context, EtlKey key) throws IOException {
        Partitioner partitioner = getPartitioner(context, key.getTopic());
        return "data." + key.getTopic().replaceAll("\\.", "_") + "." + key.getNodeId() + "." + key.getPartition() + "." + partitioner.encodePartition(context, key);
    }

    public static Partitioner getDefaultPartitioner(JobContext job) {
        if(partitionersByTopic.get(ETL_DEFAULT_PARTITIONER_CLASS) == null) {
            List<Partitioner> partitioners = job.getConfiguration().getInstances(ETL_DEFAULT_PARTITIONER_CLASS, com.linkedin.camus.coders.Partitioner.class);
            partitionersByTopic.put(ETL_DEFAULT_PARTITIONER_CLASS, partitioners.get(0));
        }
        return partitionersByTopic.get(ETL_DEFAULT_PARTITIONER_CLASS);
    }

    public static Partitioner getPartitioner(JobContext job, String topicName) throws IOException {
        String customPartitionerProperty = ETL_DEFAULT_PARTITIONER_CLASS + "." + topicName;
        if(partitionersByTopic.get(customPartitionerProperty) == null) {
            List<Partitioner> partitioners = job.getConfiguration().getInstances(customPartitionerProperty, com.linkedin.camus.coders.Partitioner.class);
            if(partitioners.isEmpty()) {
                return getDefaultPartitioner(job);
            } else {
                partitionersByTopic.put(customPartitionerProperty, partitioners.get(0));
            }
        }
        return partitionersByTopic.get(customPartitionerProperty);
    }

    public static void resetPartitioners() {
        partitionersByTopic = new HashMap<String, Partitioner>();
    }

    class MultiEtlRecordWriter extends RecordWriter<EtlKey, Object> {
        private TaskAttemptContext context;
        private Writer errorWriter = null;
        private String currentTopic = "";
        private long beginTimeStamp = 0;

        private HashMap<String, RecordWriter<EtlKey, AvroWrapper<Object>>> dataWriters = new HashMap<String, RecordWriter<EtlKey, AvroWrapper<Object>>>();

        public MultiEtlRecordWriter(TaskAttemptContext context) throws IOException,
                InterruptedException {
            this.context = context;
            errorWriter = SequenceFile.createWriter(FileSystem.get(context.getConfiguration()),
                    context.getConfiguration(),
                    new Path(committer.getWorkPath(), getUniqueFile(context, ERRORS_PREFIX, "")),
                    EtlKey.class, ExceptionWritable.class);

            if (EtlInputFormat.getKafkaMaxHistoricalDays(context) != -1) {
                int maxDays = EtlInputFormat.getKafkaMaxHistoricalDays(context);
                beginTimeStamp = (new DateTime()).minusDays(maxDays).getMillis();
            } else {
                beginTimeStamp = 0;
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            for (String w : dataWriters.keySet()) {
                dataWriters.get(w).close(context);
            }
            errorWriter.close();
        }

        @Override
        public void write(EtlKey key, Object val) throws IOException, InterruptedException {
            if (val instanceof AvroWrapper<?>) {
                if (key.getTime() < beginTimeStamp) {
                    // ((Mapper.Context)context).getCounter("total",
                    // "skip-old").increment(1);
                    committer.addOffset(key);
                } else {
                    if (!key.getTopic().equals(currentTopic)) {
                        for (RecordWriter<EtlKey, AvroWrapper<Object>> writer : dataWriters
                                .values()) {
                            writer.close(context);
                        }
                        dataWriters.clear();
                        currentTopic = key.getTopic();
                    }

                    committer.addCounts(key);
                    AvroWrapper<Object> value = (AvroWrapper<Object>) val;
                    String workingFileName = getWorkingFileName(context, key);
                    if (!dataWriters.containsKey(workingFileName)) {
                        dataWriters.put(
                                workingFileName,
                                getDataRecordWriter(context, workingFileName,
                                        ((GenericRecord) value.datum()).getSchema().toString()));
                    }
                    dataWriters.get(workingFileName).write(key, value);
                }
            } else if (val instanceof ExceptionWritable) {
                committer.addOffset(key);
                System.err.println(key.toString());
                System.err.println(val.toString());
                errorWriter.append(key, (ExceptionWritable) val);
            }
        }
    }

    public class EtlMultiOutputCommitter extends FileOutputCommitter {
        Pattern workingFileMetadataPattern = Pattern.compile("data\\.([^\\.]+)\\.(\\d+)\\.(\\d+)\\.([^\\.]+)-m-\\d+.avro");

        HashMap<String, EtlCounts> counts = new HashMap<String, EtlCounts>();
        HashMap<String, EtlKey> offsets = new HashMap<String, EtlKey>();

        TaskAttemptContext context;

        public void addCounts(EtlKey key) throws IOException {
            String workingFileName = getWorkingFileName(context, key);
            if (!counts.containsKey(workingFileName))
                counts.put(workingFileName, new EtlCounts(context.getConfiguration(), key.getTopic(),
                        granularityMs));
            counts.get(workingFileName).incrementMonitorCount(key);
            addOffset(key);
        }

        public void addOffset(EtlKey key) {
            String topicPart = key.getTopic() + "-" + key.getNodeId() + "-" + key.getPartition();
            offsets.put(topicPart, new EtlKey(key));
        }

        public EtlMultiOutputCommitter(Path outputPath, TaskAttemptContext context)
                throws IOException {
            super(outputPath, context);
            this.context = context;
        }

        @Override
        public void commitTask(TaskAttemptContext context) throws IOException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            if (isRunMoveData(context)) {
                Path workPath = super.getWorkPath();
                Path baseOutDir = getDestinationPath(context);

                for (FileStatus f : fs.listStatus(workPath)) {
                    String file = f.getPath().getName();
                    if (file.startsWith("data")) {
                        String workingFileName = file.substring(0, file.lastIndexOf("-m"));
                        EtlCounts count = counts.get(workingFileName);

                        String partitionedFile = getPartitionedPath(context, file,
                                count.getEventCount(), count.getLastOffsetKey().getOffset());

                        Path dest = new Path(baseOutDir, partitionedFile);

                        if (!fs.exists(dest.getParent())) {
                                fs.mkdirs(dest.getParent());
                            }

                        fs.rename(f.getPath(), dest);

                        if (isRunTrackingPost(context)) {
                                count.writeCountsToHDFS(fs, new Path(workPath, COUNTS_PREFIX + "."
                                        + dest.getName().replace(EXT, "")));
                        }
                    }
                }
            }

            SequenceFile.Writer offsetWriter = SequenceFile.createWriter(fs,
                    context.getConfiguration(),
                    new Path(super.getWorkPath(), getUniqueFile(context, OFFSET_PREFIX, "")),
                    EtlKey.class, NullWritable.class);
            for (String s : offsets.keySet()) {
                offsetWriter.append(offsets.get(s), NullWritable.get());
            }
            offsetWriter.close();
            super.commitTask(context);
        }

        public String getPartitionedPath(JobContext context, String file, int count, long offset) throws IOException {
            Matcher m = workingFileMetadataPattern.matcher(file);
            if(! m.find()) {
                throw new IOException("Could not extract metadata from working filename '" + file + "'");
            }
            String topic = m.group(1);
            String nodeId = m.group(2);
            String partition = m.group(3);
            String encodedPartition = m.group(4);

            String partitionedPath =
                        getPartitioner(context, topic).generatePartitionedPath(context, topic, Integer.parseInt(nodeId),
                                Integer.parseInt(partition), encodedPartition);

            return partitionedPath +
                        "/" + topic + "." + nodeId + "." + partition +
                        "." + count+
                        "." + offset + EXT;
        }
    }
}
