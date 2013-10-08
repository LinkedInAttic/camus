package com.linkedin.camus.etl.kafka.mapred;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;
import com.linkedin.camus.etl.kafka.common.KafkaReader;

import java.io.IOException;
import java.util.HashSet;

import kafka.message.Message;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

public class EtlRecordReader extends RecordReader<EtlKey, CamusWrapper> {
    private static final String PRINT_MAX_DECODER_EXCEPTIONS = "max.decoder.exceptions.to.print";
    private static final String DEFAULT_SERVER = "server";
    private static final String DEFAULT_SERVICE = "service";
    private TaskAttemptContext context;

    private Mapper<EtlKey, Writable, EtlKey, Writable>.Context mapperContext;
    private KafkaReader reader;

    private long totalBytes;
    private long readBytes = 0;

    private boolean skipSchemaErrors = false;
    private MessageDecoder decoder;
    private final BytesWritable msgValue = new BytesWritable();
    private final BytesWritable msgKey = new BytesWritable();
    private final EtlKey key = new EtlKey();
    private CamusWrapper value;

    private int maxPullHours = 0;
    private int exceptionCount = 0;
    private long maxPullTime = 0;
    private long beginTimeStamp = 0;
    private long endTimeStamp = 0;
    private HashSet<String> ignoreServerServiceList = null;

    private String statusMsg = "";

    EtlSplit split;
    private static Logger log = Logger.getLogger(EtlRecordReader.class);

    /**
     * Record reader to fetch directly from Kafka
     *
     * @param split
     * @throws IOException
     * @throws InterruptedException
     */
    public EtlRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        initialize(split, context);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        this.split = (EtlSplit) split;
        this.context = context;

        if (context instanceof Mapper.Context) {
            mapperContext = (Context) context;
        }

        this.skipSchemaErrors = EtlInputFormat.getEtlIgnoreSchemaErrors(context);

        if (EtlInputFormat.getKafkaMaxPullHrs(context) != -1) {
            this.maxPullHours = EtlInputFormat.getKafkaMaxPullHrs(context);
        } else {
            this.endTimeStamp = Long.MAX_VALUE;
        }

        if (EtlInputFormat.getKafkaMaxPullMinutesPerTask(context) != -1) {
            DateTime now = new DateTime();
            this.maxPullTime = now.plusMinutes(
                    EtlInputFormat.getKafkaMaxPullMinutesPerTask(context)).getMillis();
        } else {
            this.maxPullTime = Long.MAX_VALUE;
        }

        if (EtlInputFormat.getKafkaMaxHistoricalDays(context) != -1) {
            int maxDays = EtlInputFormat.getKafkaMaxHistoricalDays(context);
            beginTimeStamp = (new DateTime()).minusDays(maxDays).getMillis();
        } else {
            beginTimeStamp = 0;
        }
        
        ignoreServerServiceList = new HashSet<String>();
        for(String ignoreServerServiceTopic : EtlInputFormat.getEtlAuditIgnoreServiceTopicList(context))
        {
        	ignoreServerServiceList.add(ignoreServerServiceTopic);
        }

        this.totalBytes = this.split.getLength();
    }

    @Override
    public synchronized void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    private CamusWrapper getWrappedRecord(String topicName, byte[] payload) throws IOException {
        CamusWrapper r = null;
        try {
            r = decoder.decode(payload);
        } catch (Exception e) {
            if (!skipSchemaErrors) {
                throw new IOException(e);
            }
        }
        return r;
    }

    private static byte[] getBytes(BytesWritable val) {
        byte[] buffer = val.getBytes();

        /*
         * FIXME: remove the following part once the below jira is fixed
         * https://issues.apache.org/jira/browse/HADOOP-6298
         */
        long len = val.getLength();
        byte[] bytes = buffer;
        if (len < buffer.length) {
            bytes = new byte[(int) len];
            System.arraycopy(buffer, 0, bytes, 0, (int) len);
        }

        return bytes;
    }

    @Override
    public float getProgress() throws IOException {
        if (getPos() == 0) {
            return 0f;
        }

        if (getPos() >= totalBytes) {
            return 1f;
        }
        return (float) ((double) getPos() / totalBytes);
    }

    private long getPos() throws IOException {
        if (reader != null) {
            return readBytes + reader.getReadBytes();
        } else {
            return readBytes;
        }
    }

    @Override
    public EtlKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public CamusWrapper getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        Message message = null;

        // we only pull for a specified time. unfinished work will be
        // rescheduled in the next
        // run.
        if (System.currentTimeMillis() > maxPullTime) {
            log.info("Max pull time reached");
            if (reader != null) {
                closeReader();
            }
            return false;
        }

        while (true) {
            try {
                if (reader == null || !reader.hasNext()) {
                    EtlRequest request = split.popRequest();
                    if (request == null) {
                        return false;
                    }

                    if (maxPullHours > 0) {
                        endTimeStamp = 0;
                    }

                    key.set(request.getTopic(), request.getLeaderId(), request.getPartition(),
                            request.getOffset(), request.getOffset(), 0);
                    value = null;
                    log.info("\n\ntopic:" + request.getTopic() + " partition:"
                            + request.getPartition() + " beginOffset:" + request.getOffset()
                            + " estimatedLastOffset:" + request.getLastOffset());

                    statusMsg += statusMsg.length() > 0 ? "; " : "";
                    statusMsg += request.getTopic() + ":" + request.getLeaderId() + ":"
                            + request.getPartition();
                    context.setStatus(statusMsg);

                    if (reader != null) {
                        closeReader();
                    }
                    reader = new KafkaReader(context, request,
                            CamusJob.getKafkaTimeoutValue(mapperContext),
                            CamusJob.getKafkaBufferSize(mapperContext));

                    decoder = MessageDecoderFactory.createMessageDecoder(context, request.getTopic());
                }
                int count = 0;
                while (reader.getNext(key, msgValue, msgKey)) {
                    count++;
                    context.progress();
                    mapperContext.getCounter("total", "data-read").increment(msgValue.getLength());
                    mapperContext.getCounter("total", "event-count").increment(1);
                    byte[] bytes = getBytes(msgValue);
                    byte[] keyBytes = getBytes(msgKey);
                    // check the checksum of message.
                    // If message has partition key, need to construct it with Key for checkSum to match
                    if (keyBytes.length == 0) {
                        message = new Message(bytes);
                    } else {
                        message = new Message(bytes, keyBytes);
                    }
                    long checksum = key.getChecksum();
                    if (checksum != message.checksum()) {
                        throw new ChecksumException("Invalid message checksum "
                                + message.checksum() + ". Expected " + key.getChecksum(),
                                key.getOffset());
                    }

                    long tempTime = System.currentTimeMillis();
                    CamusWrapper wrapper;
                    try {
                        wrapper = getWrappedRecord(key.getTopic(), bytes);
                    } catch (Exception e) {
                        if (exceptionCount < getMaximumDecoderExceptionsToPrint(context)) {
                            mapperContext.write(key, new ExceptionWritable(e));
                            exceptionCount++;
                        } else if (exceptionCount == getMaximumDecoderExceptionsToPrint(context)) {
                            exceptionCount = Integer.MAX_VALUE; //Any random value
                            log.info("The same exception has occured for more than " + getMaximumDecoderExceptionsToPrint(context) + " records. All further exceptions will not be printed");
                        }
                        continue;
                    }

                    if (wrapper == null) {
                        mapperContext.write(key, new ExceptionWritable(new RuntimeException(
                                "null record")));
                        continue;
                    }

                    long timeStamp = wrapper.getTimestamp();
                    try {
                        key.setTime(timeStamp);
                        key.setPartition(wrapper.getPartitionMap());
                        setServerService();
                    } catch (Exception e) {
                        mapperContext.write(key, new ExceptionWritable(e));
                        continue;
                    }

                    if (timeStamp < beginTimeStamp) {
                        mapperContext.getCounter("total", "skip-old").increment(1);
                    } else if (endTimeStamp == 0) {
                        DateTime time = new DateTime(timeStamp);
                        statusMsg += " begin read at " + time.toString();
                        context.setStatus(statusMsg);
                        log.info(key.getTopic() + " begin read at " + time.toString());
                        endTimeStamp = (time.plusHours(this.maxPullHours)).getMillis();
                    } else if (timeStamp > endTimeStamp || System.currentTimeMillis() > maxPullTime) {
                        if (timeStamp > endTimeStamp)
                            log.info("Kafka Max history hours reached");
                        if (System.currentTimeMillis() > maxPullTime)
                            log.info("Kafka pull time limit reached");
                        statusMsg += " max read at " + new DateTime(timeStamp).toString();
                        context.setStatus(statusMsg);
                        log.info(key.getTopic() + " max read at "
                                + new DateTime(timeStamp).toString());
                        mapperContext.getCounter("total", "request-time(ms)").increment(
                                reader.getFetchTime());
                        closeReader();
                    }

                    long secondTime = System.currentTimeMillis();
                    value = wrapper;
                    long decodeTime = ((secondTime - tempTime));

                    mapperContext.getCounter("total", "decode-time(ms)").increment(decodeTime);

                    if (reader != null) {
                        mapperContext.getCounter("total", "request-time(ms)").increment(
                                reader.getFetchTime());
                    }
                    return true;
                }
                log.info("Records read : " + count);
                count = 0;
                reader = null;
            } catch (Throwable t) {
                Exception e = new Exception(t.getLocalizedMessage(), t);
                e.setStackTrace(t.getStackTrace());
                mapperContext.write(key, new ExceptionWritable(e));
                reader = null;
                continue;
            }
        }
    }

    private void closeReader() throws IOException {
        if (reader != null) {
            try {
                readBytes += reader.getReadBytes();
                reader.close();
            } catch (Exception e) {
                // not much to do here but skip the task
            } finally {
                reader = null;
            }
        }
    }
    
    public void setServerService()
    {
    	if(ignoreServerServiceList.contains(key.getTopic()) || ignoreServerServiceList.contains("all"))
    	{
    		key.setServer(DEFAULT_SERVER);
    		key.setService(DEFAULT_SERVICE);
    	}
    }

    public static int getMaximumDecoderExceptionsToPrint(JobContext job) {
        return job.getConfiguration().getInt(PRINT_MAX_DECODER_EXCEPTIONS, 10);
    }
}
