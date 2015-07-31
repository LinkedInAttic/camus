package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.easymock.EasyMock;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.kafka.CamusJob;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlRequest;
import com.linkedin.camus.etl.kafka.common.LeaderInfo;
import com.linkedin.camus.workallocater.CamusRequest;


public class EtlInputFormatForUnitTest extends EtlInputFormat {
  public static enum ConsumerType {
    REGULAR,
    MOCK
  }

  public static enum RecordReaderClass {
    REGULAR,
    TEST
  }

  public static enum CamusRequestType {
    REGULAR,
    MOCK_OFFSET_TOO_EARLY,
    MOCK_OFFSET_TOO_LATE
  }

  public static SimpleConsumer consumer;
  public static ConsumerType consumerType = ConsumerType.REGULAR;
  public static RecordReaderClass recordReaderClass = RecordReaderClass.REGULAR;
  public static CamusRequestType camusRequestType = CamusRequestType.REGULAR;

  public EtlInputFormatForUnitTest() {
    super();
  }

  @Override
  public SimpleConsumer createSimpleConsumer(JobContext context, String host, int port) {
    switch (consumerType) {
      case REGULAR:
        return new SimpleConsumer(host, port, CamusJob.getKafkaTimeoutValue(context),
            CamusJob.getKafkaBufferSize(context), CamusJob.getKafkaClientName(context));
      case MOCK:
        return consumer;
      default:
        throw new RuntimeException("consumer type not found");
    }
  }

  @Override
  public RecordReader<EtlKey, CamusWrapper> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    switch (recordReaderClass) {
      case REGULAR:
        return new EtlRecordReader(this, split, context);
      case TEST:
        return new EtlRecordReaderForUnitTest(this, split, context);
      default:
        throw new RuntimeException("record reader class not found");
    }
  }

  @Override
  public ArrayList<CamusRequest> fetchLatestOffsetAndCreateEtlRequests(JobContext context,
      HashMap<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo) {
    ArrayList<CamusRequest> finalRequests = super.fetchLatestOffsetAndCreateEtlRequests(context, offsetRequestInfo);
    switch (camusRequestType) {
      case REGULAR:
        break;
      case MOCK_OFFSET_TOO_EARLY:
        for (CamusRequest request : finalRequests) {
          modifyRequestOffsetTooEarly(request);
        }
        break;
      case MOCK_OFFSET_TOO_LATE:
        for (CamusRequest request : finalRequests) {
          modifyRequestOffsetTooLate(request);
        }
        break;
      default:
        throw new RuntimeException("camus request class not found");
    }
    return finalRequests;
  }

  protected void writeRequests(List<CamusRequest> requests, JobContext context) throws IOException {
    //do nothing
  }

  public static void modifyRequestOffsetTooEarly(CamusRequest etlRequest) {
    etlRequest.setEarliestOffset(-1L);
    etlRequest.setOffset(-2L);
    etlRequest.setLatestOffset(1L);
  }

  public static void modifyRequestOffsetTooLate(CamusRequest etlRequest) {
    etlRequest.setEarliestOffset(-1L);
    etlRequest.setOffset(2L);
    etlRequest.setLatestOffset(1L);
  }

  public static void reset() {
    consumerType = ConsumerType.REGULAR;
    recordReaderClass = RecordReaderClass.REGULAR;
    camusRequestType = CamusRequestType.REGULAR;
  }
}
