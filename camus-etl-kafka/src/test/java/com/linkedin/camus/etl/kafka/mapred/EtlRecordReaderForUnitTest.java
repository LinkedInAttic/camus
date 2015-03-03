package com.linkedin.camus.etl.kafka.mapred;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.easymock.EasyMock;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.etl.kafka.coders.MessageDecoderFactory;
import com.linkedin.camus.schemaregistry.SchemaNotFoundException;


public class EtlRecordReaderForUnitTest extends EtlRecordReader {

  public static enum DecoderType {
    REGULAR,
    SCHEMA_NOT_FOUND_30_PERCENT,
    OTHER_30_PERCENT;
  }

  public static DecoderType decoderType = DecoderType.REGULAR;

  public EtlRecordReaderForUnitTest(EtlInputFormatForUnitTest etlInputFormatForUnitTest, InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    super(etlInputFormatForUnitTest, split, context);
  }

  @Override
  protected MessageDecoder createDecoder(String topic) {
    switch (decoderType) {
      case REGULAR:
        return MessageDecoderFactory.createMessageDecoder(context, topic);
      case SCHEMA_NOT_FOUND_30_PERCENT:
        return createMockDecoder30PercentSchemaNotFound();
      case OTHER_30_PERCENT:
        return createMockDecoder30PercentOther();
      default:
        throw new RuntimeException("decoder type undefined");
    }
  }

  public static MessageDecoder createMockDecoder30PercentSchemaNotFound() {
    MessageDecoder mockDecoder = EasyMock.createNiceMock(MessageDecoder.class);
    EasyMock.expect(mockDecoder.decode(EasyMock.anyObject())).andThrow(new SchemaNotFoundException()).times(3);
    EasyMock.expect(mockDecoder.decode(EasyMock.anyObject())).andReturn(new CamusWrapper<String>("dummy")).times(7);
    EasyMock.replay(mockDecoder);
    return mockDecoder;
  }

  public static MessageDecoder createMockDecoder30PercentOther() {
    MessageDecoder mockDecoder = EasyMock.createNiceMock(MessageDecoder.class);
    EasyMock.expect(mockDecoder.decode(EasyMock.anyObject())).andThrow(new RuntimeException()).times(3);
    EasyMock.expect(mockDecoder.decode(EasyMock.anyObject())).andReturn(new CamusWrapper<String>("dummy")).times(7);
    EasyMock.replay(mockDecoder);
    return mockDecoder;
  }

  public static void reset() {
    decoderType = DecoderType.REGULAR;
  }
}
