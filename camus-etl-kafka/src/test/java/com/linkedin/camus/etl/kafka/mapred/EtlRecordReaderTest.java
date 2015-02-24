package com.linkedin.camus.etl.kafka.mapred;

import org.easymock.EasyMock;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.schemaregistry.SchemaNotFoundException;


public class EtlRecordReaderTest {

  public static MessageDecoder mockDecoder;

  public static void createMockDecoder30PercentSchemaNotFound() {
    mockDecoder = EasyMock.createNiceMock(MessageDecoder.class);
    EasyMock.expect(mockDecoder.decode(EasyMock.anyObject())).andThrow(new SchemaNotFoundException()).times(9);
    EasyMock.expect(mockDecoder.decode(EasyMock.anyObject())).andReturn(new CamusWrapper<String>("dummy")).times(21);
    EasyMock.replay(mockDecoder);
  }

  public static void createMockDecoder30PercentOther() {
    mockDecoder = EasyMock.createNiceMock(MessageDecoder.class);
    EasyMock.expect(mockDecoder.decode(EasyMock.anyObject())).andThrow(new RuntimeException()).times(9);
    EasyMock.expect(mockDecoder.decode(EasyMock.anyObject())).andReturn(new CamusWrapper<String>("dummy")).times(21);
    EasyMock.replay(mockDecoder);
  }
}
