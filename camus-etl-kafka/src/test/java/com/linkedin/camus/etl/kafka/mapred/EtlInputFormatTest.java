package com.linkedin.camus.etl.kafka.mapred;

import java.util.List;
import java.util.Collections;

import kafka.common.ErrorMapping;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import org.junit.Test;


public class EtlInputFormatTest {

  /**
   * Test only refreshing the paritionMetadata when the error code is LeaderNotAvailable.
   * @throws Exception 
   */
  @Test
  public void testRefreshPartitioMetadataOnLeaderNotAvailable() throws Exception {
    JobContext dummyContext = null;
    //A partitionMetadata with errorCode LeaderNotAvailable
    PartitionMetadata partitionMetadata1 = createNiceMock(PartitionMetadata.class);
    expect(partitionMetadata1.errorCode()).andReturn(ErrorMapping.LeaderNotAvailableCode());
    expect(partitionMetadata1.partitionId()).andReturn(0);
    replay(partitionMetadata1);
    
    //A partitionMetadata with errorCode not LeaderNotAvailable
    PartitionMetadata partitionMetadata2 = createNiceMock(PartitionMetadata.class);
    expect(partitionMetadata2.errorCode()).andReturn(ErrorMapping.InvalidMessageCode());
    expect(partitionMetadata2.partitionId()).andReturn(0);
    replay(partitionMetadata2);

    PartitionMetadata mockedReturnedPartitionMetadata = createNiceMock(PartitionMetadata.class);
    expect(mockedReturnedPartitionMetadata.errorCode()).andReturn(ErrorMapping.NoError());
    expect(mockedReturnedPartitionMetadata.partitionId()).andReturn(0);
    replay(mockedReturnedPartitionMetadata);
    
    TopicMetadata mockedTopicMetadata = createNiceMock(TopicMetadata.class);
    expect(mockedTopicMetadata.topic()).andReturn("testTopic");
    expect(mockedTopicMetadata.partitionsMetadata()).andReturn(
        Collections.singletonList(mockedReturnedPartitionMetadata));
    replay(mockedTopicMetadata);

    EtlInputFormat etlInputFormat =
        createMock(EtlInputFormat.class,
            EtlInputFormat.class.getMethod("getKafkaMetadata", new Class[] { JobContext.class, List.class }));
    EasyMock.expect(etlInputFormat.getKafkaMetadata(dummyContext, Collections.singletonList("testTopic"))).andReturn(
        Collections.singletonList(mockedTopicMetadata));
    etlInputFormat.setLogger(Logger.getLogger(getClass()));
    replay(etlInputFormat);
    
    //For partitionMetadata2, it will not refresh if the errorcode is not LeaderNotAvailable.
    assertEquals(
        partitionMetadata2,
        etlInputFormat.refreshPartitionMetadataOnLeaderNotAvailable(partitionMetadata2, mockedTopicMetadata,
            dummyContext, 1));
    
    //For partitionMetadata1, it will refresh if the errorcode is LeaderNotAvailable.
    assertEquals(
        mockedReturnedPartitionMetadata,
        etlInputFormat.refreshPartitionMetadataOnLeaderNotAvailable(partitionMetadata1, mockedTopicMetadata,
            dummyContext, 1));
    
  }
}
