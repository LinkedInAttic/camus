//package com.linkedin.camus.etl.kafka.test;
//
//import java.io.IOException;
//import java.net.URI;
//import java.net.URISyntaxException;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//
//import kafka.api.OffsetRequest;
//import kafka.javaapi.consumer.SimpleConsumer;
//import kafka.producer.SyncProducerConfig;
//
//import org.codehaus.jackson.map.ObjectMapper;
//import org.joda.time.DateTime;
//
//import com.linkedin.camus.etl.kafka.common.DateUtils;
//import com.linkedin.camus.etl.kafka.common.EtlRequest;
//import com.linkedin.camus.etl.kafka.common.EtlZkClient;
//
///**
// * Test the kafka zookeeper client
// * 
// * @author
// * 
// */
//@SuppressWarnings("unused")
//public class TestKafkaZkClient
//{
//  private static final long   DEFAULT_OFFSET      = -1;
//  private static final String TOPIC_KEY           = "topic";
//  private static final String NODE_KEY            = "node";
//  private static final String URI_KEY             = "uri";
//  private static final String PARTITION_KEY       = "partition";
//  private static final String OFFSET_KEY          = "offset";
//  private static final String PREVIOUS_OFFSET_KEY = "previous";
//
//  private final String        topic               = "test";
//  private final String        nodeId              = "node";
//  private final int           partition           = 1;
//
//  private URI                 uri;
//  private final long          offset              = DEFAULT_OFFSET;
//  private final long          previousOffset      = DEFAULT_OFFSET;
//
//  private static void printPartition(String date)
//  {
//
//    DateTime time = DateUtils.MINUTE_FORMATTER.parseDateTime(date);
//    long partition = DateUtils.getPartition(3600000, time.getMillis());
//
//    System.out.println(partition + " " + DateUtils.MINUTE_FORMATTER.print(partition)
//        + " " + time.getMillis() + " "
//        + DateUtils.MINUTE_FORMATTER.print(time.getMillis()) + " dsd " + partition
//        / 3600000);
//  }
//
//  private static void testSchema()
//  {
//    String schemaStr =
//        "{'type':'record', 'name':'test 'fields':[{'name':'col1', 'type':'int'},{'name':'col2', 'type':'boolean', 'default':false}]}";
//  }
//
//  public static void main(String[] args)
//  {
//    String unsanitizedSchema =
//        "{'type':'record','name':'NetworkUpdateEvent','namespace':'com.linkedin.events.nus','fields':[{'name':'header','type':{'type':'record','name':'EventHeader','namespace':'com.linkedin.events','fields':[{'name':'memberId','type':'int','doc':'The member id of the user initiating the action'},{'name':'time','type':'long','doc':'The time of the event'},{'name':'server','type':'string','doc':'The name of the server'},{'name':'service','type':'string','doc':'The name of the service'},{'name':'guid','type':{'type':'fixed','name':'Guid','namespace':'com.linkedin.events','size':16},'doc':'A unique identifier for the message'}]}},{'name':'source','type':{'type':'record','name':'EntityID','namespace':'com.linkedin.events','fields':[{'name':'type','type':{'type':'enum','name':'EntityType','namespace':'com.linkedin.events','symbols':['ANET','ARTC','CMPY','EVNT','JOB','MBR','QSTN','SHAR','LART','CMTS','CMTA','NUDG','PDCT','REVW','PMOD','INDY','CSRC','STTL','ANSW','APP','AD']}},{'name':'id','type':'string'}]}},{'name':'updateId','type':'string','default':''},{'name':'updateType','type':'string','default':''},{'name':'updateSubtype','type':'string','default':''},{'name':'visibility','type':'string','default':''},{'name':'primaryUrl','type':'string','default':''},{'name':'destinations','type':{'type':'array','items':'com.linkedin.events.EntityID'}},{'name':'targets','type':{'type':'array','items':{'type':'record','name':'Target','fields':[{'name':'destination','type':'com.linkedin.events.EntityID'},{'name':'visibility','type':'string','default':''}]}}},{'name':'references','type':{'type':'array','items':'com.linkedin.events.EntityID'}},{'name':'content','type':{'type':'map','values':'string'}},{'name':'joinedData','type':{'type':'map','values':[{'type':'record','name':'PersonInfo','namespace':'com.linkedin.firehose','fields':[{'name':'id','type':'int'},{'name':'active','type':'boolean'},{'name':'deleted','type':'boolean'},{'name':'name','type':'string'},{'name':'firstName','type':'string'},{'name':'lastName','type':'string'},{'name':'maidenName','type':'string'},{'name':'headline','type':'string'},{'name':'industryId','type':'int'},{'name':'pictureId','type':'string'},{'name':'showPictureSetting','type':'string'},{'name':'showFullLastName','type':'boolean'},{'name':'createdTime','type':'long'},{'name':'location','type':{'type':'record','name':'Location','namespace':'com.linkedin.firehose','fields':[{'name':'country','type':'string','default':''},{'name':'region','type':'string','default':''},{'name':'postalCode','type':'string','default':''},{'name':'place','type':'string','default':''},{'name':'latitude','type':'string','default':''},{'name':'longitude','type':'string','default':''}]}},{'name':'currentIndustries','type':{'type':'array','items':'int'}},{'name':'currentTitles','type':{'type':'array','items':'int'}},{'name':'currentCompanies','type':{'type':'array','items':'int'}},{'name':'currentFuncareas','type':{'type':'array','items':'int'}},{'name':'currentJobNames','type':{'type':'array','items':'int'}},{'name':'pastTitles','type':{'type':'array','items':'int'}},{'name':'pastCompanies','type':{'type':'array','items':'int'}},{'name':'pastFuncareas','type':{'type':'array','items':'int'}},{'name':'pastIndustries','type':{'type':'array','items':'int'}},{'name':'pastJobNames','type':{'type':'array','items':'int'}},{'name':'jobSeniorities','type':{'type':'array','items':'int'}},{'name':'standardizedSkills','type':{'type':'array','items':'int'}},{'name':'schools','type':{'type':'array','items':'int'}},{'name':'fieldsOfStudy','type':{'type':'array','items':'int'}},{'name':'degreeIds','type':{'type':'array','items':'int'}},{'name':'skillsAndScores','type':{'type':'array','items':{'type':'record','name':'SkillAndScore','namespace':'com.linkedin.firehose','fields':[{'name':'id','type':'int'},{'name':'score','type':'float'}]}}},{'name':'anetMembership','type':{'type':'array','items':'int'}},{'name':'connectionCount','type':'int'},{'name':'fortune1000','type':{'type':'array','items':'int'}}]},{'type':'record','name':'UrlUnwoundInfo','namespace':'com.linkedin.firehose','fields':[{'name':'submittedUrl','type':'string'},{'name':'resolvedUrl','type':'string'},{'name':'title','type':'string'},{'name':'description','type':'string'},{'name':'primaryImageUrl','type':'string'},{'name':'createdAt','type':'long'},{'name':'entityId','type':'com.linkedin.events.EntityID'},{'name':'hasBadWords','type':'boolean','default':false}]}]},'doc':'array of joined entities','default':{}}]}";
//    unsanitizedSchema = unsanitizedSchema.replace("'", "\"");
//    System.out.println("Before " + unsanitizedSchema);
//    String sanitized = unsanitizedSchema.replaceAll(",\"doc\":\"[^\"]*\"", "");
//    System.out.println("After " + sanitized);
//
//    Properties props = new Properties();
//    props.put("host", "localhost");
//    props.put("port", String.valueOf(1234));
//
//    SyncProducerConfig config = new SyncProducerConfig(props);
//
//    testSimpleConsumerOffset();
//
//    printPartition("2011-01-10-11-45");
//    printPartition("2011-01-10-11-42");
//    printPartition("2011-01-10-11-49");
//    printPartition("2011-01-10-11-39");
//    printPartition("2011-01-10-11-55");
//    printPartition("2011-01-10-12-55");
//
//    // testConnection();
//    TestKafkaZkClient client = new TestKafkaZkClient();
//
//    try
//    {
//      client.testJackson();
//    }
//    catch (Exception e)
//    {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
//  }
//
//  private void testJackson() throws IOException,
//      URISyntaxException
//  {
//    uri = new URI("tcp://172.16.78.167:10251");
//
//    System.out.println("JSON String " + topic + " n:" + nodeId + " p:" + partition
//        + " o:" + offset + " pr:" + previousOffset + " uri:" + uri.toString());
//    Map<String, Object> objectizedRequest = new HashMap<String, Object>();
//    objectizedRequest.put(TOPIC_KEY, topic);
//    objectizedRequest.put(NODE_KEY, nodeId);
//    objectizedRequest.put(PARTITION_KEY, partition);
//    objectizedRequest.put(OFFSET_KEY, offset);
//    objectizedRequest.put(PREVIOUS_OFFSET_KEY, previousOffset);
//
//    System.out.println("JSON String 0");
//
//    if (uri == null)
//    {
//      throw new IOException("URI needs to be set for node " + nodeId);
//    }
//    objectizedRequest.put(URI_KEY, uri.toString());
//
//    ObjectMapper m = new ObjectMapper();
//    System.out.println("JsonString 1");
//    String output = m.writeValueAsString(objectizedRequest);
//
//    System.out.println("JsonString " + output);
//  }
//
//  private static void testSimpleConsumerOffset()
//  {
//    try
//    {
//      URI uri = new URI("tcp://172.16.78.170:10251");
//      SimpleConsumer consumer =
//          new SimpleConsumer(uri.getHost(), uri.getPort(), 30000, 1024 * 1024);
//
//      long[] earliestTime =
//          consumer.getOffsetsBefore("TrackingMonitoringEvent",
//                                    0,
//                                    OffsetRequest.EarliestTime(),
//                                    1);
//      long[] latestTime =
//          consumer.getOffsetsBefore("TrackingMonitoringEvent",
//                                    0,
//                                    OffsetRequest.LatestTime(),
//                                    1);
//      writeLongArray("Earliest offset ", earliestTime);
//      writeLongArray("Latest offset ", latestTime);
//      writeLongArray("Time before now ",
//                     consumer.getOffsetsBefore("TrackingMonitoringEvent",
//                                               0,
//                                               1303168253000l,
//                                               1));
//    }
//    catch (URISyntaxException e)
//    {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
//  }
//
//  private static void writeLongArray(String prefix, long[] array)
//  {
//    String text = "{";
//    for (long val : array)
//    {
//      text += val + ",";
//    }
//    text += "}";
//
//    System.out.println(prefix + text);
//  }
//
//  private static void testConnection()
//  {
//    EtlZkClient client = null;
//    try
//    {
//      client =
//          new EtlZkClient("esv4-be111.stg.linkedin.com:12913,esv4-be112.stg.linkedin.com:12913,esv4-be113.stg.linkedin.com:12913/kafka");
//    }
//    catch (IOException e)
//    {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
//    Map<String, URI> brokersToUri = client.getBrokersToUriMap();
//
//    System.out.println("**Brokers**");
//    for (Map.Entry<String, URI> entry : brokersToUri.entrySet())
//    {
//      System.out.println("Node: " + entry.getKey() + " Uri: " + entry.getValue());
//    }
//
//    System.out.println("\n**Topics**");
//    Map<String, List<EtlRequest>> request = client.getTopicKafkaRequests();
//    for (Map.Entry<String, List<EtlRequest>> entry : request.entrySet())
//    {
//      String topic = entry.getKey();
//      List<EtlRequest> requestKeys = entry.getValue();
//
//      System.out.println("*" + topic + "*");
//      for (EtlRequest requestKey : requestKeys)
//      {
//        System.out.println(requestKey);
//      }
//    }
//  }
//}
