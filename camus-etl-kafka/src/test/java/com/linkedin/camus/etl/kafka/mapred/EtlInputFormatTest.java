package com.linkedin.camus.etl.kafka.mapred;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class EtlInputFormatTest {

  @Test
  public void testEmptyWhitelistBlacklistEntries() {
    Configuration conf = new Configuration();
    conf.set(EtlInputFormat.KAFKA_WHITELIST_TOPIC, ",TopicA,TopicB,,TopicC,");
    conf.set(EtlInputFormat.KAFKA_BLACKLIST_TOPIC, ",TopicD,TopicE,,,,,TopicF,");

    String[] whitelistTopics = EtlInputFormat.getKafkaWhitelistTopic(conf);
    Assert.assertEquals(Arrays.asList("TopicA", "TopicB", "TopicC"),
                        Arrays.asList(whitelistTopics));

    String[] blacklistTopics = EtlInputFormat.getKafkaBlacklistTopic(conf);
    Assert.assertEquals(Arrays.asList("TopicD", "TopicE", "TopicF"),
                        Arrays.asList(blacklistTopics));
  }

}
