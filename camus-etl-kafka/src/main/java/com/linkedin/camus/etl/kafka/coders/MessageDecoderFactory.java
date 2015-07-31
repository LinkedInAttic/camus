package com.linkedin.camus.etl.kafka.coders;

import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.mapreduce.JobContext;

import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;


public class MessageDecoderFactory {

  public static MessageDecoder<?, ?> createMessageDecoder(JobContext context, String topicName) {
    MessageDecoder<?, ?> decoder;
    try {
      decoder = (MessageDecoder<?, ?>) EtlInputFormat.getMessageDecoderClass(context, topicName).newInstance();

      Properties props = new Properties();
      for (Entry<String, String> entry : context.getConfiguration()) {
        props.put(entry.getKey(), entry.getValue());
      }

      decoder.init(props, topicName);

      return decoder;
    } catch (Exception e) {
      throw new MessageDecoderException(e);
    }
  }

}
