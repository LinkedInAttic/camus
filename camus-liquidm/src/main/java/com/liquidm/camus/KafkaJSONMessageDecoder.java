package com.liquidm.camus;

import org.codehaus.jackson.JsonNode;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import org.codehaus.jackson.map.ObjectMapper;

public class KafkaJSONMessageDecoder extends MessageDecoder<byte[], JSONRecord> {
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public CamusWrapper<JSONRecord> decode(byte[] message) {
    try {
      final JSONRecord record = new JSONRecord(mapper.readValue(message, JsonNode.class));

      Double rubyTs = Double.parseDouble(record.get("timestamp").toString());
      String msgType = record.get("type").toString();

      return new CamusWrapper<JSONRecord>(record, rubyTs.longValue() * 1000, "liquidm", msgType);
    } catch (Exception e) {
      throw new MessageDecoderException("Unable to deserialize event: " + message.toString(), e);
    }
  }
}