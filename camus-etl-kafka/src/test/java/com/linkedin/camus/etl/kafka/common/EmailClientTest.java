/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.camus.etl.kafka.common;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.dumbster.smtp.SimpleSmtpServer;
import com.dumbster.smtp.SmtpMessage;


public class EmailClientTest {

  protected static final int PORT = 9966;

  @Test
  public void test() {
    String to = "some@example.com";
    String from = "sender@example.com";
    String subject = "subject";
    String content = "content";

    SimpleSmtpServer server = SimpleSmtpServer.start(PORT);

    Properties properties = new Properties();
    properties.put("alert.email.host", "localhost");
    properties.put("alert.email.port", Integer.toString(PORT));
    properties.put("alert.email.addresses", to);
    properties.put("alert.email.subject", subject);
    properties.put("alert.email.sender", from);
    properties.put("alert.on.topic.falling.behind", "true");

    EmailClient.setup(properties);
    EmailClient.sendEmail(content);

    try {
      Thread.sleep(1000);
    } catch(Exception e){

    }

    Assert.assertEquals(server.getReceivedEmailSize(), 1);
    SmtpMessage message = (SmtpMessage)server.getReceivedEmail().next();
    Assert.assertEquals(from, message.getHeaderValue("From"));
    Assert.assertEquals(to, message.getHeaderValue("To"));
    Assert.assertEquals(subject, message.getHeaderValue("Subject"));
    Assert.assertEquals(content, message.getBody());

    server.stop();
  }

}
