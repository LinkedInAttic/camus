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

import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;


/**
 * Handles sending emails on Camus events using logging libraries.
 *
 * To send a message call EmailLogger.sendEmail("message").
 */
public class EmailClient {

  private static Logger LOGGER = LoggerFactory.getLogger(EmailClient.class);

  private static String mailHost;
  private static String mailPort;
  private static List<String> emailList;
  private static String subjectLine;
  private static String senderEmail;
  private static boolean shouldSendEmail = false;

  public static void setup(Properties properties) {
    mailHost = properties.getProperty("alert.email.host", "localhost");
    mailPort = properties.getProperty("alert.email.port", "25");
    String emails = properties.getProperty("alert.email.addresses", "");
    if (!emails.isEmpty()) {
      emailList = Lists.newArrayList(emails.split(","));
    }
    subjectLine = properties.getProperty("alert.email.subject", "Camus topic falling behind alert");
    senderEmail = properties.getProperty("alert.email.sender", "camus@linkedin.com");
    shouldSendEmail = properties.getProperty("alert.on.topic.falling.behind", "true").equalsIgnoreCase("true");
  }

  public static void sendEmail(String content) {
    if (!shouldSendEmail) {
      LOGGER.warn("EmailLogger is disabled (email.send must be set to true) or has not been setup. " +
          "Will not send email. Message: " + content);
      return;
    }

    if (emailList == null) {
      LOGGER.warn("Email list is empty. " +
              "Will not send email. Message: " + content);
      return;
    }

    try {
      Properties props = System.getProperties();

      props.setProperty("mail.transport.protocol", "smtp");
      props.put("mail.smtp.host", mailHost);
      props.put("mail.smtp.port", mailPort);

      Session session = Session.getInstance(props, null);
      MimeMessage message = new MimeMessage(session);
      message.setFrom(new InternetAddress(senderEmail));

      for (String to : emailList) {
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(to, false));
      }
      message.setSubject(subjectLine);

      message.setContent(content, "text/html");
      Transport.send(message);
    } catch(Exception exception) {
      LOGGER.warn("Could not send requested email. Message: " + content, exception);
    }

  }

}
