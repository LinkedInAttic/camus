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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;


/**
 * Handles sending emails on Camus events using logging libraries
 *
 * To send a message call EmailLogger.log("message")
 * Set logger appender appropriately for sending the email message.
 *
 * Example log4j configuration
 * ----------------------------
 * log4j.appender.mail=org.apache.log4j.SMTPAppender
 * log4j.appender.mail.To=owner@example.com,someone@example.com
 * log4j.appender.mail.From=from@example.com
 * log4j.appender.mail.SMTPHost=smtp.example.com
 * log4j.appender.mail.SMTPUsername=user@example.com
 * log4j.appender.mail.SMTPPassword=password
 * log4j.appender.mail.Subject=Error in Camus
 * log4j.appender.mail.layout=org.apache.log4j.PatternLayout
 * log4j.com.linkedin.camus.email=mail
 *
 * Example logback configuration
 * -----------------------------
 * <configuration>
 *   <appender name="EMAIL" class="ch.qos.logback.classic.net.SMTPAppender">
 *     <evaluator class="ch.qos.logback.classic.boolex.OnMarkerEvaluator">
 *       <marker>EMAIL_OWNERS</marker>
 *     </evaluator>
 *     <smtpHost>smtp.example.com</smtpHost>
 *     <username>user@example.com</username>
 *     <password>password</password>
 *     <to>owner@example.com</to>
 *     <to>someone@example.com</to>
 *     <from>from@example.com</from>
 *     <subject>Error in Camus</subject>
 *     <layout class="ch.qos.logback.classic.PatternLayout">
 *       <pattern>%date %-5level %logger{35} - %message%n</pattern>
 *     </layout>
 *   </appender>
 *
 *   <root level="DEBUG">
 *     <appender-ref ref="EMAIL" />
 *   </root>
 * </configuration>
 *
 */
public class EmailLogger {
  private static final Logger EMAIL_LOGGER = LoggerFactory.getLogger("com.linkedin.camus.email");
  private static final String EMAIL_MARKER_TEXT = "EMAIL_OWNERS";
  private static final Marker EMAIL_MARKER = MarkerFactory.getMarker(EMAIL_MARKER_TEXT);

  public static void log(String message) {
    EMAIL_LOGGER.error(EMAIL_MARKER, message);
  }

}
