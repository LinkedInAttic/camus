package com.linkedin.camus.etl.kafka.common;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class DateUtils {
  public static DateTimeZone PST = DateTimeZone.forID("America/Los_Angeles");
  public static DateTimeFormatter MINUTE_FORMATTER = getDateTimeFormatter("YYYY-MM-dd-HH-mm");

  public static DateTimeFormatter getDateTimeFormatter(String str) {
    return getDateTimeFormatter(str, PST);
  }

  public static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
    return DateTimeFormat.forPattern(str).withZone(timeZone);
  }

  public static long getPartition(long timeGranularityMs, long timestamp) {
    return (timestamp / timeGranularityMs) * timeGranularityMs;
  }

  public static long getPartition(long timeGranularityMs, long timestamp, DateTimeZone outputDateTimeZone) {
    long adjustedTimeStamp = outputDateTimeZone.convertUTCToLocal(timestamp);
    long partitionedTime = (adjustedTimeStamp / timeGranularityMs) * timeGranularityMs;
    return outputDateTimeZone.convertLocalToUTC(partitionedTime, false);
  }

  public static DateTime getMidnight() {
    DateTime time = new DateTime(PST);
    return new DateTime(time.getYear(), time.getMonthOfYear(), time.getDayOfMonth(), 0, 0, 0, 0, PST);
  }
}
