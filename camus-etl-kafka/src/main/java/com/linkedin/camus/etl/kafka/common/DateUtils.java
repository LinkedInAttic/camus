package com.linkedin.camus.etl.kafka.common;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateUtils {
    // TODO:  Pacific time is probably a bad default.
    // UTC should be the default.
	public static DateTimeZone DEFAULT_TIMEZONE = DateTimeZone.forID("America/Los_Angeles");

	public static DateTimeFormatter MINUTE_FORMATTER = getDateTimeFormatter("YYYY-MM-dd-HH-mm");

	public static DateTimeFormatter getDateTimeFormatter(String str) {
		return getDateTimeFormatter(str, DEFAULT_TIMEZONE);
	}
	
	public static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
		return DateTimeFormat.forPattern(str).withZone(timeZone);
	}

	public static long getPartition(long timeGranularityMs, long timestamp) {
		return (timestamp / timeGranularityMs) * timeGranularityMs;
	}

	public static DateTime getMidnight() {
		DateTime time = new DateTime(DEFAULT_TIMEZONE);
		return new DateTime(time.getYear(), time.getMonthOfYear(), time.getDayOfMonth(), 0, 0, 0, 0, DEFAULT_TIMEZONE);
	}
}