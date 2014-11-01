package com.linkedin.camus.sweeper.utils;

import java.util.Properties;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateUtils {
    public DateTimeZone zone;
    public DateTimeFormatter minuteFormatter;
    
    public DateUtils(Properties props)
    {
      zone = DateTimeZone.forID(props.getProperty("camus.default.timezone", "America/Los_Angeles"));
      minuteFormatter = getDateTimeFormatter("YYYY-MM-dd-HH-mm");
    }
    
    public DateTimeFormatter getDateTimeFormatter(String str) {
        return DateTimeFormat.forPattern(str).withZone(zone);
    }
    
    public static long getPartition(long timeGranularityMs, long timestamp) {
        return (timestamp / timeGranularityMs) * timeGranularityMs;
    }
    
    public DateTime getMidnight() {
    	DateTime time = new DateTime(zone);
    	return new DateTime(time.getYear(), time.getMonthOfYear(), time.getDayOfMonth(), 0, 0, 0, 0, zone);
    }
    public DateTime getCurrentTime(){
        return new DateTime(zone);
    }
}

