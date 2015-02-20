package com.linkedin.camus.etl.kafka.common;

import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;

public class DateUtilsTest {

    @Test(expected = IllegalArgumentException.class)
    public void testGetPSTFormatterBadString() {
        DateUtils.getDateTimeFormatter("qwerty");
    }

    @Test
    public void testGetPSTFormatterShortString() {
        DateTimeFormatter actualResult = DateUtils.getDateTimeFormatter("yyyy-MM-dd");
        Assert.assertEquals("2004-05-03", actualResult.print(1083628800000L));
    }

    @Test
    public void testGetPartition() {
        long actualResult = DateUtils.getPartition(500000L, 1083628800000L);
        Assert.assertEquals(1083628500000L, actualResult);
    }

    @Test(expected = ArithmeticException.class)
    public void testGetPartitionWithZeroGranularity() {
        DateUtils.getPartition(0L, 1083628800000L);
    }

    @Test
    public void testGetPartitionWithZeroTimestamp() {
        long actualResult = DateUtils.getPartition(500000L, 0L);
        Assert.assertEquals(0L, actualResult);
    }


}
