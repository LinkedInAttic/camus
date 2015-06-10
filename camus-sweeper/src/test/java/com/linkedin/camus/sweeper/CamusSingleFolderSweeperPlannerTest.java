package com.linkedin.camus.sweeper;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import com.linkedin.camus.sweeper.utils.DateUtils;

import static org.junit.Assert.*;


public class CamusSingleFolderSweeperPlannerTest extends EasyMockSupport {
  @Test
  public void testCreateSweeperJobProps() throws Exception {
    FileSystem mockedFs = createMock(FileSystem.class);
    Path inputDir = new Path("inputDir");
    Path outputDir = new Path("outputDir");
    DateUtils dUtils = new DateUtils(new Properties());
    DateTime currentHour = dUtils.getCurrentHour();
    DateTimeFormatter hourFormatter = dUtils.getDateTimeFormatter("YYYY/MM/dd/HH");
    String hour = currentHour.minusHours(1).toString(hourFormatter);
    Path inputDirWithHour = new Path(inputDir, hour);
    Path outputDirWithHour = new Path(outputDir, hour);

    //inputDir should exist, but outputDir shouldn't.
    EasyMock.expect(mockedFs.exists(inputDir)).andReturn(true).once();
    EasyMock.expect(mockedFs.exists(outputDirWithHour)).andReturn(false).once();

    FileStatus mockedFileStatus = createMock(FileStatus.class);
    FileStatus[] fileStatuses = { mockedFileStatus };
    EasyMock.expect(mockedFs.globStatus((Path) EasyMock.anyObject())).andReturn(fileStatuses).once();

    EasyMock.expect(mockedFileStatus.getPath()).andReturn(inputDirWithHour).anyTimes();

    ContentSummary mockedContentSummary = createMock(ContentSummary.class);
    long dataSize = 100;
    EasyMock.expect(mockedContentSummary.getLength()).andReturn(dataSize).once();
    EasyMock.expect(mockedFs.getContentSummary(inputDirWithHour)).andReturn(mockedContentSummary).once();

    replayAll();

    String topic = "testTopic";

    List<Properties> jobPropsList =
        new CamusSingleFolderSweeperPlanner().setPropertiesLogger(new Properties(), Logger.getLogger("testLogger"))
            .createSweeperJobProps(topic, inputDir, outputDir, mockedFs);

    assertEquals(1, jobPropsList.size());

    Properties jobProps = jobPropsList.get(0);
    String topicAndHour = topic + ":" + hour;

    assertEquals(topic, jobProps.getProperty("topic"));
    assertEquals(topicAndHour, jobProps.getProperty(CamusSingleFolderSweeper.TOPIC_AND_HOUR));
    assertEquals(inputDirWithHour.toString(), jobProps.getProperty(CamusSingleFolderSweeper.INPUT_PATHS));
    assertEquals(outputDirWithHour.toString(), jobProps.getProperty(CamusSingleFolderSweeper.DEST_PATH));
  }
}
