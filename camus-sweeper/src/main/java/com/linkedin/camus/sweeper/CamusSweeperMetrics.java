package com.linkedin.camus.sweeper;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;


public class CamusSweeperMetrics {

  private static final Logger LOG = Logger.getLogger(CamusSweeperMetrics.class);

  private final Map<String, Long> dataSizeByTopic;
  private final Map<String, Long> runnerStartTimeByTopic;
  private final Map<String, Long> mrSubmitTimeByTopic;
  private final Map<String, Long> mrStartRunningTimeByTopic;
  private final Map<String, Long> mrFinishTimeByTopic;

  private long totalDataSize;
  private long timeStart;

  CamusSweeperMetrics() {
    this.dataSizeByTopic = new ConcurrentHashMap<String, Long>();
    this.mrSubmitTimeByTopic = new ConcurrentHashMap<String, Long>();
    this.mrFinishTimeByTopic = new ConcurrentHashMap<String, Long>();
    this.runnerStartTimeByTopic = new ConcurrentHashMap<String, Long>();
    this.mrStartRunningTimeByTopic = new ConcurrentHashMap<String, Long>();
    timeStart = 0;
    totalDataSize = 0;
  }

  public long getTotalDataSize() {
    return totalDataSize;
  }

  public void addToTotalDataSize(long size) {
    this.totalDataSize += size;
  }

  public long getTimeStart() {
    return timeStart;
  }

  public void setTimeStart(long timeStart) {
    this.timeStart = timeStart;
  }

  public void recordDataSizeByTopic(String topic, long dataSize) {
    this.dataSizeByTopic.put(topic, dataSize);
  }

  public void recordRunnerStartTimeByTopic(String topic, long time) {
    this.runnerStartTimeByTopic.put(topic, time);
  }

  public void recordMrSubmitTimeByTopic(String topic, long time) {
    this.mrSubmitTimeByTopic.put(topic, time);
  }

  public void recordMrStartRunningTimeByTopic(String topic, long time) {
    this.mrStartRunningTimeByTopic.put(topic, time);
  }

  public void recordMrFinishTimeByTopic(String topic, long time) {
    this.mrFinishTimeByTopic.put(topic, time);
  }

  public void reportTotalRunningTime() {
    LOG.info("total running time: " + (System.currentTimeMillis() - this.timeStart) / 1000L + " sec");
  }

  public void reportTotalDataSize() {
    double totalDataSizeGB = (double) this.totalDataSize / 1024.0 / 1024.0 / 1024.0;
    LOG.info("total data size: " + String.format("%.2f", totalDataSizeGB) + "GB");
  }

  public void reportDataSizeByTopic() {
    this.reportDataSizeByTopic(Integer.MAX_VALUE);
  }

  public void reportDataSizeByTopic(int maxToReport) {
    LOG.info("Data Size By Topic:");
    List<Map.Entry<String, Long>> dataSizeByTopicSorted = sortByValueDesc(this.dataSizeByTopic);
    for (int i = 0; i < dataSizeByTopicSorted.size() && i < maxToReport; i++) {
      Map.Entry<String, Long> entry = dataSizeByTopicSorted.get(i);
      double dataSizeMB = (double) entry.getValue() / 1024.0 / 1024.0;
      LOG.info("  " + entry.getKey() + ": " + String.format("%.2f", dataSizeMB) + "MB");
    }
  }

  public void reportDurationFromStartToRunnerStart() {
    this.reportDurationFromStartToRunnerStart(Integer.MAX_VALUE);
  }

  public void reportDurationFromStartToRunnerStart(int maxToReport) {
    LOG.info("Time from start processing to runner starts By Topic:");
    Map<String, Long> durations = getDurations(this.timeStart, this.runnerStartTimeByTopic);
    reportDurations(durations, maxToReport);
  }

  public void reportDurationFromRunnerStartToMRSubmitted() {
    this.reportDurationFromRunnerStartToMRSubmitted(Integer.MAX_VALUE);
  }

  public void reportDurationFromRunnerStartToMRSubmitted(int maxToReport) {
    LOG.info("Time from runner start to MR submitted By Topic:");
    Map<String, Long> durations = getDurations(this.runnerStartTimeByTopic, this.mrSubmitTimeByTopic);
    reportDurations(durations, maxToReport);
  }

  public void reportDurationFromMRSubmittedToMRStarted() {
    this.reportDurationFromMRSubmittedToMRStarted(Integer.MAX_VALUE);
  }

  public void reportDurationFromMRSubmittedToMRStarted(int maxToReport) {
    LOG.info("Time from MR submitted to MR started running By Topic:");
    Map<String, Long> durations = getDurations(this.mrSubmitTimeByTopic, this.mrStartRunningTimeByTopic);
    reportDurations(durations, maxToReport);
  }

  public void reportDurationFromMRStartedToMRFinished() {
    this.reportDurationFromMRStartedToMRFinished(Integer.MAX_VALUE);
  }

  public void reportDurationFromMRStartedToMRFinished(int maxToReport) {
    LOG.info("Time from MR started running to MR finished By Topic:");
    Map<String, Long> durations = getDurations(this.mrStartRunningTimeByTopic, this.mrFinishTimeByTopic);
    reportDurations(durations, maxToReport);
  }

  private void reportDurations(Map<String, Long> durations, int maxToReport) {
    List<Map.Entry<String, Long>> durationsSorted = sortByValueDesc(durations);
    for (int i = 0; i < durations.size() && i < maxToReport; i++) {
      Map.Entry<String, Long> entry = durationsSorted.get(i);
      LOG.info("  " + entry.getKey() + ": " + entry.getValue() / 1000L + " sec");
    }
  }

  private Map<String, Long> getDurations(Map<String, Long> startTimes, Map<String, Long> endTimes) {
    Map<String, Long> durations = new HashMap<String, Long>();
    for (String topic : startTimes.keySet()) {
      if (endTimes.containsKey(topic)) {
        durations.put(topic, endTimes.get(topic) - startTimes.get(topic));
      }
    }
    return durations;
  }

  private Map<String, Long> getDurations(long startTime, Map<String, Long> endTimes) {
    Map<String, Long> durations = new HashMap<String, Long>();
    for (String topic : endTimes.keySet()) {
      durations.put(topic, endTimes.get(topic) - startTime);
    }
    return durations;
  }

  private static <K, V extends Comparable<? super V>> List<Map.Entry<K, V>> sortByValueDesc(Map<K, V> map) {
    List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
    Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
      public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
        return (o2.getValue()).compareTo(o1.getValue());
      }
    });
    return list;
  }
}
