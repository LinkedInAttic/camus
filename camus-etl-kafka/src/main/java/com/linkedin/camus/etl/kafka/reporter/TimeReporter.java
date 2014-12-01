package com.linkedin.camus.etl.kafka.reporter;

import java.util.Map;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskReport;

import com.linkedin.camus.etl.kafka.reporter.BaseReporter;

public class TimeReporter extends BaseReporter {
    public void report(Job job, Map<String, Long> timingMap) throws IOException {
        StringBuilder sb = new StringBuilder();

        sb.append("***********Timing Report*************\n");

        sb.append("Job time (seconds):\n");

        double preSetup = timingMap.get("pre-setup") / 1000;
        double getSplits = timingMap.get("getSplits") / 1000;
        double hadoop = timingMap.get("hadoop") / 1000;
        double commit = timingMap.get("commit") / 1000;
        double total = timingMap.get("total") / 1000;

        sb.append(String.format("    %12s %6.1f (%s)\n", "pre setup", preSetup,
            NumberFormat.getPercentInstance().format(preSetup / total)
                .toString()));
        sb.append(String.format("    %12s %6.1f (%s)\n", "get splits",
            getSplits,
            NumberFormat.getPercentInstance().format(getSplits / total)
                .toString()));
        sb.append(String.format("    %12s %6.1f (%s)\n", "hadoop job", hadoop,
            NumberFormat.getPercentInstance().format(hadoop / total)
                .toString()));
        sb.append(String.format("    %12s %6.1f (%s)\n", "commit", commit,
            NumberFormat.getPercentInstance().format(commit / total)
                .toString()));

        int minutes = (int) total / 60;
        int seconds = (int) total % 60;

        sb.append(String.format("Total: %d minutes %d seconds\n", minutes,
            seconds));

        JobClient client = new JobClient(new JobConf(job.getConfiguration()));

        TaskReport[] tasks = client.getMapTaskReports(JobID.downgrade(job
            .getJobID()));

        double min = Long.MAX_VALUE, max = 0, mean = 0;
        double minRun = Long.MAX_VALUE, maxRun = 0, meanRun = 0;
        long totalTaskTime = 0;
        TreeMap<Long, List<TaskReport>> taskMap = new TreeMap<Long, List<TaskReport>>();

        for (TaskReport t : tasks) {
          long wait = t.getStartTime() - timingMap.get("hadoop_start");
          min = wait < min ? wait : min;
          max = wait > max ? wait : max;
          mean += wait;

          long runTime = t.getFinishTime() - t.getStartTime();
          totalTaskTime += runTime;
          minRun = runTime < minRun ? runTime : minRun;
          maxRun = runTime > maxRun ? runTime : maxRun;
          meanRun += runTime;

          if (!taskMap.containsKey(runTime)) {
            taskMap.put(runTime, new ArrayList<TaskReport>());
          }
          taskMap.get(runTime).add(t);
        }

        mean /= tasks.length;
        meanRun /= tasks.length;

        // convert to seconds
        min /= 1000;
        max /= 1000;
        mean /= 1000;
        minRun /= 1000;
        maxRun /= 1000;
        meanRun /= 1000;

        sb.append("\nHadoop job task times (seconds):\n");
        sb.append(String.format("    %12s %6.1f\n", "min", minRun));
        sb.append(String.format("    %12s %6.1f\n", "mean", meanRun));
        sb.append(String.format("    %12s %6.1f\n", "max", maxRun));
        sb.append(String.format("    %12s %6.1f/%.1f = %.2f\n", "skew",
            meanRun, maxRun, meanRun / maxRun));

        sb.append("\nTask wait time (seconds):\n");
        sb.append(String.format("    %12s %6.1f\n", "min", min));
        sb.append(String.format("    %12s %6.1f\n", "mean", mean));
        sb.append(String.format("    %12s %6.1f\n", "max", max));

        CounterGroup totalGrp = job.getCounters().getGroup("total");

        long decode = totalGrp.findCounter("decode-time(ms)").getValue();
        long request = totalGrp.findCounter("request-time(ms)").getValue();
        long map = totalGrp.findCounter("mapper-time(ms)").getValue();
        long mb = totalGrp.findCounter("data-read").getValue();

        long other = totalTaskTime - map - request - decode;

        sb.append("\nHadoop task breakdown:\n");
        sb.append(String.format("    %12s %s\n", "kafka", NumberFormat
            .getPercentInstance().format(request / (double) totalTaskTime)));
        sb.append(String.format("    %12s %s\n", "decode", NumberFormat
            .getPercentInstance().format(decode / (double) totalTaskTime)));
        sb.append(String.format("    %12s %s\n", "map output", NumberFormat
            .getPercentInstance().format(map / (double) totalTaskTime)));
        sb.append(String.format("    %12s %s\n", "other", NumberFormat
            .getPercentInstance().format(other / (double) totalTaskTime)));

        sb.append(String.format("\n%16s %s\n", "Total MB read:",
            mb / 1024 / 1024));

        log.info(sb.toString());
    }
}
