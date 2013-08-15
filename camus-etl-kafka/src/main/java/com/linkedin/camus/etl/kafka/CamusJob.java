package com.linkedin.camus.etl.kafka;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TIPStatus;
// import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.common.EtlCounts;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.EtlZkClient;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

public class CamusJob extends Configured implements Tool {

    public static final String ETL_EXECUTION_BASE_PATH = "etl.execution.base.path";
    public static final String ETL_EXECUTION_HISTORY_PATH = "etl.execution.history.path";
    public static final String ETL_COUNTS_PATH = "etl.counts.path";
    public static final String ETL_KEEP_COUNT_FILES = "etl.keep.count.files";
    public static final String ETL_EXECUTION_HISTORY_MAX_OF_QUOTA = "etl.execution.history.max.of.quota";
    public static final String ZK_AUDIT_HOSTS = "zookeeper.audit.hosts";
    public static final String KAFKA_MONITOR_TIER = "kafka.monitor.tier";
    public static final String CAMUS_MESSAGE_ENCODER_CLASS = "camus.message.encoder.class";
    public static final String BROKER_URI_FILE = "brokers.uri";
    public static final String POST_TRACKING_COUNTS_TO_KAFKA = "post.tracking.counts.to.kafka";
   
 private final Properties props;

    public CamusJob() {
        this.props = new Properties();
    }
    
    public CamusJob(Properties props) throws IOException {
        this.props = props;
    }

    private static HashMap<String, Long> timingMap = new HashMap<String, Long>();

    public static void startTiming(String name) {
        timingMap.put(
                name,
                (timingMap.get(name) == null ? 0 : timingMap.get(name))
                        - System.currentTimeMillis());
    }

    public static void stopTiming(String name) {
        timingMap.put(
                name,
                (timingMap.get(name) == null ? 0 : timingMap.get(name))
                        + System.currentTimeMillis());
    }

    public static void setTime(String name) {
        timingMap.put(
                name,
                (timingMap.get(name) == null ? 0 : timingMap.get(name))
                        + System.currentTimeMillis());
    }

    private void writeBrokers(FileSystem fs, Job job, List<URI> brokerURI) throws IOException {
        Path output = new Path(FileOutputFormat.getOutputPath(job), BROKER_URI_FILE);

        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(output)));

        for (URI u : brokerURI) {
            writer.write(u.toString());
            writer.newLine();
        }

        writer.close();
    }

    private List<URI> readBrokers(FileSystem fs, Job job) throws IOException, URISyntaxException {
        ArrayList<URI> brokerURI = new ArrayList<URI>();
        Path input = new Path(FileInputFormat.getInputPaths(job)[0], BROKER_URI_FILE);

        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(input)));

        String str;

        while ((str = reader.readLine()) != null) {
            brokerURI.add(new URI(str));
        }

        reader.close();

        return brokerURI;
    }

    private Job createJob(Properties props) throws IOException {
        Job job = new Job(getConf());
        job.setJarByClass(CamusJob.class);
        job.setJobName("Camus Job");

        // Set the default partitioner
        job.getConfiguration().set(EtlMultiOutputFormat.ETL_DEFAULT_PARTITIONER_CLASS, "com.linkedin.camus.etl.kafka.coders.DefaultPartitioner");

        for (Object key : props.keySet()) {
            job.getConfiguration().set(key.toString(), props.getProperty(key.toString()));
        }

        // For reasons not clear, just setting the property is not enough.
        if(props.containsKey("fs.defaultFS")) {
            FileSystem.setDefaultUri(job.getConfiguration(), props.get("fs.defaultFS").toString());
        }

        FileSystem fs = FileSystem.get(job.getConfiguration());

        String hadoopCacheJarDir = job.getConfiguration().get("hdfs.default.classpath.dir", null);
        if (hadoopCacheJarDir != null) {
            FileStatus[] status = fs.listStatus(new Path(hadoopCacheJarDir));

            if (status != null) {
                for (int i = 0; i < status.length; ++i) {
                    if (!status[i].isDir()) {
                        System.out.println("Adding Jar to Distributed Cache Archive File:"
                                + status[i].getPath());

                        DistributedCache.addFileToClassPath(status[i].getPath(),
                                job.getConfiguration(), fs);
                    }
                }
            } else {
                System.out
                        .println("hdfs.default.classpath.dir " + hadoopCacheJarDir + " is empty.");
            }
        }

        // Adds External jars to hadoop classpath
        String externalJarList = job.getConfiguration().get("hadoop.external.jarFiles", null);
        if (externalJarList != null) {
            String[] jarFiles = externalJarList.split(",");
            for (String jarFile : jarFiles) {
                System.out.println("Adding extenral jar File:" + jarFile);
                DistributedCache.addFileToClassPath(new Path(jarFile), job.getConfiguration(), fs);
            }
        }

        return job;
    }

    public void run() throws Exception {
        startTiming("pre-setup");
        startTiming("total");
        System.out.println("Starting Kafka ETL Job");

        Job job = createJob(props);
        FileSystem fs = FileSystem.get(job.getConfiguration());

        System.out.println("The blacklisted topics: "
                + Arrays.toString(EtlInputFormat.getKafkaBlacklistTopic(job)));
        System.out.println("The whitelisted topics: "
                + Arrays.toString(EtlInputFormat.getKafkaWhitelistTopic(job)));
        System.out.println("Dir Destination set to: "
                + EtlMultiOutputFormat.getDestinationPath(job));

        System.out.println("Getting the base paths.");

        Path execBasePath = new Path(props.getProperty(ETL_EXECUTION_BASE_PATH));
        Path execHistory = new Path(props.getProperty(ETL_EXECUTION_HISTORY_PATH));

        if (!fs.exists(execBasePath)) {
            System.out.println("The execution base path does not exist. Creating the directory");
            fs.mkdirs(execBasePath);
        }
        if (!fs.exists(execHistory)) {
            System.out.println("The history base path does not exist. Creating the directory.");
            fs.mkdirs(execHistory);
        }

        // enforcing max retention on the execution directories to avoid
        // exceeding HDFS quota. retention is set to a percentage of available
        // quota.
        ContentSummary content = fs.getContentSummary(execBasePath);
        long limit = (long) (content.getQuota() * job.getConfiguration().getFloat(
                ETL_EXECUTION_HISTORY_MAX_OF_QUOTA, (float) .5));
        limit = limit == 0 ? 50000 : limit;

        long currentCount = content.getFileCount() + content.getDirectoryCount();

        FileStatus[] executions = fs.listStatus(execHistory);
        Iterator<FileStatus> iter = Arrays.asList(fs.listStatus(execHistory)).iterator();

        // removes oldest directory until we get under required % of count
        // quota. Won't delete the most recent directory.
        for (int i = 0; i < executions.length - 1 && limit < currentCount; i++) {
            FileStatus stat = executions[i];
            System.out.println("removing old execution: " + stat.getPath().getName());
            ContentSummary execContent = fs.getContentSummary(stat.getPath());
            currentCount -= execContent.getFileCount() - execContent.getDirectoryCount();
            fs.delete(stat.getPath(), true);
        }

        // determining most recent execution and using as the starting point for
        // this execution
        if (executions.length > 0) {
            Path previous = executions[executions.length - 1].getPath();
            FileInputFormat.setInputPaths(job, previous);
            System.out.println("Previous execution: " + previous.toString());
        } else {
            System.out
                    .println("No previous execution, all topics pulled from earliest available offset");
        }

        // creating new execution dir. offsets, error_logs, and count files will
        // be written to this directory. data is not written to the
        // output directory in a normal run, but instead written to the
        // appropriate date-partitioned subdir in camus.destination.path
        DateTimeFormatter dateFmt = DateUtils.getDateTimeFormatter("YYYY-MM-dd-HH-mm-ss",
                DateTimeZone.UTC);
        Path newExecutionOutput = new Path(execBasePath, new DateTime().toString(dateFmt));
        FileOutputFormat.setOutputPath(job, newExecutionOutput);
        System.out.println("New execution temp location: " + newExecutionOutput.toString());

        job.setInputFormatClass(EtlInputFormat.class);
        job.setOutputFormatClass(EtlMultiOutputFormat.class);
        job.setOutputKeyClass(EtlKey.class);
        job.setOutputValueClass(AvroWrapper.class);

        job.setNumReduceTasks(0);

        stopTiming("pre-setup");
        job.submit();
        job.waitForCompletion(true);

        // dump all counters
        Counters counters = job.getCounters();
        for (String groupName : counters.getGroupNames()) {
            CounterGroup group = counters.getGroup(groupName);
            System.out.println("Group: " + group.getDisplayName());
            for (Counter counter : group) {
                System.out.println(counter.getDisplayName() + ":\t" + counter.getValue());
            }
        }

        stopTiming("hadoop");

        startTiming("commit");

        // this is for kafka-audit. currently that is only semi-open sourced,
        // but you can find more info
        // at https://issues.apache.org/jira/browse/KAFKA-260. For now, we
        // disable this section.
        if (EtlMultiOutputFormat.isRunTrackingPost(job)) {
            HashMap<String, EtlCounts> countsMap = new HashMap<String, EtlCounts>();

            for (FileStatus f : fs.listStatus(newExecutionOutput, new PrefixFilter(
                    EtlMultiOutputFormat.COUNTS_PREFIX))) {
                String topic = f.getPath().getName().split("\\.")[1];

                if (!countsMap.containsKey(topic)) {
                    countsMap.put(topic, new EtlCounts(job.getConfiguration(), topic,
                            EtlMultiOutputFormat.getMonitorTimeGranularityMins(job) * 60 * 1000L));
                }

                countsMap.get(topic).loadStreamingCountsFromDir(fs, f.getPath(), true);

                if (props.getProperty(ETL_KEEP_COUNT_FILES, "false").equals("true")) {
                    fs.rename(f.getPath(), new Path(props.getProperty(ETL_COUNTS_PATH), f.getPath()
                            .getName()));
                } else {
                    fs.delete(f.getPath(), true);
                }
            }

            List<URI> brokerURI;
            try {
                EtlZkClient zkClient = new EtlZkClient(props.getProperty(ZK_AUDIT_HOSTS),
                        EtlInputFormat.getZkSessionTimeout(job),
                        EtlInputFormat.getZkConnectionTimeout(job),
                        EtlInputFormat.getZkTopicPath(job), EtlInputFormat.getZkBrokerPath(job));
                brokerURI = new ArrayList<URI>(zkClient.getBrokersToUriMap().values());
            } catch (Exception e) {
                System.err
                        .println("Can't get brokers from zookeeper, using previously found brokers");
                brokerURI = readBrokers(fs, job);
            }

            writeBrokers(fs, job, brokerURI);
	    if(getPostTrackingCountsToKafka(job)) {
            	for (EtlCounts count : countsMap.values()) {
            		count.postTrackingCountToKafka(props.getProperty(KAFKA_MONITOR_TIER), brokerURI);
           	 }
            }

        }

        // echo any errors in the error files. hopefully there are none
        // TODO: we might want to add an option that throws an exception if
        // there
        // any errors recorded in these files, if the option is enabled
        for (FileStatus f : fs.listStatus(newExecutionOutput, new PrefixFilter(
                EtlMultiOutputFormat.ERRORS_PREFIX))) {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, f.getPath(), fs.getConf());

            EtlKey key = new EtlKey();
            ExceptionWritable value = new ExceptionWritable();

            while (reader.next(key, value)) {
                System.err.println(key.toString());
                System.err.println(value.toString());
            }
            reader.close();
        }

        // committing this job to the history directory. below we check if the
        // job failed,
        // but even a failed job will have some successfully completed work. Any
        // output in
        // the current execution dir can be safely committed to the history dir.
        fs.rename(newExecutionOutput, execHistory);

        System.out.println("Job finished");

        stopTiming("commit");

        stopTiming("total");
        createReport(job, timingMap);

        // the hadoop job should never fail, since all errors in the hadoop
        // tasks should be
        // isolated and logged in the error files. The downside is if a task
        // does fail, then
        // the OutputCommitter doesn't commit the error files, since failed
        // tasks don't commit
        // any files as rule. In this case, we have this block of code to to
        // echo any errors
        // we may have encountered.
        if (!job.isSuccessful()) {
            JobClient client = new JobClient(new JobConf(job.getConfiguration()));

            // TaskCompletionEvent[] tasks = job.getTaskCompletionEvents(0); This is
	    // incompatible with MR2. Setting maxEvents to 10 as per 
	    // https://issues.apache.org/jira/browse/MAPREDUCE-4932
            TaskCompletionEvent[] tasks = job.getTaskCompletionEvents(0, 10);

            // for (TaskReport task : client.getMapTaskReports(tasks[0].getTaskAttemptId().getJobID())) {
            for (TaskReport task : client.getMapTaskReports(tasks[0].getTaskAttemptId().getJobID().toString())) {
                if (task.getCurrentStatus().equals(TIPStatus.FAILED)) {
                    for (String s : task.getDiagnostics()) {
                        System.err.println("task error: " + s);
                    }
                }
            }
            throw new RuntimeException("hadoop job failed");
        }
    }

    /**
     * Creates a diagnostic report mostly focused on timing breakdowns. Useful
     * for determining where to optimize.
     * 
     * @param job
     * @param timingMap
     * @throws IOException
     */
    private void createReport(Job job, Map<String, Long> timingMap) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();

        sb.append("***********Timing Report*************\n");

        sb.append("Job time (seconds):\n");

        double preSetup = timingMap.get("pre-setup") / 1000;
        double getSplits = timingMap.get("getSplits") / 1000;
        double hadoop = timingMap.get("hadoop") / 1000;
        double commit = timingMap.get("commit") / 1000;
        double total = timingMap.get("total") / 1000;

        sb.append(String.format("    %12s %6.1f (%s)\n", "pre setup", preSetup, NumberFormat
                .getPercentInstance().format(preSetup / total).toString()));
        sb.append(String.format("    %12s %6.1f (%s)\n", "get splits", getSplits, NumberFormat
                .getPercentInstance().format(getSplits / total).toString()));
        sb.append(String.format("    %12s %6.1f (%s)\n", "hadoop job", hadoop, NumberFormat
                .getPercentInstance().format(hadoop / total).toString()));
        sb.append(String.format("    %12s %6.1f (%s)\n", "commit", commit, NumberFormat
                .getPercentInstance().format(commit / total).toString()));

        int minutes = (int) total / 60;
        int seconds = (int) total % 60;

        sb.append(String.format("Total: %d minutes %d seconds\n", minutes, seconds));

        JobClient client = new JobClient(new JobConf(job.getConfiguration()));

        TaskReport[] tasks = client.getMapTaskReports(JobID.downgrade(job.getJobID()));

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
        sb.append(String.format("    %12s %6.1f/%.1f = %.2f\n", "skew", meanRun, maxRun, meanRun
                / maxRun));

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
        sb.append(String.format("    %12s %s\n", "kafka",
                NumberFormat.getPercentInstance().format(request / (double) totalTaskTime)));
        sb.append(String.format("    %12s %s\n", "decode", NumberFormat.getPercentInstance()
                .format(decode / (double) totalTaskTime)));
        sb.append(String.format("    %12s %s\n", "map output", NumberFormat.getPercentInstance()
                .format(map / (double) totalTaskTime)));
        sb.append(String.format("    %12s %s\n", "other",
                NumberFormat.getPercentInstance().format(other / (double) totalTaskTime)));

        sb.append(String.format("\n%16s %s\n", "Total MB read:", mb / 1024 / 1024));

        System.out.println(sb.toString());
    }

    /**
     * Path filter that filters based on prefix
     */
    private class PrefixFilter implements PathFilter {
        private final String prefix;

        public PrefixFilter(String prefix) {
            this.prefix = prefix;
        }

        public boolean accept(Path path) {
            // TODO Auto-generated method stub
            return path.getName().startsWith(prefix);
        }
    }

    public static void main(String[] args) throws Exception {
        CamusJob job = new CamusJob();
        ToolRunner.run(job, args);
    }

    @SuppressWarnings("static-access")
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption("p", true, "properties filename from the classpath");
        options.addOption("P", true, "external properties filename");

        options.addOption(OptionBuilder.withArgName("property=value").hasArgs(2)
                .withValueSeparator().withDescription("use value for given property").create("D"));

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (!(cmd.hasOption('p') || cmd.hasOption('P'))) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("CamusJob.java", options);
            return 1;
        }

        if (cmd.hasOption('p'))
            props.load(this.getClass().getClassLoader().getResourceAsStream(
                    cmd.getOptionValue('p')));

        if (cmd.hasOption('P')) {
            File file = new File(cmd.getOptionValue('P'));
            FileInputStream fStream = new FileInputStream(file);
            props.load(fStream);
        }

        props.putAll(cmd.getOptionProperties("D"));

        run();
        return 0;
    }

    public static boolean getPostTrackingCountsToKafka(Job job){
    	return job.getConfiguration().getBoolean(POST_TRACKING_COUNTS_TO_KAFKA, true);
    }
}
