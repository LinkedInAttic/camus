package com.linkedin.camus.etl.kafka;

import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.common.EtlCounts;
import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.linkedin.camus.etl.kafka.common.ExceptionWritable;
import com.linkedin.camus.etl.kafka.common.Source;
import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.Comparator;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;

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
	public static final String KAFKA_FETCH_REQUEST_MAX_WAIT = "kafka.fetch.request.max.wait";
	public static final String KAFKA_FETCH_REQUEST_MIN_BYTES = "kafka.fetch.request.min.bytes";
	public static final String KAFKA_FETCH_REQUEST_CORRELATION_ID = "kafka.fetch.request.correlationid";
	public static final String KAFKA_CLIENT_NAME = "kafka.client.name";
	public static final String KAFKA_FETCH_BUFFER_SIZE = "kafka.fetch.buffer.size";
	public static final String KAFKA_BROKERS = "kafka.brokers";
	public static final String KAFKA_HOST_URL = "kafka.host.url";
	public static final String KAFKA_HOST_PORT = "kafka.host.port";
	public static final String KAFKA_TIMEOUT_VALUE = "kafka.timeout.value";
	public static final String LOG4J_CONFIGURATION = "log4j.configuration";
	private static org.apache.log4j.Logger log = Logger
			.getLogger(CamusJob.class);

	private final Properties props;

	public CamusJob() {
		this.props = new Properties();
	}

	public CamusJob(Properties props) throws IOException {
		this.props = props;
	}

	private static HashMap<String, Long> timingMap = new HashMap<String, Long>();

	public static void startTiming(String name) {
		timingMap.put(name,
				(timingMap.get(name) == null ? 0 : timingMap.get(name))
						- System.currentTimeMillis());
	}

	public static void stopTiming(String name) {
		timingMap.put(name,
				(timingMap.get(name) == null ? 0 : timingMap.get(name))
						+ System.currentTimeMillis());
	}

	public static void setTime(String name) {
		timingMap.put(name,
				(timingMap.get(name) == null ? 0 : timingMap.get(name))
						+ System.currentTimeMillis());
	}

	private Job createJob(Properties props) throws IOException {
		
		if(getConf() == null)
		{
			Configuration conf = new Configuration();
			for(Object key : props.keySet())
			{
				conf.set(key.toString(), props.getProperty(key.toString()));
			}
			setConf(conf);
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(CamusJob.class);
		

		// Set the default partitioner
		job.getConfiguration().set(
				EtlMultiOutputFormat.ETL_DEFAULT_PARTITIONER_CLASS,
				"com.linkedin.camus.etl.kafka.coders.DefaultPartitioner");

		for (Object key : props.keySet()) {
			job.getConfiguration().set(key.toString(),
					props.getProperty(key.toString()));
		}
			
		job.setJobName("Camus Job");
		if(job.getConfiguration().get("camus.job.name") != null)
		{
			job.setJobName(job.getConfiguration().get("camus.job.name"));
		}
		
		
		FileSystem fs = FileSystem.get(job.getConfiguration());

		String hadoopCacheJarDir = job.getConfiguration().get(
				"hdfs.default.classpath.dir", null);
		if (hadoopCacheJarDir != null) {
			FileStatus[] status = fs.listStatus(new Path(hadoopCacheJarDir));

			if (status != null) {
				for (int i = 0; i < status.length; ++i) {
					if (!status[i].isDir()) {
						log.info("Adding Jar to Distributed Cache Archive File:"
								+ status[i].getPath());

						DistributedCache
								.addFileToClassPath(status[i].getPath(),
										job.getConfiguration(), fs);
					}
				}
			} else {
				System.out.println("hdfs.default.classpath.dir "
						+ hadoopCacheJarDir + " is empty.");
			}
		}

		// Adds External jars to hadoop classpath
		String externalJarList = job.getConfiguration().get(
				"hadoop.external.jarFiles", null);
		if (externalJarList != null) {
			String[] jarFiles = externalJarList.split(",");
			for (String jarFile : jarFiles) {
				log.info("Adding external jar File:" + jarFile);
				DistributedCache.addFileToClassPath(new Path(jarFile),
						job.getConfiguration(), fs);
			}
		}

		return job;
	}

	public void run() throws Exception {

		startTiming("pre-setup");
		startTiming("total");
		Job job = createJob(props);
		if (getLog4jConfigure(job)) {
			DOMConfigurator.configure("log4j.xml");
		}
		FileSystem fs = FileSystem.get(job.getConfiguration());

		log.info("Dir Destination set to: "
				+ EtlMultiOutputFormat.getDestinationPath(job));

		Path execBasePath = new Path(props.getProperty(ETL_EXECUTION_BASE_PATH));
		Path execHistory = new Path(
				props.getProperty(ETL_EXECUTION_HISTORY_PATH));

		if (!fs.exists(execBasePath)) {
			log.info("The execution base path does not exist. Creating the directory");
			fs.mkdirs(execBasePath);
		}
		if (!fs.exists(execHistory)) {
			log.info("The history base path does not exist. Creating the directory.");
			fs.mkdirs(execHistory);
		}

		// enforcing max retention on the execution directories to avoid
		// exceeding HDFS quota. retention is set to a percentage of available
		// quota.
		ContentSummary content = fs.getContentSummary(execBasePath);
		long limit = (long) (content.getQuota() * job.getConfiguration()
				.getFloat(ETL_EXECUTION_HISTORY_MAX_OF_QUOTA, (float) .5));
		limit = limit == 0 ? 50000 : limit;

		long currentCount = content.getFileCount()
				+ content.getDirectoryCount();

		FileStatus[] executions = fs.listStatus(execHistory);
		Arrays.sort(executions, new Comparator<FileStatus>() {
			public int compare(FileStatus f1, FileStatus f2) {
				return f1.getPath().getName().compareTo(f2.getPath().getName());
			}
		});
		
		// removes oldest directory until we get under required % of count
		// quota. Won't delete the most recent directory.
		for (int i = 0; i < executions.length - 1 && limit < currentCount; i++) {
			FileStatus stat = executions[i];
			log.info("removing old execution: " + stat.getPath().getName());
			ContentSummary execContent = fs.getContentSummary(stat.getPath());
			currentCount -= execContent.getFileCount()
					- execContent.getDirectoryCount();
			fs.delete(stat.getPath(), true);
		}

		// determining most recent execution and using as the starting point for
		// this execution
		if (executions.length > 0) {
			Path previous = executions[executions.length - 1].getPath();
			FileInputFormat.setInputPaths(job, previous);
			log.info("Previous execution: " + previous.toString());
		} else {
			System.out
					.println("No previous execution, all topics pulled from earliest available offset");
		}

		// creating new execution dir. offsets, error_logs, and count files will
		// be written to this directory. data is not written to the
		// output directory in a normal run, but instead written to the
		// appropriate date-partitioned subdir in camus.destination.path
		DateTimeFormatter dateFmt = DateUtils.getDateTimeFormatter(
				"YYYY-MM-dd-HH-mm-ss", DateTimeZone.UTC);
		Path newExecutionOutput = new Path(execBasePath,
				new DateTime().toString(dateFmt));
		FileOutputFormat.setOutputPath(job, newExecutionOutput);
		log.info("New execution temp location: "
				+ newExecutionOutput.toString());

		job.setInputFormatClass(EtlInputFormat.class);
		job.setOutputFormatClass(EtlMultiOutputFormat.class);
		job.setNumReduceTasks(0);

		stopTiming("pre-setup");
		job.submit();
		job.waitForCompletion(true);

		// dump all counters
		Counters counters = job.getCounters();
		for (String groupName : counters.getGroupNames()) {
			CounterGroup group = counters.getGroup(groupName);
			log.info("Group: " + group.getDisplayName());
			for (Counter counter : group) {
				log.info(counter.getDisplayName() + ":\t" + counter.getValue());
			}
		}

		stopTiming("hadoop");
		startTiming("commit");

		// Send Tracking counts to Kafka
		sendTrackingCounts(job, fs, newExecutionOutput);

		// Print any potentail errors encountered
		printErrors(fs, newExecutionOutput);

		fs.rename(newExecutionOutput, execHistory);

		log.info("Job finished");
		stopTiming("commit");
		stopTiming("total");
		createReport(job, timingMap);

		if (!job.isSuccessful()) {
			JobClient client = new JobClient(
					new JobConf(job.getConfiguration()));

			TaskCompletionEvent[] tasks = job.getTaskCompletionEvents(0);

			for (TaskReport task : client.getMapTaskReports(tasks[0]
					.getTaskAttemptId().getJobID())) {
				if (task.getCurrentStatus().equals(TIPStatus.FAILED)) {
					for (String s : task.getDiagnostics()) {
						System.err.println("task error: " + s);
					}
				}
			}
			throw new RuntimeException("hadoop job failed");
		}
	}

	public void printErrors(FileSystem fs, Path newExecutionOutput)
			throws IOException {
		for (FileStatus f : fs.listStatus(newExecutionOutput, new PrefixFilter(
				EtlMultiOutputFormat.ERRORS_PREFIX))) {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs,
					f.getPath(), fs.getConf());

			EtlKey key = new EtlKey();
			ExceptionWritable value = new ExceptionWritable();

			while (reader.next(key, value)) {
				System.err.println(key.toString());
				System.err.println(value.toString());
			}
			reader.close();
		}
	}

	// Posts the tracking counts to Kafka
	public void sendTrackingCounts(JobContext job, FileSystem fs,
			Path newExecutionOutput) throws IOException, URISyntaxException {
		if (EtlMultiOutputFormat.isRunTrackingPost(job)) {
			FileStatus[] gstatuses = fs.listStatus(newExecutionOutput,
					new PrefixFilter("counts"));
			HashMap<String, EtlCounts> allCounts = new HashMap<String, EtlCounts>();
			for (FileStatus gfileStatus : gstatuses) {
				FSDataInputStream fdsis = fs.open(gfileStatus.getPath());

				BufferedReader br = new BufferedReader(new InputStreamReader(
						fdsis), 1048576);
				StringBuffer buffer = new StringBuffer();
				String temp = "";
				while ((temp = br.readLine()) != null) {
					buffer.append(temp);
				}
				ObjectMapper mapper = new ObjectMapper();
				mapper.configure(
						DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES,
						false);
				ArrayList<EtlCounts> countsObjects = mapper.readValue(
						buffer.toString(),
						new TypeReference<ArrayList<EtlCounts>>() {
						});

				for (EtlCounts count : countsObjects) {
					String topic = count.getTopic();
					if (allCounts.containsKey(topic)) {
						EtlCounts existingCounts = allCounts.get(topic);
						existingCounts
								.setEndTime(Math.max(
										existingCounts.getEndTime(),
										count.getEndTime()));
						existingCounts.setLastTimestamp(Math.max(
								existingCounts.getLastTimestamp(),
								count.getLastTimestamp()));
						existingCounts.setStartTime(Math.min(
								existingCounts.getStartTime(),
								count.getStartTime()));
						existingCounts.setFirstTimestamp(Math.min(
								existingCounts.getFirstTimestamp(),
								count.getFirstTimestamp()));
						existingCounts.setErrorCount(existingCounts
								.getErrorCount() + count.getErrorCount());
						existingCounts.setGranularity(count.getGranularity());
						existingCounts.setTopic(count.getTopic());
						for (Entry<String, Source> entry : count.getCounts()
								.entrySet()) {
							Source source = entry.getValue();
							if (existingCounts.getCounts().containsKey(
									source.toString())) {
								Source old = existingCounts.getCounts().get(
										source.toString());
								old.setCount(old.getCount() + source.getCount());
								existingCounts.getCounts().put(old.toString(),
										old);
							} else {
								existingCounts.getCounts().put(
										source.toString(), source);
							}
							allCounts.put(topic, existingCounts);
						}
					} else {
						allCounts.put(topic, count);
					}
				}
			}

			for (FileStatus countFile : fs.listStatus(newExecutionOutput,
					new PrefixFilter("counts"))) {
				if (props.getProperty(ETL_KEEP_COUNT_FILES, "false").equals(
						"true")) {
					fs.rename(countFile.getPath(),
							new Path(props.getProperty(ETL_COUNTS_PATH),
									countFile.getPath().getName()));
				} else {
					fs.delete(countFile.getPath(), true);
				}
			}

			String brokerList = getKafkaBrokers(job);
			for (EtlCounts finalCounts : allCounts.values()) {
				finalCounts.postTrackingCountToKafka(job.getConfiguration(),
						props.getProperty(KAFKA_MONITOR_TIER), brokerList);
			}
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
	private void createReport(Job job, Map<String, Long> timingMap)
			throws IOException {
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

		options.addOption(OptionBuilder.withArgName("property=value")
				.hasArgs(2).withValueSeparator()
				.withDescription("use value for given property").create("D"));

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

	// Temporarily adding all Kafka parameters here
	public static boolean getPostTrackingCountsToKafka(Job job) {
		return job.getConfiguration().getBoolean(POST_TRACKING_COUNTS_TO_KAFKA,
				true);
	}

	public static int getKafkaFetchRequestMinBytes(JobContext context) {
		return context.getConfiguration().getInt(KAFKA_FETCH_REQUEST_MIN_BYTES,
				1024);
	}

	public static int getKafkaFetchRequestMaxWait(JobContext job) {
		return job.getConfiguration()
				.getInt(KAFKA_FETCH_REQUEST_MAX_WAIT, 1000);
	}

	public static String getKafkaBrokers(JobContext job) {
		String brokers = job.getConfiguration().get(KAFKA_BROKERS);
		if (brokers == null) {
			brokers = job.getConfiguration().get(KAFKA_HOST_URL);
			if (brokers != null) {
				log.warn("The configuration properties " + KAFKA_HOST_URL + " and " + 
					KAFKA_HOST_PORT + " are deprecated. Please switch to using " + KAFKA_BROKERS);
				return brokers + ":" + job.getConfiguration().getInt(KAFKA_HOST_PORT, 10251);
			}
		}
		return brokers;
	}

	public static int getKafkaFetchRequestCorrelationId(JobContext job) {
		return job.getConfiguration().getInt(
				KAFKA_FETCH_REQUEST_CORRELATION_ID, -1);
	}

	public static String getKafkaClientName(JobContext job) {
		return job.getConfiguration().get(KAFKA_CLIENT_NAME);
	}

	public static String getKafkaFetchRequestBufferSize(JobContext job) {
		return job.getConfiguration().get(KAFKA_FETCH_BUFFER_SIZE);
	}

	public static int getKafkaTimeoutValue(JobContext job) {
		int timeOut = job.getConfiguration().getInt(KAFKA_TIMEOUT_VALUE, 30000);
		return timeOut;
	}

	public static int getKafkaBufferSize(JobContext job) {
		return job.getConfiguration().getInt(KAFKA_FETCH_BUFFER_SIZE,
				1024 * 1024);
	}

	public static boolean getLog4jConfigure(JobContext job) {
		return job.getConfiguration().getBoolean(LOG4J_CONFIGURATION, false);
	}
}
