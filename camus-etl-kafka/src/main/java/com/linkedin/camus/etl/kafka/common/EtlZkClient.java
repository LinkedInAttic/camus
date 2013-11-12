package com.linkedin.camus.etl.kafka.common;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.regex.Pattern;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.log4j.Logger;

/**
 * Loads the topic and the node information from zookeeper.
 * 
 * @author Richard B Park
 */
public class EtlZkClient {
	public static final int DEFAULT_ZOOKEEPER_TIMEOUT = 30000;
	private static final String DEFAULT_ZOOKEEPER_TOPIC_PATH = "/brokers/topics";
	private static final String DEFAULT_ZOOKEEPER_BROKER_PATH = "/brokers/ids";
    private static final Set<String> DEFAULT_WHITE_LIST = Collections.EMPTY_SET;
    private static final Set<String> DEFAULT_BLACK_LIST = Collections.EMPTY_SET;

	private final String zkTopicPath;
	private final String zkBrokerPath;

	private ZkClient zkClient;
	private Map<String, List<EtlRequest>> topicToRequests = null;
	private Map<String, URI> brokerUri = null;
	private static Logger log = Logger.getLogger(EtlZkClient.class);

	/**
	 * Zookeeper client for Kafka
	 * 
	 * Sets the session timeout defaulted to 30s Sets connection timeout
	 * defaulted to 30s Zookeeper broker path defaulted to /brokers/ids
	 * Zookeeper topic path defaulted to /brokers/topics
	 * 
	 * @param zkHosts
	 *            The zookeeper host connection scheme.
	 * @throws IOException
	 */
	public EtlZkClient(String zkHosts) throws IOException {
		this(zkHosts, DEFAULT_ZOOKEEPER_TIMEOUT, DEFAULT_ZOOKEEPER_TIMEOUT);
	}

	/**
	 * Zookeeper client for Kafka
	 * 
	 * Zookeeper broker path defaulted to /brokers/ids Zookeeper topic path
	 * defaulted to /brokers/topics
	 * 
	 * @param zkHosts
	 *            The zookeeper host connection scheme.
	 * @param zkSessionTimeout
	 *            The session timeout in millisec
	 * @param zkConnectionTimeout
	 *            The connection timeout in millisec
	 * @throws IOException
	 *             Connection issues
	 */
	public EtlZkClient(String zkHosts, int zkSessionTimeout, int zkConnectionTimeout) throws IOException {
		this(zkHosts, zkSessionTimeout, zkConnectionTimeout, DEFAULT_ZOOKEEPER_TOPIC_PATH, DEFAULT_ZOOKEEPER_BROKER_PATH);
	}

	/**
	 * Zookeeper client for Kafka
	 * 
	 * @param zkHosts
	 *            The zookeeper host connection scheme.
	 * @param zkSessionTimeout
	 *            The session timeout in millisec
	 * @param zkConnectionTimeout
	 *            The connection timeout in millisec
	 * @param zkTopicPath
	 *            The zookeeper path for topics
	 * @param zkBrokerPath
	 *            The borker path for topics
	 * @throws IOException
	 */
	public EtlZkClient(String zkHosts, int zkSessionTimeout, int zkConnectionTimeout, String zkTopicPath, String zkBrokerPath) throws IOException {
		this(zkHosts, zkSessionTimeout, zkConnectionTimeout, zkTopicPath, zkBrokerPath, DEFAULT_WHITE_LIST, DEFAULT_BLACK_LIST);
	}

	/**
	 * Zookeeper client for Kafka
	 *
	 * @param zkHosts
	 *            The zookeeper host connection scheme.
	 * @param zkSessionTimeout
	 *            The session timeout in millisec
	 * @param zkConnectionTimeout
	 *            The connection timeout in millisec
	 * @param zkTopicPath
	 *            The zookeeper path for topics
	 * @param zkBrokerPath
	 *            The borker path for topics
	 * @param whiteListTopics
	 *            White list of topics to include in topic list
	 * @param whiteListTopics
	 *          black list of topics to include in topic list
	 * @throws IOException
	 */
	public EtlZkClient(String zkHosts, int zkSessionTimeout, int zkConnectionTimeout,
	                   String zkTopicPath, String zkBrokerPath, Set<String> whiteListTopics, Set<String> blackListTopics) throws IOException {
		this.zkTopicPath = zkTopicPath;
		this.zkBrokerPath = zkBrokerPath;

		try {
			zkClient = new ZkClient(zkHosts, zkSessionTimeout, zkConnectionTimeout, new BytesPushThroughSerializer());
			// Loads the Node Id's to URI
			loadKafkaNodes();

			// Loads the Topics available
			loadKafkaTopic(whiteListTopics, blackListTopics);
		} finally {
			zkClient.close();
		}
	}

	/**
	 * Return all the topics that are found in Zookeeper.
	 * 
	 * @return
	 */
	public List<String> getTopics() {
		ArrayList<String> topics = new ArrayList<String>(topicToRequests.keySet());

		return topics;
	}

	/**
	 * Returns the topics in the zookeeper that aren't in the blacklist.
	 * 
	 * @param blacklist
	 * @return
	 */
	public List<String> getTopics(Set<String> blacklist) {
		ArrayList<String> topics = new ArrayList<String>();
		for (String topic : topicToRequests.keySet()) {
			if (!matchesPattern(blacklist, topic, true)) {
				topics.add(topic);
			}
		}

		return topics;
	}

	public List<String> getTopics(Set<String> whitelist, Set<String> blacklist) {
		return getTopics(topicToRequests.keySet(), whitelist, blacklist);
	}

	public List<String> getTopics(Collection<String> topics, Set<String> whitelist, Set<String> blacklist) {
		ArrayList<String> filteredTopics = new ArrayList<String>();
		for (String topic : topics) {
			if (!matchesPattern(blacklist, topic, true) && matchesPattern(whitelist, topic, false)) {
				filteredTopics.add(topic);
			}
		}

		return filteredTopics;
	}

	private boolean matchesPattern(Set<String> list, String compare, boolean reverse) {
        if (list == null || list.isEmpty()) {
            return !reverse;
        }
		for (String pattern : list) {
			if (Pattern.matches(pattern, compare)) {
				return reverse;
			}
		}

		return !reverse;
	}

	// /**
	// * Returns the intersection of the whitelist and the topics in zookeeper
	// *
	// * @param blacklist
	// * @return
	// */
	// public List<String> getIntersectTopics(Set<String> whitelist) {
	// ArrayList<String> topics = new ArrayList<String>();
	// for (String topic: topicToRequests.keySet()) {
	// if (whitelist.contains(topic)) {
	// topics.add(topic);
	// }
	// }
	//
	// return topics;
	// }

	/**
	 * Returns a map of Topic to List of RequestKeys
	 * 
	 * @return
	 */
	public Map<String, List<EtlRequest>> getTopicKafkaRequests() {
		return topicToRequests;
	}

	/**
	 * Returns a map of Topic to List of RequestKeys
	 * 
	 * @return
	 */
	public List<EtlRequest> getKafkaRequest(String topic) {
		return topicToRequests.get(topic);
	}

	/**
	 * Returns a mapping of broker ids to maps.
	 * 
	 * @return
	 */
	public Map<String, URI> getBrokersToUriMap() {
		return brokerUri;
	}

	/**
	 * Load kafka brokers nodes
	 */
	private void loadKafkaNodes() throws IOException {
		List<String> brokers = zkClient.getChildren(zkBrokerPath);

		brokerUri = new HashMap<String, URI>();
		for (String id : brokers) {
			String brokerPath = zkBrokerPath + "/" + id;

			String nodeData = getZKString(brokerPath);
			if (nodeData == null) {
				throw new IOException("Node data from " + brokerPath + " is empty or null");
			}

			// The data is ip-timestamp:ip:port. We want to strip out the
			// ip:port.
			int index = nodeData.indexOf(':');
			String uriStr = "tcp://" + nodeData.substring(index + 1);

			URI uri = null;
			try {
				uri = new URI(uriStr);
			} catch (URISyntaxException e) {
				throw new IOException(e);
			}

			brokerUri.put(id, uri);
		}
	}

	/**
	 * Loading kafka topics
	 * 
	 * @throws IOException
	 * @param whiteListTopics
	 * @param blackListTopics
	 */
	private void loadKafkaTopic(Set<String> whiteListTopics, Set<String> blackListTopics) throws IOException {
		log.info("Getting topics from " + zkTopicPath);
		List<String> topics = getTopics(zkClient.getChildren(zkTopicPath), whiteListTopics, blackListTopics);
		log.info("Number of topics is " + topics.size());
		topicToRequests = new HashMap<String, List<EtlRequest>>();

		for (String topic : topics) {
			String topicPath = zkTopicPath + "/" + topic;
			List<String> nodeIds = zkClient.getChildren(topicPath);

			ArrayList<EtlRequest> requestKeys = new ArrayList<EtlRequest>();
			for (String nodeId : nodeIds) {
				String nodePath = topicPath + "/" + nodeId;
				String numPartitions = getZKString(nodePath);
				if (numPartitions == null) {
					log.error("Error on Topic: " + topic + ", Cannot find partitions in " + nodePath);
					continue;
				}

				URI uri = brokerUri.get(nodeId);
				if (uri == null) {
					throw new IOException("Error on Topic: " + topic + ", Broker uri doesn't exist for node " + nodeId);
				}
				int partition = Integer.parseInt(numPartitions);

				for (int i = 0; i < partition; ++i) {
					EtlRequest key = new EtlRequest(topic, nodeId, i, brokerUri.get(nodeId));
					requestKeys.add(key);
				}
			}

			topicToRequests.put(topic, requestKeys);
		}
	}

	/**
	 * Get String data from zookeeper
	 * 
	 * @param path
	 * @return
	 */
	private String getZKString(String path) {
		byte[] bytes = zkClient.readData(path);
		if (bytes == null) {
			return null;
		}
		String nodeData = new String(bytes);

		return nodeData;
	}
}
