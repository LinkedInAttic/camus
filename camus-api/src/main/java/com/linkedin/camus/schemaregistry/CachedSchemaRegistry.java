package com.linkedin.camus.schemaregistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class CachedSchemaRegistry<S> implements SchemaRegistry<S> {
  private static final String GET_SCHEMA_BY_ID_MAX_RETIRES = "get.schema.by.id.max.retries";
  private static final String GET_SCHEMA_BY_ID_MAX_RETIRES_DEFAULT = "3";
  private static final String GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS = "get.schema.by.id.min.interval.seconds";
  private static final String GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS_DEFAULT = "1";

  private final SchemaRegistry<S> registry;
  private final ConcurrentHashMap<CachedSchemaTuple, S> cachedById;
  private final ConcurrentHashMap<String, SchemaDetails<S>> cachedLatest;
  private int getByIdMaxRetries;
  private int getByIdMinIntervalSeconds;
  private Map<String, FailedFetchHistory> failedFetchHistories;

  private class FailedFetchHistory {
    private int numOfAttempts;
    private long previousAttemptTime;

    private FailedFetchHistory(int numOfnumOfAttempts, long previousAttemptTime) {
      this.numOfAttempts = numOfnumOfAttempts;
      this.previousAttemptTime = previousAttemptTime;
    }
  }

  public void init(Properties props) {
    this.getByIdMaxRetries =
        Integer.parseInt(props.getProperty(GET_SCHEMA_BY_ID_MAX_RETIRES, GET_SCHEMA_BY_ID_MAX_RETIRES_DEFAULT));
    this.getByIdMinIntervalSeconds =
        Integer.parseInt(props.getProperty(GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS,
            GET_SCHEMA_BY_ID_MIN_INTERVAL_SECONDS_DEFAULT));
  }

  public CachedSchemaRegistry(SchemaRegistry<S> registry, Properties props) {
    this.registry = registry;
    this.cachedById = new ConcurrentHashMap<CachedSchemaTuple, S>();
    this.cachedLatest = new ConcurrentHashMap<String, SchemaDetails<S>>();
    this.failedFetchHistories = new HashMap<String, FailedFetchHistory>();
    this.init(props);
  }

  public String register(String topic, S schema) {
    return registry.register(topic, schema);
  }

  public S getSchemaByID(String topic, String id) {
    CachedSchemaTuple cacheKey = new CachedSchemaTuple(topic, id);
    S schema = cachedById.get(cacheKey);
    if (schema == null) {
      synchronized (this) {
        if (shouldFetchFromSchemaRegistry(id)) {
          schema = fetchFromSchemaRegistry(topic, id);
          cachedById.putIfAbsent(new CachedSchemaTuple(topic, id), schema);
        } else {
          throw new SchemaNotFoundException();
        }
      }
    }
    return schema;
  }

  private synchronized boolean shouldFetchFromSchemaRegistry(String id) {
    if (!failedFetchHistories.containsKey(id)) {
      return true;
    }
    FailedFetchHistory failedFetchHistory = failedFetchHistories.get(id);
    boolean maxRetriesNotExceeded = failedFetchHistory.numOfAttempts < getByIdMaxRetries;
    boolean minRetryIntervalSatisfied =
        System.nanoTime() - failedFetchHistory.previousAttemptTime >= TimeUnit.SECONDS
            .toNanos(getByIdMinIntervalSeconds);
    return maxRetriesNotExceeded && minRetryIntervalSatisfied;
  }

  private synchronized S fetchFromSchemaRegistry(String topic, String id) {
    try {
      S schema = registry.getSchemaByID(topic, id);
      return schema;
    } catch (SchemaNotFoundException e) {
      addFetchToFailureHistory(id);
      throw e;
    }
  }

  private void addFetchToFailureHistory(String id) {
    if (!failedFetchHistories.containsKey(id)) {
      failedFetchHistories.put(id, new FailedFetchHistory(1, System.nanoTime()));
    } else {
      failedFetchHistories.get(id).numOfAttempts++;
      failedFetchHistories.get(id).previousAttemptTime = System.nanoTime();
    }
  }

  public SchemaDetails<S> getLatestSchemaByTopic(String topicName) {
    SchemaDetails<S> schema = cachedLatest.get(topicName);
    if (schema == null) {
      schema = registry.getLatestSchemaByTopic(topicName);
      cachedLatest.putIfAbsent(topicName, schema);
    }
    return schema;
  }

  public static class CachedSchemaTuple {
    private final String topic;
    private final String id;

    public CachedSchemaTuple(String topic, String id) {
      this.topic = topic;
      this.id = id;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((id == null) ? 0 : id.hashCode());
      result = prime * result + ((topic == null) ? 0 : topic.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      CachedSchemaTuple other = (CachedSchemaTuple) obj;
      if (id == null) {
        if (other.id != null)
          return false;
      } else if (!id.equals(other.id))
        return false;
      if (topic == null) {
        if (other.topic != null)
          return false;
      } else if (!topic.equals(other.topic))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "CachedSchemaTuple [topic=" + topic + ", id=" + id + "]";
    }
  }
}
