package com.linkedin.camus.schemaregistry;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class CachedSchemaRegistry<S> implements SchemaRegistry<S> {
  private static final String GETSCHEMA_BYID_MAX_RETIRES = "getschema.byid.max.retries";
  private static final String GETSCHEMA_BYID_MAX_RETIRES_DEFAULT = String
      .valueOf(Integer.MAX_VALUE);
  private static final String GETSCHEMA_BYID_MIN_INTERVAL_SECONDS = "getschema.byid.min.interval.seconds";
  private static final String GETSCHEMA_BYID_MIN_INTERVAL_SECONDS_DEFAULT = "0";

  private final SchemaRegistry<S> registry;
  private final ConcurrentHashMap<CachedSchemaTuple, S> cachedById;
  private final ConcurrentHashMap<String, SchemaDetails<S>> cachedLatest;
  private int getByIdMaxRetries;
  private int getByIdMinIntervalSeconds;
  private ConcurrentMap<String, FailedFetchHistory> failedFetchHistories;

  private class FailedFetchHistory {
    private int numOfAttempts;
    private long previousAttemptTime;

    private FailedFetchHistory(int numOfnumOfAttempts, long previousAttemptTime) {
      this.numOfAttempts = numOfnumOfAttempts;
      this.previousAttemptTime = previousAttemptTime;
    }
  }

  public void init(Properties props) {
    this.getByIdMaxRetries = Integer.parseInt(props.getProperty(
        GETSCHEMA_BYID_MAX_RETIRES, GETSCHEMA_BYID_MAX_RETIRES_DEFAULT));
    this.getByIdMinIntervalSeconds = Integer.parseInt(props.getProperty(
        GETSCHEMA_BYID_MIN_INTERVAL_SECONDS,
        GETSCHEMA_BYID_MIN_INTERVAL_SECONDS_DEFAULT));
  }

  public CachedSchemaRegistry(SchemaRegistry<S> registry) {
    this.registry = registry;
    this.cachedById = new ConcurrentHashMap<CachedSchemaTuple, S>();
    this.cachedLatest = new ConcurrentHashMap<String, SchemaDetails<S>>();
    this.getByIdMaxRetries = Integer
        .parseInt(GETSCHEMA_BYID_MAX_RETIRES_DEFAULT);
    getByIdMinIntervalSeconds = Integer
        .parseInt(GETSCHEMA_BYID_MIN_INTERVAL_SECONDS_DEFAULT);
    this.failedFetchHistories = new ConcurrentHashMap<String, FailedFetchHistory>();
  }

  public String register(String topic, S schema) {
    return registry.register(topic, schema);
  }

  public S getSchemaByID(String topic, String id) {
    CachedSchemaTuple cacheKey = new CachedSchemaTuple(topic, id);
    S schema = cachedById.get(cacheKey);
    if (schema == null) {
      if (shouldFetchFromSchemaRegistry(id)) {
        schema = fetchFromSchemaRegistry(topic, id);
      } else {
        throw new SchemaNotFoundException();
      }
    }
    return schema;
  }

  private boolean shouldFetchFromSchemaRegistry(String id) {
    if (!failedFetchHistories.containsKey(id)) {
      return true;
    }
    FailedFetchHistory failedFetchHistory = failedFetchHistories.get(id);
    boolean maxRetriesNotExceeded = failedFetchHistory.numOfAttempts < getByIdMaxRetries;
    boolean minRetryIntervalSatisfied = System.nanoTime()
        - failedFetchHistory.previousAttemptTime >= TimeUnit.SECONDS
        .toNanos(getByIdMinIntervalSeconds);
    return maxRetriesNotExceeded && minRetryIntervalSatisfied;
  }

  private S fetchFromSchemaRegistry(String topic, String id) {
    try {
      S schema = registry.getSchemaByID(topic, id);
      return schema;
    } catch (SchemaNotFoundException e) {
      addFetchToFailureHistory(id);
      throw new SchemaNotFoundException(e);
    }
  }

  private void addFetchToFailureHistory(String id) {
    if (!failedFetchHistories.containsKey(id)) {
      failedFetchHistories
          .put(id, new FailedFetchHistory(1, System.nanoTime()));
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
