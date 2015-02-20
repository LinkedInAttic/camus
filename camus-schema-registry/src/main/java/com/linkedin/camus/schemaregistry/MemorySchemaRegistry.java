package com.linkedin.camus.schemaregistry;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This is an in-memory implementation of a SchemaRegistry. It has no
 * persistence. If you wish to make schema IDs match between executions, you
 * must issue register calls in the same order each time, as the schema IDs are
 * a long that's incremented on every register call.
 * 
 * @param <S>
 *            The type of the schema that this registry manages.
 */
public class MemorySchemaRegistry<S> implements SchemaRegistry<S> {
  private final Map<MemorySchemaRegistryTuple, S> schemasById;
  private final Map<String, MemorySchemaRegistryTuple> latest;
  private final AtomicLong ids;

  public void init(Properties props) {
  }

  public MemorySchemaRegistry() {
    this.schemasById = new ConcurrentHashMap<MemorySchemaRegistryTuple, S>();
    this.latest = new ConcurrentHashMap<String, MemorySchemaRegistryTuple>();
    this.ids = new AtomicLong(0);
  }

  @Override
  public String register(String topic, S schema) {
    long id = ids.incrementAndGet();
    MemorySchemaRegistryTuple tuple = new MemorySchemaRegistryTuple(topic, id);
    schemasById.put(tuple, schema);
    latest.put(topic, tuple);
    return Long.toString(id);
  }

  @Override
  public S getSchemaByID(String topicName, String idStr) {
    try {
      S schema = schemasById.get(new MemorySchemaRegistryTuple(topicName, Long.parseLong(idStr)));

      if (schema == null) {
        throw new SchemaNotFoundException();
      }

      return schema;
    } catch (NumberFormatException e) {
      throw new SchemaNotFoundException("Supplied a non-long id string.", e);
    }
  }

  @Override
  public SchemaDetails<S> getLatestSchemaByTopic(String topicName) {
    MemorySchemaRegistryTuple tuple = latest.get(topicName);

    if (tuple == null) {
      throw new SchemaNotFoundException();
    }

    S schema = schemasById.get(tuple);

    if (schema == null) {
      throw new SchemaNotFoundException();
    }

    return new SchemaDetails<S>(topicName, Long.toString(tuple.getId()), schema);
  }

  public class MemorySchemaRegistryTuple {
    private final String topicName;
    private final long id;

    public MemorySchemaRegistryTuple(String topicName, long id) {
      this.topicName = topicName;
      this.id = id;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + getOuterType().hashCode();
      result = prime * result + (int) (id ^ (id >>> 32));
      result = prime * result + ((topicName == null) ? 0 : topicName.hashCode());
      return result;
    }

    public String getTopicName() {
      return topicName;
    }

    public long getId() {
      return id;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      MemorySchemaRegistryTuple other = (MemorySchemaRegistryTuple) obj;
      if (!getOuterType().equals(other.getOuterType()))
        return false;
      if (id != other.id)
        return false;
      if (topicName == null) {
        if (other.topicName != null)
          return false;
      } else if (!topicName.equals(other.topicName))
        return false;
      return true;
    }

    private MemorySchemaRegistry getOuterType() {
      return MemorySchemaRegistry.this;
    }

    @Override
    public String toString() {
      return "MemorySchemaRegistryTuple [topicName=" + topicName + ", id=" + id + "]";
    }
  }
}
