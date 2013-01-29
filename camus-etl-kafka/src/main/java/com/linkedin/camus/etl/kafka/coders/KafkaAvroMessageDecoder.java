package com.linkedin.camus.etl.kafka.coders;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.linkedin.camus.schemaregistry.CachedSchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistry;

public class KafkaAvroMessageDecoder implements Configurable {
	protected Configuration conf;
	protected String topicName;
	protected final DecoderFactory decoderFactory;
	protected final SchemaRegistry<Schema> registry;

	@SuppressWarnings("unchecked")
	public KafkaAvroMessageDecoder(Configuration conf, String topicName) {
		this.conf = conf;
		this.topicName = topicName;

		try {
			Constructor<?> constructor = Class
					.forName(
							conf.get(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS))
					.getConstructor(Configuration.class);
			SchemaRegistry<Schema> registry = (SchemaRegistry<Schema>) constructor
					.newInstance(conf);
			
			// The call below ensures the KafkaAvroMessageDecoder will fail to construct if
			// its schema registry fails to provide a schema for the specified topicName.
			registry.getLatestSchemaByTopic(topicName); 
			
			this.registry = new CachedSchemaRegistry<Schema>(registry);
		} catch (Exception e) {
			throw new KafkaMessageDecoderException(e);
		}

		decoderFactory = DecoderFactory.get();
	}

	private class MessageDecoderHelper {
		private Message message;
		private ByteBuffer buffer;
		private Schema schema;
		private int start;
		private int length;
		private Schema targetSchema;
		private static final byte MAGIC_BYTE = 0x0;
		private final SchemaRegistry<Schema> registry;
		private final String topicName;

		public MessageDecoderHelper(SchemaRegistry<Schema> registry,
				String topicName, Message message) {
			this.registry = registry;
			this.topicName = topicName;
			this.message = message;
		}

		public ByteBuffer getBuffer() {
			return buffer;
		}

		public Schema getSchema() {
			return schema;
		}

		public int getStart() {
			return start;
		}

		public int getLength() {
			return length;
		}

		public Schema getTargetSchema() {
			return targetSchema;
		}

		private ByteBuffer getByteBuffer(Message message) {
			ByteBuffer buffer = message.payload();
			if (buffer.get() != MAGIC_BYTE)
				throw new IllegalArgumentException("Unknown magic byte!");
			return buffer;
		}

		public MessageDecoderHelper invoke() {
			buffer = getByteBuffer(message);
			String id = Integer.toString(buffer.getInt());
			schema = registry.getSchemaByID(topicName, id);
			if (schema == null)
				throw new IllegalStateException("Unknown schema id: " + id);

			start = buffer.position() + buffer.arrayOffset();
			length = buffer.limit() - 5;

			// try to get a target schema, if any
			targetSchema = registry.getLatestSchemaByTopic(topicName)
					.getSchema();
			return this;
		}
	}

	public CamusWrapper decode(Message message) {
		try {
			MessageDecoderHelper helper = new MessageDecoderHelper(registry,
					topicName, message).invoke();
			DatumReader<Record> reader = (helper.getTargetSchema() == null) ? new GenericDatumReader<Record>(
					helper.getSchema()) : new GenericDatumReader<Record>(
					helper.getSchema(), helper.getTargetSchema());

			return new CamusWrapper(reader.read(null, decoderFactory
					.binaryDecoder(helper.getBuffer().array(),
							helper.getStart(), helper.getLength(), null)));
		} catch (IOException e) {
			throw new KafkaMessageDecoderException(e);
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

}
