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

import com.linkedin.camus.schemaregistry.CachedSchemaResolver;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistryResolver;
import com.linkedin.camus.schemaregistry.SchemaResolver;

public class KafkaAvroMessageDecoder implements Configurable {
	protected Configuration conf;
	protected String topicName;
	protected Schema latestSchema;
	protected static SchemaResolver resolver = null;
	protected final DecoderFactory decoderFactory;

	public KafkaAvroMessageDecoder(Configuration conf, String topicName)
			throws KafkaMessageDecoderException {
		this.conf = conf;
		this.topicName = topicName;

		try {
			String schemaRegistryClassName = conf.get(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS);
			
			if (schemaRegistryClassName == null) {
				throw new IllegalArgumentException("You must provide a schema registry implementation by setting the following property: "
						+ KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS);
			}
			
			Constructor<?> constructor = Class.forName(schemaRegistryClassName)
					.getConstructor(Configuration.class);
			SchemaRegistry registry = (SchemaRegistry) constructor
					.newInstance(conf);

			registry.getLatestSchemaByTopic(topicName);
			SchemaRegistryResolver actualResolver = new SchemaRegistryResolver(
					registry, topicName);
			resolver = new CachedSchemaResolver(topicName, actualResolver);
		} catch (Exception e) {
			throw new KafkaMessageDecoderException(e);
		}

		decoderFactory = DecoderFactory.get();
	}

	private class MessageDecoderHelper {
		private Message _message;
		private ByteBuffer _buffer;
		private Schema _schema;
		private int _start;
		private int _length;
		private Schema _targetSchema;
		private static final byte MAGIC_BYTE = 0x0;

		public MessageDecoderHelper(Message message) {
			_message = message;
		}

		public ByteBuffer getBuffer() {
			return _buffer;
		}

		public Schema getSchema() {
			return _schema;
		}

		public int getStart() {
			return _start;
		}

		public int getLength() {
			return _length;
		}

		public Schema getTargetSchema() {
			return _targetSchema;
		}

		private ByteBuffer getByteBuffer(Message message) {
			ByteBuffer buffer = message.payload();
			if (buffer.get() != MAGIC_BYTE)
				throw new IllegalArgumentException("Unknown magic byte!");
			return buffer;
		}

		public MessageDecoderHelper invoke() {
			_buffer = getByteBuffer(_message);
			String id = Integer.toString(_buffer.getInt());
			_schema = resolver.resolve(id);
			if (_schema == null)
				throw new IllegalStateException("Unknown schema id: "
						+ id);

			_start = _buffer.position() + _buffer.arrayOffset();
			_length = _buffer.limit() - 5;
			
			// try to get a target schema, if any
			_targetSchema = resolver.getTargetSchema();
			return this;
		}
	}

	public CamusWrapper decode(Message message) {
		MessageDecoderHelper helper = new MessageDecoderHelper(message)
				.invoke();
		try {
			DatumReader<Record> reader = (helper.getTargetSchema() == null) ? new GenericDatumReader<Record>(
					helper.getSchema()) : new GenericDatumReader<Record>(
					helper.getSchema(), helper.getTargetSchema());

			return new CamusWrapper(reader.read(null, decoderFactory.binaryDecoder(helper
					.getBuffer().array(), helper.getStart(),
					helper.getLength(), null)));
		} catch (IOException e) {
			throw new RuntimeException(e);
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
