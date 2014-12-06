package com.linkedin.camus.etl.kafka.coders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.codec.binary.Hex;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.schemaregistry.CachedSchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import org.apache.hadoop.io.Text;

public class KafkaAvroMessageDecoder extends MessageDecoder<byte[], Record> {
	protected DecoderFactory decoderFactory;
	protected SchemaRegistry<Schema> registry;
	private Schema latestSchema;
	
	@Override
	public void init(Properties props, String topicName) {
	    super.init(props, topicName);
	    try {
            SchemaRegistry<Schema> registry = (SchemaRegistry<Schema>) Class
                    .forName(
                            props.getProperty(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS)).newInstance();
            
            registry.init(props);
            
            this.registry = new CachedSchemaRegistry<Schema>(registry);
            this.latestSchema = registry.getLatestSchemaByTopic(topicName).getSchema();
        } catch (Exception e) {
            throw new MessageDecoderException(e);
        }

        decoderFactory = DecoderFactory.get();
	}

	private class MessageDecoderHelper {
		//private Message message;
		private ByteBuffer buffer;
		private Schema schema;
		private int start;
		private int length;
		private Schema targetSchema;
		private static final byte MAGIC_BYTE_V0 = 0x0;
		private static final byte MAGIC_BYTE_V1 = 0x1;
		private final SchemaRegistry<Schema> registry;
		private final String topicName;
		private byte[] payload;

		public MessageDecoderHelper(SchemaRegistry<Schema> registry,
				String topicName, byte[] payload) {
			this.registry = registry;
			this.topicName = topicName;
			//this.message = message;
			this.payload = payload;
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

		private ByteBuffer getByteBuffer(byte[] payload) {
			return ByteBuffer.wrap(payload);
		
		}

		public MessageDecoderHelper invoke() {
			buffer = getByteBuffer(payload);
			byte magicByte = buffer.get();
			if (magicByte != MAGIC_BYTE_V0 || magicByte != MAGIC_BYTE_V1)
				throw new IllegalArgumentException("Unknown magic byte!");

			String id;
			if (magicByte == MAGIC_BYTE_V0) {
				// This is for backwards compatibility where the id was only represented
				// by an integer.
				id = Integer.toString(buffer.getInt());
			} else {
				// The new method is to get the length of the ID, then read that length
				// from the buffer into a Hex String.
				int idLength = buffer.getInt();
				byte[] dst = new byte[idLength];
				buffer.get(dst);
				id = Hex.encodeHexString(dst); 
			}

			schema = registry.getSchemaByID(topicName, id);
			if (schema == null)
				throw new IllegalStateException("Unknown schema id: " + id);

			start = buffer.position() + buffer.arrayOffset();
			length = buffer.limit() - 5;

			// try to get a target schema, if any
			targetSchema = latestSchema;
			return this;
		}
	}

	public CamusWrapper<Record> decode(byte[] payload) {
		try {
			MessageDecoderHelper helper = new MessageDecoderHelper(registry,
					topicName, payload).invoke();
			DatumReader<Record> reader = (helper.getTargetSchema() == null) ? new GenericDatumReader<Record>(
					helper.getSchema()) : new GenericDatumReader<Record>(
					helper.getSchema(), helper.getTargetSchema());

			return new CamusAvroWrapper(reader.read(null, decoderFactory
                    .binaryDecoder(helper.getBuffer().array(),
                            helper.getStart(), helper.getLength(), null)));
	
		} catch (IOException e) {
			throw new MessageDecoderException(e);
		}
	}

	public static class CamusAvroWrapper extends CamusWrapper<Record> {

	    public CamusAvroWrapper(Record record) {
            super(record);
            Record header = (Record) super.getRecord().get("header");
   	        if (header != null) {
               if (header.get("server") != null) {
                   put(new Text("server"), new Text(header.get("server").toString()));
               }
               if (header.get("service") != null) {
                   put(new Text("service"), new Text(header.get("service").toString()));
               }
            }
        }
	    
	    @Override
	    public long getTimestamp() {
	        Record header = (Record) super.getRecord().get("header");

	        if (header != null && header.get("time") != null) {
	            return (Long) header.get("time");
	        } else if (super.getRecord().get("timestamp") != null) {
	            return (Long) super.getRecord().get("timestamp");
	        } else {
	            return System.currentTimeMillis();
	        }
	    }
	}
}
