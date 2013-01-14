package com.linkedin.batch.etl.kafka.coders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import kafka.message.Message;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DirectBinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.log4j.Logger;

import com.linkedin.batch.etl.kafka.schemaregistry.SchemaDetails;
import com.linkedin.batch.etl.kafka.schemaregistry.SchemaNotFoundException;
import com.linkedin.batch.etl.kafka.schemaregistry.SchemaRegistry;
import com.linkedin.batch.etl.kafka.schemaregistry.SchemaRegistryException;

public class KafkaAvroMessageEncoder {

    private static final byte MAGIC_BYTE = 0x0;
    private static final Logger logger = Logger
            .getLogger(KafkaAvroMessageDecoder.class);

    private final SchemaRegistry client;
    private final Set<String> cache;
    private final String topic;

    public KafkaAvroMessageEncoder() {
        this(null, "");
    }

    public KafkaAvroMessageEncoder(SchemaRegistry client, String topic) {
        this.client = client;
        this.cache = Collections.synchronizedSet(new HashSet<String>());
        this.topic = topic;
    }

    public Message toMessage(IndexedRecord record) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(MAGIC_BYTE);
            /* TODO: this is expensive and should be cached somehow */
            byte[] schemaBytes = Utils.getBytes(
                    record.getSchema().toString(false), "UTF-8");
            logger.debug("Schema string UTF-8 bytes in hex: "
                    + Utils.hex(schemaBytes));
            byte[] md5Bytes = Utils.md5(schemaBytes);
            logger.debug("MD5 bytes in hex: " + Utils.hex(md5Bytes));
            String id = md5Bytes.toString();

            // auto-register schema if it is not in the registry, and we've got
            // a
            // registry client

            if (client != null && !cache.contains(id)) {
                try {
                    client.getSchemaByID(topic, id);
                } catch (SchemaNotFoundException e) {
                    try {
                        client.register(topic, schemaBytes.toString());
                    } catch (SchemaRegistryException E) {
                        throw new AvroIncompatibleSchemaException(
                                "Unable to register new schema, so aboring message conversion.");
                    }

                    cache.add(id);
                } catch (SchemaRegistryException E) {
                    throw new AvroIncompatibleSchemaException(
                            "Unable to register new schema, so aboring message conversion.");
                }
            }

            out.write(md5Bytes);
            BinaryEncoder encoder = new BinaryEncoder(out);
            DatumWriter<IndexedRecord> writer;
            if (record instanceof SpecificRecord) {
                writer = new SpecificDatumWriter<IndexedRecord>(
                        record.getSchema());
            } else {
                writer = new GenericDatumWriter<IndexedRecord>(
                        record.getSchema());
            }
            writer.write(record, encoder);
            return new Message(out.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
