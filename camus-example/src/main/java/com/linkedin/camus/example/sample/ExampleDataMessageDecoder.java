package com.linkedin.camus.example.sample;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.BinaryDecoder;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;

import com.linkedin.camus.example.sample.ExampleData;

public class ExampleDataMessageDecoder extends MessageDecoder<Message, GenericRecord> {

    protected DecoderFactory decoderFactory;
    protected Schema schema;

    // schemas per topic
    public void init(Properties props, String topicName) {

	decoderFactory = DecoderFactory.get();

	// to do - actually establish a schema registry to better handle
	// multiple topics
        schema = new ExampleData().getSchema();

    }


    public CamusWrapper <GenericRecord> decode (Message message) {

	try {

	    ByteBuffer buffer = message.payload();
            byte [] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);

            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
            BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(bytes, null);
	    
            GenericRecord gr = datumReader.read(null, binaryDecoder);

	    CamusWrapper<GenericRecord> cw = new CamusWrapper<GenericRecord>(gr);

	    return cw;

	}  catch (IOException e) {
	    throw new MessageDecoderException(e);
	}

    }

}