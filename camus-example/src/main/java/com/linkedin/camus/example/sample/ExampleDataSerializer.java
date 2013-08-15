package com.linkedin.camus.example.sample;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import kafka.message.Message;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.BinaryEncoder;

import com.linkedin.camus.example.sample.ExampleData;


public  class ExampleDataSerializer implements kafka.serializer.Encoder<ExampleData> {

    private final EncoderFactory encoderFactory = EncoderFactory.get();

    public Message toMessage(ExampleData dLog) {
        try {

	    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(outputStream, null);

	    DatumWriter<ExampleData> userDatumWriter = new SpecificDatumWriter<ExampleData>(ExampleData.class);
	    userDatumWriter.write(dLog, encoder);
	    encoder.flush();

	    byte[] encodedByteArray = outputStream.toByteArray();

            return new Message(encodedByteArray);

	} catch (IOException e) {
            e.printStackTrace();
            return null;   // TODO
        }
    }
}