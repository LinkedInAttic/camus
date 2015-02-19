package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.schemaregistry.SchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

import java.nio.BufferUnderflowException;

import static org.junit.Assert.assertEquals;
import static org.easymock.EasyMock.*;

public class MessageDecoderHelperTest {


    @Test
    public void testWithSchema() {

        SchemaRegistry schemaRegistry = createMock(SchemaRegistry.class);
        Schema schema = SchemaBuilder.record("testRecord").namespace("my.name.space")
                .fields().name("field").type().stringType().noDefault().endRecord();
        expect(schemaRegistry.getSchemaByID("myTopic", "1751217253")).andReturn(schema);
        replay(schemaRegistry);

        KafkaAvroMessageDecoder kafkaAvroMessageDecoder = new KafkaAvroMessageDecoder();
        final byte[] bytes = "whatever".getBytes();
        bytes[0] = 0x0; // Magic byte
        KafkaAvroMessageDecoder.MessageDecoderHelper messageDecoderHelper = kafkaAvroMessageDecoder.new MessageDecoderHelper(schemaRegistry, "myTopic", bytes);
        KafkaAvroMessageDecoder.MessageDecoderHelper actualResult = messageDecoderHelper.invoke();
        verify(schemaRegistry);

        assertEquals("my.name.space", actualResult.getSchema().getNamespace());
        assertEquals(5, actualResult.getStart());
        assertEquals(bytes, actualResult.getBuffer().array());
        assertEquals(3, actualResult.getLength());

    }

    @Test(expected = BufferUnderflowException.class)
    public void testNoIdNoBody() {

        SchemaRegistry schemaRegistry = createMock(SchemaRegistry.class);
        replay(schemaRegistry);

        KafkaAvroMessageDecoder kafkaAvroMessageDecoder = new KafkaAvroMessageDecoder();
        final byte[] bytes = new byte[1];
        bytes[0] = 0x0; // Magic byte
        KafkaAvroMessageDecoder.MessageDecoderHelper messageDecoderHelper = kafkaAvroMessageDecoder.new MessageDecoderHelper(schemaRegistry, "myTopic", bytes);
        messageDecoderHelper.invoke();
        verify(schemaRegistry);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithNoMagicByte() {

        SchemaRegistry schemaRegistry = createMock(SchemaRegistry.class);

        KafkaAvroMessageDecoder kafkaAvroMessageDecoder = new KafkaAvroMessageDecoder();
        KafkaAvroMessageDecoder.MessageDecoderHelper messageDecoderHelper = kafkaAvroMessageDecoder.new MessageDecoderHelper(schemaRegistry, "myTopic", "whatever".getBytes());
        messageDecoderHelper.invoke();

    }

    @Test(expected = IllegalStateException.class)
    public void testUnknownSchemaId() {
        SchemaRegistry schemaRegistry = createMock(SchemaRegistry.class);
        final byte[] bytes = "whatever".getBytes();
        bytes[0] = 0x0; // Magic byte

        KafkaAvroMessageDecoder kafkaAvroMessageDecoder = new KafkaAvroMessageDecoder();
        KafkaAvroMessageDecoder.MessageDecoderHelper messageDecoderHelper = kafkaAvroMessageDecoder.new MessageDecoderHelper(schemaRegistry, "myTopic", bytes);
        messageDecoderHelper.invoke();

    }

}
