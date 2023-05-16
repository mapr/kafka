package com.mapr.kafka.eventstreams.impl.producer;

import com.mapr.fs.jni.MarlinJniListener;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(EasyMockRunner.class)
public class MarlinProducerTest {
    private String topic = "/s:t";
    private ProducerRecord<String, String> stringStringRecord
            = new ProducerRecord(topic, 0,  "key", "value");
    private ProducerRecord<Integer, String> intStringRecord
            = new ProducerRecord(topic, 1,  12, "value");
    private ProducerRecord<String, Integer> stringIntRecord
            = new ProducerRecord(topic, 2,  "key", 12);

    @BeforeClass
    public static void staticSetUp(){
        //something going wrong when calling ShimLoader.load() so then we have ClassNotFound on one of inner classes
        //of MarlinJniListener. In order to prevent it just load it before like this
        MarlinJniListener.class.getDeclaredMethods();
    }

    @Test
    public void testSendThrowsExceptions() {
        StringSerializer stringSerializer = new StringSerializer();
        MarlinProducerImpl producerImpl = EasyMock.createNiceMock(MarlinProducerImpl.class);
        MarlinProducer marlinProducer = new MarlinProducer(getProducerConfig(stringSerializer, stringSerializer),
                stringSerializer, stringSerializer, producerImpl);

        try {
            marlinProducer.send(intStringRecord);
        } catch (Exception e) {
            assertEquals(SerializationException.class, e.getClass());
            assertEquals("Can't convert key of class java.lang.Integer to class class " +
                    "org.apache.kafka.common.serialization.StringSerializer specified in key.serializer", e.getMessage());
        }

        try {
            marlinProducer.send(stringIntRecord);
        } catch (Exception e) {
            assertEquals(SerializationException.class, e.getClass());
            assertEquals("Can't convert value of class java.lang.Integer to class class " +
                    "org.apache.kafka.common.serialization.StringSerializer specified in value.serializer", e.getMessage());
        }
    }

    @Test
    public void testIfJniMethodCalledWithParameters() {
        StringSerializer stringSerializer = new StringSerializer();
        byte[] serializedKey = stringSerializer.serialize("/s:t", "key");
        byte[] serializedValue = stringSerializer.serialize("/s:t", "value");
        MarlinProducerImpl producerImpl = EasyMock.mock(MarlinProducerImpl.class);
        MarlinProducer marlinProducer = new MarlinProducer(getProducerConfig(stringSerializer, stringSerializer),
                stringSerializer, stringSerializer, producerImpl);
        EasyMock.expect(producerImpl.send(stringStringRecord, 0, serializedKey,
                serializedValue, null)).andReturn(null).once();
        EasyMock.replay(producerImpl);

        marlinProducer.send(stringStringRecord);

        EasyMock.verify(producerImpl);
    }

    private ProducerConfig getProducerConfig(Serializer keySerializer, Serializer valueSerializer) {
        String keySerializerClassName = keySerializer.getClass().getName();
        String valueSerializerClassName = valueSerializer.getClass().getName();
        Properties props = new Properties();
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClassName);
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClassName);
        return new ProducerConfig(props);
    }
}
