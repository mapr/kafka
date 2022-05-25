package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

@SuppressWarnings("unchecked")
/*
    Use this class to initialize KafkaConsumer when your tests require apache initialization inside the constructor.
    Our mapr consumer do actual initialization separately (in initializeConsumer) and if default stream is not specified
    it is not executed inside the constructor. But many unit tests require it to be called there. So just replace
    new KafkaConsumer(...) with TestMaprConsumerInitializer.newKafkaConsumer(...) and initializeConsumer()
    will be called right after constructor via reflection.
 */
public class TestMaprConsumerInitializer {
    public static <K, V> KafkaConsumer<K, V> newKafkaConsumer(Map<?, ?> config,
                                                              Deserializer<K> keyDeserializer,
                                                              Deserializer<V> valueDeserializer) {
        KafkaConsumer<K, V> consumer = new KafkaConsumer<>((Map<String, Object>) config, keyDeserializer, valueDeserializer);
        try {
            Method initializeConsumer = consumer.getClass().getDeclaredMethod("initializeConsumer", String.class);
            initializeConsumer.setAccessible(true);
            initializeConsumer.invoke(consumer, "topic");
        } catch (InvocationTargetException ite) {
            throw (RuntimeException) ite.getCause();
        } catch (ReflectiveOperationException roe) {
            throw new RuntimeException(roe);
        }
        return consumer;
    }
}
