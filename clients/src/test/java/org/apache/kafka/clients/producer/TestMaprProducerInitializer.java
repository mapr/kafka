package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

@SuppressWarnings("unchecked")
/*
    Use this class to initialize KafkaProducer when your tests require apache initialization inside the constructor.
    Our mapr producer do actual initialization separately (in initializeProducer) and if default stream is not specified
    it is not executed inside the constructor. But many unit tests require it to be called there. So just replace
    new KafkaProducer(...) with TestMaprProducerInitializer.newKafkaProducer(...) and initializeProducer()
    will be called right after constructor via reflection.
 */
public class TestMaprProducerInitializer {

    public static <K, V> KafkaProducer<K, V> newKafkaProducer(Map<?, ?> config,
                                                              Serializer<K> keySerializer,
                                                              Serializer<V> valueSerializer) {
        return newKafkaProducer(config, keySerializer, valueSerializer, null, null, null, Time.SYSTEM);
    }

    public static <K, V> KafkaProducer<K, V> newKafkaProducer(Map<?, ?> configs,
                                                              Serializer<K> keySerializer,
                                                              Serializer<V> valueSerializer,
                                                              ProducerMetadata metadata,
                                                              KafkaClient kafkaClient,
                                                              ProducerInterceptors<K, V> interceptors,
                                                              Time time) {
        KafkaProducer<K, V> producer = new KafkaProducer<>((Map<String, Object>) configs,
                keySerializer, valueSerializer, metadata, kafkaClient, interceptors, time);
        try {
            Method initializeProducer = producer.getClass().getDeclaredMethod("initializeProducer", String.class, KafkaClient.class);
            initializeProducer.setAccessible(true);
            initializeProducer.invoke(producer, "topic", kafkaClient);
        } catch (InvocationTargetException ite) {
            throw (RuntimeException) ite.getCause();
        } catch (ReflectiveOperationException roe) {
            throw new RuntimeException(roe);
        }
        return producer;
    }
}
