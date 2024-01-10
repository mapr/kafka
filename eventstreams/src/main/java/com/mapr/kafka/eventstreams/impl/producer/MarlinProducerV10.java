package com.mapr.kafka.eventstreams.impl.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.Serializer;

public class MarlinProducerV10<K,V> extends MarlinProducer<K, V> {

    public MarlinProducerV10(ProducerConfig config, Serializer<K> keySerializer,
                          Serializer<V> valueSerializer) throws KafkaException {
      super(config, keySerializer, valueSerializer, new MarlinProducerImplV10(config));
    }
}
