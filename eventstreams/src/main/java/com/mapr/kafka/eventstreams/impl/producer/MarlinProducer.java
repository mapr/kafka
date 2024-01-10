/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.kafka.eventstreams.impl.producer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.kafka.eventstreams.impl.MarlinClient;

/*
 * This class implements all the Kafka APIs and translates them to internal
 * Marlin Producer calls. This is also where we do data structure transforms
 * between Marlin and Kafka.
 */
public class MarlinProducer<K, V> extends MarlinClient implements Producer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(MarlinProducer.class);

    private final Serializer<K> _keySerializer;
    private final Serializer<V> _valueSerializer;
    private final MarlinProducerImpl _producer;

    protected MarlinProducer(ProducerConfig config, Serializer<K> keySerializer,
                          Serializer<V> valueSerializer,
                          MarlinProducerImpl producer) throws KafkaException {
      LOG.debug("Starting Streams producer");
      _keySerializer = keySerializer;
      _valueSerializer = valueSerializer;
      _producer = producer;
    }

    public MarlinProducer(ProducerConfig config, Serializer<K> keySerializer,
                          Serializer<V> valueSerializer) throws KafkaException {
      LOG.debug("Starting Streams producer");
      _keySerializer = keySerializer;
      _valueSerializer = valueSerializer;
      _producer = new MarlinProducerImpl(config);
    }

    @Override
    public void initTransactions() {
      throw new KafkaException("initTransactions API not implemented");
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
      throw new KafkaException("beginTransaction API not implemented");
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                  String consumerGroupId) throws ProducerFencedException {
      throw new KafkaException("sendOffsetsToTransaction API not implemented");
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        throw new KafkaException("sendOffsetsToTransaction API not implemented");
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
      throw new KafkaException("commitTransaction API not implemented");
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
      throw new KafkaException("abortTransaction API not implemented");
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        byte[] serializedKey;
        try {
            serializedKey = _keySerializer.serialize(record.topic(), record.key());
        } catch (ClassCastException cce) {
            throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                                             " to class " +
                                             _keySerializer.getClass() + " specified in key.serializer");
        }
        byte[] serializedValue;
        try {
            serializedValue = _valueSerializer.serialize(record.topic(), record.value());
        } catch (ClassCastException cce) {
            throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                                             " to class " +
                                             _valueSerializer.getClass() + " specified in value.serializer");
        }

        int partID;
        if (record.partition() == null) {
          partID = -1;
        } else {
          partID = record.partition();
        }
        return _producer.send(record, partID, serializedKey,
                              serializedValue, callback);
    }

    @Override
    public void flush() {
      _producer.flush();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
    return _producer.getTopicInfo(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
      return _producer.metrics();
    }

    @Override
    public void close() {
      _producer.close();
    }

    @Override
    public void close(Duration timeout) {
      try {
        _producer.close(timeout);
      } catch (InterruptedException e) {
        throw new KafkaException("Failed to close kafka producer", e);
      }
    }
}



