package com.mapr.kafka.eventstreams.impl.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

public class MarlinProducerResultImplV10 extends MarlinProducerResultImpl {

  public MarlinProducerResultImplV10(String topic, int feed, Callback callback,
                                     int serKeySz, int serValSz) {
    super(topic, feed, callback, serKeySz, serValSz);
  }

  @Override
  public RecordMetadata getRecordMetadata() {
    /*
     * TODO Send Timestamp/ serializedKeySize
     *  and serializedValueSize correctly.
     */
    return new RecordMetadata(new TopicPartition(this.topic, this.feed),
        this.offset, 0, this.timestamp,  serializedKeySize, serializedValueSize);
  }
}
