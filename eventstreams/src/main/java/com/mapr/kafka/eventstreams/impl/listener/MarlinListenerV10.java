
package com.mapr.kafka.eventstreams.impl.listener;

import com.mapr.fs.proto.Marlinserver.MarlinConfigDefaults;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerInterceptors;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MarlinListenerV10<K,V> extends MarlinListener<K,V> {
  private static final Logger log = LoggerFactory.getLogger(MarlinListenerV10.class);

  public MarlinListenerV10(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,
      ConsumerInterceptors<?, ?> interceptors) {
    super(config, keyDeserializer, valueDeserializer,
        new MarlinListenerImplV10(config, interceptors, DeserializerToCDCOpenFormatType(valueDeserializer)));

    MarlinConfigDefaults mConfDef = MarlinConfigDefaults.getDefaultInstance();
    List<ConsumerPartitionAssignor> assignors = config.getConfiguredInstances(mConfDef.getPartitionAssignmentStrategy(),
        ConsumerPartitionAssignor.class);
    if (assignors.size() > 1) {
      throw new KafkaException("Multiple partition assignors is not supported!!");
    }
    if (!config.getString(mConfDef.getGroupID()).isEmpty()) {
      _groupMetadata = new ConsumerGroupMetadata(config.getString(mConfDef.getGroupID()));
      if (config.getBoolean(mConfDef.getClientSidePartitionAssignment())) {
        if (assignors.size() == 0) {
          throw new KafkaException(
              "Client-side partition assignment requires config partition.assignment.strategy to be set.");
        }
        if (config.getString(mConfDef.getClientSidePartitionAssignmentInternalStream()).isEmpty()) {
          throw new KafkaException(
              "Client-side partition assignment requires config streams.clientside.partition.assignment.internal.stream to be set.");
        }

        _clientSidePartitioningEnabled = true;
        _coordinator = new MarlinConsumerCoordinator(this, _listener, config.getString(mConfDef.getGroupID()), assignors,
            config.getString(mConfDef.getClientSidePartitionAssignmentInternalStream()), _groupMetadata);
      }
    }

    log.debug("MarlinListenerV10 constructor");
  }

  @Override
  protected <K, V> ConsumerRecord<K, V> generateConsumerRecord(String topic, ListenerRecord rec, K kkey, V kvalue,
      int serializedKeySize, int serializedValueSize) {
    MarlinListenerImplV10 impl = (MarlinListenerImplV10) _listener;
    TimestampType timestampType = impl.convertMarlinTimestampTypeToKafka(rec.timestampType());
    return new ConsumerRecord<K, V>(topic, rec.feedId(), rec.offset(), rec.timestamp(), timestampType, 0L,
        serializedKeySize, serializedValueSize, kkey, kvalue, rec.headers(), rec.producer());
  }
}
