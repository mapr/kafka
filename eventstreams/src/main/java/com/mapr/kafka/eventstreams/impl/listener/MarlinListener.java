package com.mapr.kafka.eventstreams.impl.listener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.Collection;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mapr.kafka.eventstreams.impl.MarlinClient;
import com.mapr.kafka.eventstreams.TopicRefreshRegexListener;
import com.mapr.kafka.eventstreams.TopicRefreshListListener;
import com.mapr.kafka.eventstreams.MapRCDCDeserializer;
import com.mapr.fs.proto.Dbserver.CDCOpenFormatType;
import com.mapr.fs.proto.Marlinserver.JoinGroupInfo;
import com.mapr.fs.proto.Marlinserver.JoinGroupResponse;
import com.mapr.fs.proto.Marlinserver.JoinGroupDesc;

/**
 * A Marlin wrapper that implements the Kafka Consumer Interface. This code will
 * internally pass all the calls to MarlinListenerImpl which actually implements the
 * functionality. This class is primarily responsible for converting input/output
 * data structures to and from Marlin/Kafka.
 */
public class MarlinListener<K, V> extends MarlinClient implements Consumer<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(MarlinListener.class);

    private final Deserializer<K> _keyDeserializer;
    private final Deserializer<V> _valueDeserializer;
    protected final MarlinListenerImpl _listener;
    protected MarlinConsumerCoordinator _coordinator;
    protected ConsumerGroupMetadata _groupMetadata;

    protected boolean _clientSidePartitioningEnabled = false;

    @SuppressWarnings("unchecked")
    protected MarlinListener(ConsumerConfig config,
                          Deserializer<K> keyDeserializer,
                          Deserializer<V> valueDeserializer,
                          MarlinListenerImpl listener) {
        LOG.debug("Starting Streams Listener");

        _keyDeserializer = keyDeserializer == null ?
         config.getConfiguredInstance(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
             Deserializer.class) : keyDeserializer;

        _valueDeserializer = valueDeserializer == null ?
        config.getConfiguredInstance(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Deserializer.class) :
        valueDeserializer;

        _listener = listener;

        LOG.debug("Streams listener created");
    }

    @SuppressWarnings("unchecked")
    public MarlinListener(ConsumerConfig config,
                          Deserializer<K> keyDeserializer,
                          Deserializer<V> valueDeserializer) {
      this(config, keyDeserializer, valueDeserializer,
           new MarlinListenerImpl(config, null /* Interceptors */, DeserializerToCDCOpenFormatType(valueDeserializer)));

    }

    public static CDCOpenFormatType DeserializerToCDCOpenFormatType(Deserializer<?> valueDeserializer) {
      // only 3 type supported now, default to json
      if (valueDeserializer == null) {
         return CDCOpenFormatType.COFT_NONE;
      }

      // for backward compatability with pre- kafka-eventstreams clients
      if (Arrays.stream(valueDeserializer.getClass().getInterfaces())
              .anyMatch(c -> "com.mapr.streams.MapRCDCDeserializer".equals(c.getName()))) {
        return CDCOpenFormatType.COFT_CDRECORD;
      }

      if (valueDeserializer instanceof MapRCDCDeserializer) {
         return ((MapRCDCDeserializer) valueDeserializer).getOpenFormatType();
      } else {
         //default to json
         return CDCOpenFormatType.COFT_JSON;
      }
    }

    @Override
    public Set<TopicPartition> assignment() {
      return _listener.assignment();
    }

    @Override
    public Set<String> subscription() {
      if (_clientSidePartitioningEnabled) {
        return _coordinator.subscription();
      } else {
        return _listener.subscription();
      }
    }

    @Override
    public void subscribe(Collection<String> topics) {
    if (_clientSidePartitioningEnabled) {
        _coordinator.subscribe(topics, new NoOpConsumerRebalanceListener());
      } else {
      subscribe(topics, new NoOpConsumerRebalanceListener());
      }
    }

    public void topicRefresherRegex(Pattern pattern, TopicRefreshRegexListener callback) {
      _listener.topicRefresherRegex(pattern, callback);
    }

    public void topicRefresherList(Collection<String> topics, TopicRefreshListListener callback) {
      _listener.topicRefresherList(topics, callback);
    }

    @Override
    public void subscribe(List<String> topics) {
    if (_clientSidePartitioningEnabled) {
        _coordinator.subscribe(topics, new NoOpConsumerRebalanceListener());
      } else {
      subscribe(topics, new NoOpConsumerRebalanceListener());
      }
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback){
    if (_clientSidePartitioningEnabled) {
        _coordinator.subscribe(topics, callback);
      } else {
      _listener.subscribe(topics, callback);
      }
    }

    @Override
    public void subscribe(List<String> topics, ConsumerRebalanceListener listener) {
    if (_clientSidePartitioningEnabled) {
        _coordinator.subscribe(topics, listener);
      } else {
      subscribe((Collection<String>)topics, listener);
      }
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
    if (_clientSidePartitioningEnabled) {
        _coordinator.assign(partitions);
      } else {
      _listener.assign(partitions);
      }
    }

    @Override
    public void assign(List<TopicPartition> partitions) {
    if (_clientSidePartitioningEnabled) {
        _coordinator.assign(partitions);
      } else {
      assign((Collection<TopicPartition>)partitions);
      }
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    if (_clientSidePartitioningEnabled) {
        _coordinator.subscribe(pattern, callback);
      } else {
      _listener.subscribe(pattern, callback);
      }
    }

    @Override
    public void subscribe(Pattern pattern) {
      throw new KafkaException("subscribe API not implemented");
    }

    @Override
    public void unsubscribe() {
    if (_clientSidePartitioningEnabled) {
        _coordinator.unsubscribe();
      } else {
      _listener.unsubscribe();
      }
    }

    @Override
    public ConsumerRecords<K, V> poll(long timeoutMS) {
      Map<TopicPartition, List<ListenerRecord>> marlinRecMap = _listener.poll(timeoutMS);
      Map<TopicPartition, List<ConsumerRecord<K, V>>> kafkaRecMap = new
        HashMap<TopicPartition, List<ConsumerRecord<K, V>>> ();

      Iterator it = marlinRecMap.entrySet().iterator();
      while (it.hasNext()) {
        boolean feedsAdded = false;
        Map.Entry entry = (Map.Entry)it.next();
        TopicPartition partition = (TopicPartition) entry.getKey();
        List<ListenerRecord> feedRecords = (List<ListenerRecord>)entry.getValue();
        List<ConsumerRecord<K, V>> partitionRecords = new ArrayList<ConsumerRecord<K, V>>();
        for (ListenerRecord feedRec : feedRecords) {
          feedsAdded = true;
          partitionRecords.add(toKafkaConsumerRecord(feedRec, _keyDeserializer,
                                                     _valueDeserializer));
        }
        if (feedsAdded)
          kafkaRecMap.put(partition, partitionRecords);
      }
      return new ConsumerRecords(kafkaRecMap);
    }

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
      return poll(timeout.getSeconds()*1000);
    }

    @Override
    public void commitSync() {
      _listener.commitSync();
    }

    @Override
    public void commitSync(Duration timeout) {
      //TODO MS-880 Handle timeout
      _listener.commitSync();
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
      _listener.commitSync(offsets);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
      //TODO MS-880 Handle timeout
      _listener.commitSync(offsets);
    }

    @Override
    public void commitAsync() {
      _listener.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
      _listener.commitAsync(callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
      _listener.commitAsync(offsets, callback);
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
      _listener.seek(partition, offset);
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
      _listener.seek(partition, offsetAndMetadata.offset());
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
      _listener.seekToBeginning(partitions);
    }

    @Override
    @Deprecated
    public void seekToBeginning(TopicPartition... partitions) {
      seekToBeginning(Arrays.asList(partitions));
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
      _listener.seekToEnd(partitions);
    }

    @Override
    @Deprecated
    public void seekToEnd(TopicPartition... partitions) {
      seekToEnd(Arrays.asList(partitions));
    }

    @Override
    public long position(TopicPartition partition) {
      return _listener.position(partition);
    }

    @Override
    public long position(TopicPartition partition, final Duration timeout) {
      //TODO MS-880 Handle timeout
      return _listener.position(partition);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
      return _listener.committed(partition);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, final Duration timeout) {
      //TODO MS-880 Handle timeout
      return _listener.committed(partition);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(partitions.size());
      for (TopicPartition p : partitions) {
        if (p != null) {
          offsets.put(p, _listener.committed(p));
        }
      }

      return offsets;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, final Duration timeout) {
      //TODO MS-880 Handle timeout
      return committed(partitions);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
      return _listener.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
      return _listener.getTopicInfo(topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
      //TODO MS-880 Handle timeout
      return _listener.getTopicInfo(topic);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
      return _listener.listTopics();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
      //TODO MS-880 Handle timeout
      return _listener.listTopics();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(String stream) {
      return _listener.listTopics(stream);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(String stream, Duration timeout) {
      //TODO MS-880 Handle timeout
      return _listener.listTopics(stream);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Pattern pattern) {
      return _listener.listTopics(pattern);
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
      _listener.pause(partitions);
    }

    @Override
    @Deprecated
    public void pause(TopicPartition... partitions) {
      pause(Arrays.asList(partitions));
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
      _listener.resume(partitions);
    }

    @Override
    @Deprecated
    public void resume(TopicPartition... partitions) {
      resume(Arrays.asList(partitions));
    }

    @Override
    public Set<TopicPartition> paused() {
      return _listener.paused();
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
      return _listener.offsetsForTimes(timestampsToSearch);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
      int tmillis = ((int)timeout.getSeconds())*1000;
      return _listener.offsetsForTimes(timestampsToSearch, tmillis);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
      return _listener.beginningOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
      int tmillis = ((int)timeout.getSeconds())*1000;
      return _listener.beginningOffsets(partitions, tmillis);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
      return _listener.endOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
      int tmillis = ((int)timeout.getSeconds())*1000;
      return _listener.endOffsets(partitions, tmillis);
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
      if (_groupMetadata == null) {
        throw new InvalidGroupIdException("To use the group management, you must " +
            "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
      }
      return _groupMetadata;
    }

    @Override
    public void enforceRebalance() {
      if (_coordinator == null) {
        throw new KafkaException("Tried to force a rebalance but consumer does not have a group.");
      }
      _coordinator.requestRejoin();
    }

    @Override
    public void close() {
      if (_clientSidePartitioningEnabled) {
        _coordinator.close();
      }
      _listener.close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close(long timeout, TimeUnit timeUnit) {
      if (_clientSidePartitioningEnabled) {
        _coordinator.close();
      }
      _listener.close(timeout, timeUnit);
    }

    @Override
    public void close(Duration timeout) {
      if (_clientSidePartitioningEnabled) {
        _coordinator.close();
      }
      _listener.close(timeout.getSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void wakeup() {
      _listener.wakeup();
    }

    public JoinGroupResponse join(JoinGroupDesc desc, MarlinJoinCallback cb) {
      return _listener.join(desc, cb);
    }

    protected <K, V> ConsumerRecord<K, V>
    generateConsumerRecord(String topic, ListenerRecord rec, K kkey, V kvalue,
                            int serializedKeySize,
                            int serializedValueSize) {
        return new ConsumerRecord<K, V>(topic, rec.feedId(), rec.offset(),
                                        kkey, kvalue,
                                        rec.timestamp(), rec.producer());
    }

    public <K, V> ConsumerRecord<K, V>
      toKafkaConsumerRecord(ListenerRecord rec, Deserializer<K> keyDeserializer,
                            Deserializer<V> valueDeserializer) {
        K kkey = null;
        byte[] key = rec.key();
        byte[] value = rec.value();
        String topic = rec.topic();
        if (key != null) {
          kkey = keyDeserializer.deserialize(topic, key);
        }

        V kvalue = null;
        if (value != null) {
          kvalue = valueDeserializer.deserialize(topic, value);
        }

        return generateConsumerRecord(topic, rec, kkey, kvalue,
                                      key == null ? ConsumerRecord.NULL_SIZE : key.length,
                                      value == null ? ConsumerRecord.NULL_SIZE : value.length);
    }

    public interface MarlinJoinCallback {
      public void onJoin(JoinGroupInfo joinInfo);
      public void onRejoin(JoinGroupInfo joinInfo);
    }
}

