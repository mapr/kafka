/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.producer.StreamsPartitioner;
import org.apache.kafka.common.Configurable;
import java.util.Map;

public class DefaultStreamsPartitioner implements StreamsPartitioner {

    public void configure(Map<String, ?> configs) {}

    /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes The serialized key to partition on( or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param numPartitions Number of partitions the topic has
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, int numPartitions) {
      // Just return topic partition -1, since the default behavior is for the C producer
      // to either to sticky round-robin or hash based on the key.
      return -1;
    }

    /**
     * This is called when partitioner is closed.
     */
    public void close() {}

}
