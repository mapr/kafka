/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package org.apache.kafka.clients.producer;
import org.apache.kafka.common.Configurable;

/**
 * Partitioner Interface
 */

public interface StreamsPartitioner extends Configurable {

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
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, int numPartitions);

    /**
     * This is called when partitioner is closed.
     */
    public void close();

}
