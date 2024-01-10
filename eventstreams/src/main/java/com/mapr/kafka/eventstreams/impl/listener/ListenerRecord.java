package com.mapr.kafka.eventstreams.impl.listener;

import org.apache.kafka.common.header.Headers;

/**
 * A key/value pair to be received from Marlin. This consists of a topic name
 * and a feedId number, from which the
 * record is being received and an offset that points to the record in Marlin
 * feedId.
 */
public final class ListenerRecord {
    private final String topic;
    private final int feedId;
    private final long offset;
    private final long timestamp;
    private final int timestampType;
    private final byte[] key;       // In marlin the key is always a byte array
    private final byte[] value;     // In marlin the value is always a byte array
    private final Headers headers;
    private final String producer;

    /**
     * Create a record
     *
     * @param topic The topic this record is received from
     * @param feedId The feedId of the topic this record is received from
     * @param offset The offset of this record in the corresponding partition
     * @param timestamp The timestamp at which the msg was produced
     * @param key The record key
     * @param value The record contents
     * @param headers Record headers
     * @param producer The record producer
     */
    public ListenerRecord(String topic, int feedId,
                          long offset, long timestamp,
                          int timestampType,
                          byte[] key,
                          byte[] value,
                          Headers headers,
                          String producer) {
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null");
        this.topic = topic;
        this.feedId = feedId;
        this.offset = offset;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.key = key;
        this.value = value;
        this.headers = headers;
        this.producer = producer;
    }

    /**
     * The topic this record is received from
     */
    public String topic() {
        return this.topic;
    }

    /**
     * The feedId from which this record is received
     */
    public int feedId() {
        return this.feedId;
    }

    /**
     * The key (or null if no key is specified)
     */
    public byte[] key() {
        return key;
    }

    /**
     * The value
     */
    public byte[] value() {
        return value;
    }

    /**
     * The producer
     */
    public String producer() {
        return producer;
    }

    /**
     * Headers
     */
    public Headers headers() {
      return headers;
    }

    /**
     * The position of this record in the corresponding Kafka partition.
     */
    public long offset() {
        return offset;
    }

    /**
     * The timestamp at which  this record was produced
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * The timestamp type of the record
     */
    public int timestampType() {
      return timestampType;
    }

    @Override
    public String toString() {
        return "ListenerRecord" +
               "(topic = " + topic() +
               ", partition = " + feedId() +
               ", offset = " + offset() +
               ", timestamp = " + timestamp() +
               ", key = " + key +
               ", value = " + value +
               ", producer = " + producer +
               ")";
    }
}
