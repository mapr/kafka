/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.kafka.eventstreams.impl.producer;

import com.mapr.fs.jni.MarlinProducerResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.DuplicateSequenceException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceResponse;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This structure maintains all the data structures related to a producer
 * request. Once marlin server comes back with a completion callback for a
 * request then this request is marked done and populated with all the metadata
 * coming from the server. The future returned to the caller waits on this done
 * call to actually return the request metadata.
 */

public class MarlinProducerResultImpl implements MarlinProducerResult {
  private final CountDownLatch latch = new CountDownLatch(1);

  /* User given data */
  protected final String topic;
  private final Callback callback;

  /* info filled by Marlin server using send response */
  protected volatile int feed = -1;
  protected volatile long offset = 0;
  protected volatile long timestamp = ConsumerRecord.NO_TIMESTAMP;

  /* Additional info used to return as part of RecordMetadata */
  protected int serializedKeySize;
  protected final int serializedValueSize;

  private volatile Exception error;

  public MarlinProducerResultImpl(String topic, int feed, Callback callback, int serKeySz, int serValSz) {
    this.topic = topic;
    this.feed = feed; 
    this.callback = callback;
    this.serializedKeySize = serKeySz;
    this.serializedValueSize = serValSz;
  }

  @Override
  public void done(int feedid,  long offset, long timestamp, Exception error) {
    if (feedid == -1 || this.feed == -1 || this.feed == feedid) {
      // System.out.println("MarlinProducerResult received " + feedid + " but was originally set to " + this.feed);
    }

    this.feed = feedid; 
    this.offset = offset;
    this.timestamp = timestamp;
    this.error = error;
    this.latch.countDown();

    if (error instanceof DuplicateSequenceException) {
      this.error = null;
      this.timestamp = RecordBatch.NO_TIMESTAMP;
      this.offset = ProduceResponse.INVALID_OFFSET;
    }
  }

  public String getTopic() {
    return topic;
  }

  public int getFeed() {
    return feed;
  }

  public RecordMetadata getRecordMetadata() {
    return new RecordMetadata(new TopicPartition(this.topic, this.feed),
                              this.offset, 0L);
  }

  /**
   * Await the completion of this request
   */
  public void await() throws InterruptedException {
      latch.await();
  }

  /**
   * Await the completion of this request (up to the given time interval)
   * @param timeout The maximum time to wait
   * @param unit The unit for the max time
   * @return true if the request completed, false if we timed out
   */
  public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
      return latch.await(timeout, unit);
  }

  /**
   * Has the request completed?
   */
  public boolean completed() {
      return this.latch.getCount() == 0L;
  }

  /**
   * The base offset for the request (the first offset in the record set)
   */
  public long offset() {
      return offset;
  }

  /**
   * The error thrown (generally on the server) while processing this request
   */
  public Exception error() {
      return error;
  }

  public Callback callback() {
    return callback;
  }

  @Override
  public void onCompletion() {
    if (callback != null)
      callback.onCompletion(getRecordMetadata(), error());
  }

}

