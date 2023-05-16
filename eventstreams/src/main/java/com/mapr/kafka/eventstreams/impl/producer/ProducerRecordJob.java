package com.mapr.kafka.eventstreams.impl.producer;

public class ProducerRecordJob {
  private boolean flushJob;
  private boolean flushJobDone;
  private MarlinProducerResultImpl result;
  private byte[] topic;
  private byte[] key;
  private byte[] value;
  private long timestamp;

  public ProducerRecordJob(MarlinProducerResultImpl res, byte[] k, byte[] v, long t) {
    flushJob = false;
    flushJobDone = false;
    result = res;
    key = k;
    value = v;
    timestamp = t;
  }

  public ProducerRecordJob() {
    flushJob = true;
    flushJobDone = false;
  }

  public boolean isFlushJob() {  return flushJob;  }
  public boolean getFlushJobDone() {  return flushJobDone; }
  public void markFlushJobDone() {  flushJobDone = true; }
  public byte[] getTopic() {
    if ((topic == null) && !flushJob)
      topic = result.getTopic().getBytes();
    return topic;
  }
  public byte[] getKey() {  return key;  }
  public byte[] getValue() {  return value;  }
  public MarlinProducerResultImpl getResult() {  return result; }
  public long getTimestamp() { return timestamp; }
  public int estimatedSizeInBytes() {
    return (topic == null ? 0 : topic.length) +
            (key == null ? 0 : key.length) +
            (value == null ? 0 : value.length);
  }
}
