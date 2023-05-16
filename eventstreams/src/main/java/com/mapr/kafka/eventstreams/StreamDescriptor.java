/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.kafka.eventstreams;

/**
 * A StreamDescriptor contains the details about a stream such as the
 * ttl, compression algo, default number of partitions per topic and so on.
 */
public interface StreamDescriptor extends Cloneable {
  /**
   * @return the default number of partitions per topic.
   */
  public int getDefaultPartitions();

  /**
   * Sets the default number of partitions per topic.
   * @param numPartitions the number of partitions
   */
  public void setDefaultPartitions(int numPartitions);

  /**
   * @return time-to-live for messages in seconds.
   */
  public long getTimeToLiveSec();

  /**
   * Sets the time-to-live for messages.
   * @param sec the time to live
   */
  public void setTimeToLiveSec(long sec);

  /**
   * @return the compression algo.
   */
  public String getCompressionAlgo();

  /**
   * Sets the compression algo.
   * @param algo the compression algorithm name.
   */
  public void setCompressionAlgo(String algo);

  /**
   * @return {@code true} if topics should be auto-created when a message is
   * produced.
   */
  public boolean getAutoCreateTopics();

  /**
   * Sets if topics should be auto-created when a message is produced.
   * @param allow {@code true} if topics should be auto-created.
   */
  public void setAutoCreateTopics(boolean allow);

  /**
   * @return the producer permission ace.
   */
  public String getProducePerms();

 /**
   * Sets the producer permission ace.
   * @param perms the producer permission ace.
   */
  public void setProducePerms(String perms);

  /**
   * @return the consume permission ace.
   */
  public String getConsumePerms();

 /**
   * Sets the consume permission ace.
   * @param perms the consume permission ace.
   */
  public void setConsumePerms(String perms);

  /**
   * @return the topic permission ace.
   */
  public String getTopicPerms();

 /**
   * Sets the topic permission ace.
   * @param perms the topic permission ace.
   */
  public void setTopicPerms(String perms);

  /**
   * @return the copy permission ace.
   */
  public String getCopyPerms();

 /**
   * Sets the copy permission ace.
   * @param perms the copy permission ace.
   */
  public void setCopyPerms(String perms);

  /**
   * @return the admin permission ace.
   */
  public String getAdminPerms();

 /**
   * Sets the admin permission ace.
   * @param perms the admin permission ace.
   */
  public void setAdminPerms(String perms);

  /**
   * Sets the type of the stream to be Changelog.
   */
  public void setIsChangelog(boolean ischglog);

  /**
   * @return {@code true} if this stream is changelog type.
   */
  public boolean getIsChangelog();

  /**
   * Sets timestamp type
   * @param logAppendTime can be CREATE_TIME or LOG_APPEND_TIME 
   */
  public void setDefaultTimestampType(TimestampType logAppendTime);

  /**
   * @return timestampType
   */
  public TimestampType getDefaultTimestampType();

  /**
   * Sets the log compaction on stream.
   */
  public void setCompact(boolean compact);

  /**
   * Gets log compaction on a stream.
   * @return {@code true} if stream has log compaction set.
   */
  public boolean getCompact();

  public void setForce();
  public boolean getForce();

  /**
   * Applies only if log compaction is enabled on the stream.
   * @return The minimum time in millisecond a message will remain uncompacted
   * in the topic-partition.
   */
  public long getMinCompactionLagMS();

  /**
   * Set time in millisecond a message should remain uncompacted in the
   * topic-partition.Applies only if log compaction is enabled on the stream.
   * @param ts time in milliseconds
   */
  public void setMinCompactionLagMS(long ts);

  /*
   * Hidden API to get compaction throttle factor
   * Throttles the load of compaction index.
   */
  public long getCompactionThrottleFactor();
  /*
   * Hidden API to set compaction throttle factor
   */
  public void setCompactionThrottleFactor(long tf);
 
  /**
   * Applies only if log compaction is enabled on the stream.
   * @return The time in millisecond for which delete records are retained.
   */
  public long getDeleteRetentionMS();

  /**
   * Set the time in millisecond for which delete records are retained.
   * Applies only if log compaction is enabled on the stream.
   * @param ts time in milliseconds
   */
  public void setDeleteRetentionMS(long ts);

  /**
   * Sets the producer id expiry
   * @param producerIdExpirySecs the producer id expiry in secs.
   */
  public void setProducerIdExpirySecs(long producerIdExpirySecs);

  /**
   *  @return the producer id expiry in secs
   */
  public long getProducerIdExpirySecs();

  /**
   * @return If the stream is a kafka topic stream.
   */
  public boolean isKafkaTopic();

  /**
   * Sets if the stream is a kafka topic stream.
   * @param isKafkaTopic if the stream is a kafka topic stream.
   */
  public void setKafkaTopic(boolean kafkaTopic);
}
