/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

package com.mapr.kafka.eventstreams.impl.admin;

import com.mapr.kafka.eventstreams.StreamDescriptor;
import com.mapr.kafka.eventstreams.TimestampType;

public class MStreamDescriptor implements StreamDescriptor {
  /* #partitions created by default on topic create */
  int defaultPartitions;
  boolean hasDefaultPartitions;

  /* time-to-live for messages in sec */
  long timeToLiveSec;
  boolean hasTimeToLiveSec;

  /* compression type */
  String compressionAlgo;
  boolean hasCompressionAlgo;

  /* client compression */
  boolean clientCompression;
  boolean hasClientCompression;

  /* allow auto-create for topics */
  boolean autoCreateTopics;
  boolean hasAutoCreateTopics;

  /* perms for produce */
  String producePerms;
  boolean hasProducePerms;

  /* perms for consume */
  String consumePerms;
  boolean hasConsumePerms;

  /* perms for topic create/remove/edit */
  String topicPerms;
  boolean hasTopicPerms;

  /* perms for copy (xcr/copytable/..) */
  String copyPerms;
  boolean hasCopyPerms;

  /* perms for everything else (repl/aceAdmin/..) */
  String adminPerms;
  boolean hasAdminPerms;

  /* changelog type of stream*/
  boolean isChangelog;
  boolean hasIsChangelog;

  boolean hasTimestampType;
  TimestampType timestampType;

  /*log compaction */
  boolean hasCompact;
  boolean compact;
  boolean force;

  /*
  * The minimum time in millisecond a message will remain uncompacted in the topic-partition
  */
  long minCompactionLagMS;
  boolean hasMinCompactionLagMS;

  /*
  * The time in millisecond for which delete records are retained
  */
  long deleteRetentionMS;
  boolean hasDeleteRetentionMS;

  /*
   * Throttle factor for log compaction index loading.
   */
  long compactionThrottleFactor;
  boolean hasCompactionThrottleFactor;
  

  /*Producer Id expiry in secs for idempotent producer*/
  boolean hasProducerIdExpirySecs;
  long producerIdExpirySecs;

  boolean kafkaTopic;

  public MStreamDescriptor() {
    hasDefaultPartitions = false;
    hasTimeToLiveSec = false;
    hasCompressionAlgo = false;
    hasClientCompression = false;
    hasAutoCreateTopics = false;
    hasProducePerms = false;
    hasConsumePerms = false;
    hasTopicPerms = false;
    hasCopyPerms = false;
    hasAdminPerms = false;
    hasIsChangelog = false;
    hasTimestampType = false;
    hasCompact = false;
    hasMinCompactionLagMS = false;
    hasCompactionThrottleFactor = false;
    hasDeleteRetentionMS = false;
    hasProducerIdExpirySecs = false;
    force = false;
    kafkaTopic = false;
  }

  /* accessors for defaultPartitions */
  public boolean hasDefaultPartitions() {
    return hasDefaultPartitions;
  }

  @Override
  public int getDefaultPartitions() {
    return defaultPartitions;
  }

  @Override
  public void setDefaultPartitions(int numFeeds) {
    hasDefaultPartitions = true;
    defaultPartitions = numFeeds;
  }

  /* accessors for timeToLiveSec */
  public boolean hasTimeToLiveSec() {
    return hasTimeToLiveSec;
  }

  @Override
  public long getTimeToLiveSec() {
    return timeToLiveSec;
  }

  @Override
  public void setTimeToLiveSec(long sec) {
    hasTimeToLiveSec = true;
    timeToLiveSec = sec;
  }

  /* accessors for compressionAlgo */
  public boolean hasCompressionAlgo() {
    return hasCompressionAlgo;
  }

  @Override
  public String getCompressionAlgo() {
    return compressionAlgo;
  }

  @Override
  public void setCompressionAlgo(String algo) {
    hasCompressionAlgo = true;
    compressionAlgo = algo;
  }

  /* accessors for clientCompression */
  public boolean hasClientCompression() {
    return hasClientCompression;
  }

  public boolean getClientCompression() {
    return clientCompression;
  }

  public void setClientCompression(boolean val) {
    hasClientCompression = true;
    clientCompression = val;
  }

  /* accessors for autoCreateTopics */
  public boolean hasAutoCreateTopics() {
    return hasAutoCreateTopics;
  }

  @Override
  public boolean getAutoCreateTopics() {
    return autoCreateTopics;
  }

  @Override
  public void setAutoCreateTopics(boolean allow) {
    hasAutoCreateTopics = true;
    autoCreateTopics = allow;
  }

  /* accessors for producePerms */ 
  public boolean hasProducePerms() {
    return hasProducePerms;
  }

  @Override
  public String getProducePerms() {
    return producePerms;
  }

  @Override
  public void setProducePerms(String perms) {
    hasProducePerms = true;
    producePerms = perms;
  }

  /* accessors for consumePerms */ 
  public boolean hasConsumePerms() {
    return hasConsumePerms;
  }

  @Override
  public String getConsumePerms() {
    return consumePerms;
  }

  @Override
  public void setConsumePerms(String perms) {
    hasConsumePerms = true;
    consumePerms = perms;
  }

  /* accessors for topicPerms */ 
  public boolean hasTopicPerms() {
    return hasTopicPerms;
  }

  @Override
  public String getTopicPerms() {
    return topicPerms;
  }

  @Override
  public void setTopicPerms(String perms) {
    hasTopicPerms = true;
    topicPerms = perms;
  }

  /* accessors for copyPerms */
  public boolean hasCopyPerms() {
    return hasCopyPerms;
  }

  @Override
  public String getCopyPerms() {
    return copyPerms;
  }

  @Override
  public void setCopyPerms(String perms) {
    hasCopyPerms = true;
    copyPerms = perms;
  }

  /* accessors for adminPerms */ 
  public boolean hasAdminPerms() {
    return hasAdminPerms;
  }

  @Override
  public String getAdminPerms() {
    return adminPerms;
  }

  @Override
  public void setAdminPerms(String perms) {
    hasAdminPerms = true;
    adminPerms = perms;
  }

  public boolean hasIsChangelog() {
    return hasIsChangelog;
  }

  @Override
  public void setIsChangelog(boolean ischglog) {
    hasIsChangelog = true;
    isChangelog = ischglog;
  }

  @Override
  public boolean getIsChangelog() {
    return isChangelog;
  }

  /*
   * Accessors for hasTimestampType
   */
  public boolean hasTimestampType() {
    return hasTimestampType;
  }

  @Override
  public void setDefaultTimestampType(TimestampType timestampType) {
    hasTimestampType = true;
    this.timestampType = timestampType;
  }

  @Override
  public TimestampType getDefaultTimestampType() {
    return timestampType;
  }

  /*
   * Accessors for compact
   */
  public boolean hasCompact() {
    return hasCompact;
  }

  @Override
  public void setCompact(boolean logCompaction) {
    hasCompact = true;
    compact = logCompaction;
  }

  @Override
  public boolean getCompact() {
    return compact;
  }

  @Override
  public void setForce() {
    force = true;
  }

  @Override
  public boolean getForce() {
    return force;
  }
  
  /* Accessors for MinCompactionLagMS */
  public boolean hasMinCompactionLagMS() {
    return hasMinCompactionLagMS;
  }

  @Override
  public long getMinCompactionLagMS() {
    return minCompactionLagMS;
  }

  @Override
  public void setMinCompactionLagMS(long ts) {
    hasMinCompactionLagMS = true;
    minCompactionLagMS = ts;
  }

  public boolean hasCompactionThrottleFactor() {
    return hasCompactionThrottleFactor;
  }

  @Override
  public long getCompactionThrottleFactor() {
    return compactionThrottleFactor;
  }

  @Override
  public void setCompactionThrottleFactor(long tf) {
    hasCompactionThrottleFactor = true;
    compactionThrottleFactor = tf;
  }
 
  /* Accessors for DeleteRetentionMS */
  public boolean hasDeleteRetentionMS() {
    return hasDeleteRetentionMS;
  }

  @Override
  public long getDeleteRetentionMS() {
    return deleteRetentionMS;
  }

  @Override
  public void setDeleteRetentionMS (long ts) {
    hasDeleteRetentionMS = true;
    deleteRetentionMS = ts;
  }

  /*
   * Accessor for producerIdExpirySecs
   */
  public boolean hasProducerIdExpirySecs() {
    return hasProducerIdExpirySecs;
  }

  @Override
  public void setProducerIdExpirySecs(long producerIdExpirySecs) {
    hasProducerIdExpirySecs = true;
    this.producerIdExpirySecs = producerIdExpirySecs;
  }

  @Override
  public long getProducerIdExpirySecs() {
    return producerIdExpirySecs;
  }

  @Override
  public void setKafkaTopic(boolean kafkaTopic) {
    this.kafkaTopic = kafkaTopic;
  }

  @Override
  public boolean isKafkaTopic() {
    return kafkaTopic;
  }
}
